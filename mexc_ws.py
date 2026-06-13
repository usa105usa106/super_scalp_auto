from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

import aiohttp

from mexc_client import MexcFuturesClient
from full_logger import log_event, log_debug, log_error


class MexcDepthWebSocket:
    """Fast public MEXC futures depth cache.

    MEXC futures WS pushes depth updates about every 200 ms. This class keeps an
    in-memory top-of-book cache so the scanner and active trade cycle do not have
    to call REST depth on every tick.
    """

    def __init__(self, endpoint: str | None = None, settings: dict[str, Any] | None = None):
        self.settings: dict[str, Any] = dict(settings or {})
        self.endpoint = endpoint or str(self.settings.get("mexc_futures_ws") or os.getenv("MEXC_FUTURES_WS", "wss://contract.mexc.com/edge"))
        self.desired_symbols: set[str] = set()
        self._subscribed: set[str] = set()
        self._books: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._session: aiohttp.ClientSession | None = None
        self._running = False
        self.last_error = ""
        self.last_message_ts = 0.0
        self.last_connect_ts = 0.0
        self.reconnects = 0

    def update_settings(self, settings: dict[str, Any] | None = None) -> None:
        self.settings = dict(settings or {})
        self.endpoint = str(self.settings.get("mexc_futures_ws") or os.getenv("MEXC_FUTURES_WS", "wss://contract.mexc.com/edge"))

    @staticmethod
    def _sid(symbol: str) -> str:
        return MexcFuturesClient.contract_id(symbol)

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="mexc_depth_ws")

    async def close(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        self._subscribed.clear()

    async def set_symbols(self, symbols: list[str]) -> None:
        clean = {self._sid(s) for s in symbols if s}
        async with self._lock:
            self.desired_symbols = clean
        log_event("ws_set_symbols", count=len(clean), symbols=sorted(clean)[:30])
        self.start()

    async def add_symbols(self, symbols: list[str]) -> None:
        clean = {self._sid(s) for s in symbols if s}
        async with self._lock:
            self.desired_symbols |= clean
        log_event("ws_add_symbols", add_count=len(clean), symbols=sorted(clean)[:30])
        self.start()

    def seed_book(self, symbol: str, book: dict[str, Any]) -> None:
        """Seed cache from REST snapshot so incremental WS updates become usable immediately."""
        sid = self._sid(symbol)
        bids = {float(p): float(q) for p, q in (book.get("bids") or []) if float(p) > 0 and float(q) > 0}
        asks = {float(p): float(q) for p, q in (book.get("asks") or []) if float(p) > 0 and float(q) > 0}
        if not bids or not asks:
            return
        self._books[sid] = {"bids": bids, "asks": asks, "ts": time.time(), "version": book.get("version")}
        log_debug("ws_seed_book", symbol=sid, bids=len(bids), asks=len(asks), version=book.get("version"))

    def get_book(self, symbol: str, limit: int = 20, max_age_ms: int = 700) -> dict[str, Any] | None:
        sid = self._sid(symbol)
        b = self._books.get(sid)
        if not b:
            return None
        ts = float(b.get("ts") or 0)
        age_ms = (time.time() - ts) * 1000.0
        if age_ms > max(1, int(max_age_ms or 700)):
            return None
        bids_map = b.get("bids") or {}
        asks_map = b.get("asks") or {}
        if not bids_map or not asks_map:
            return None
        bids = sorted(((float(p), float(q)) for p, q in bids_map.items() if float(q) > 0), key=lambda x: x[0], reverse=True)[:limit]
        asks = sorted(((float(p), float(q)) for p, q in asks_map.items() if float(q) > 0), key=lambda x: x[0])[:limit]
        if not bids or not asks:
            return None
        return {"symbol": sid, "bids": bids, "asks": asks, "source": "ws", "age_ms": age_ms, "version": b.get("version")}

    def stats(self) -> dict[str, Any]:
        fresh = 0
        now = time.time()
        for b in self._books.values():
            if now - float(b.get("ts") or 0) < 1.0:
                fresh += 1
        return {
            "desired": len(self.desired_symbols),
            "subscribed": len(self._subscribed),
            "books": len(self._books),
            "fresh_books": fresh,
            "last_msg_age": now - self.last_message_ts if self.last_message_ts else 0,
            "reconnects": self.reconnects,
            "last_error": self.last_error,
        }

    async def _send_json(self, ws: aiohttp.ClientWebSocketResponse, payload: dict[str, Any]) -> None:
        await ws.send_str(json.dumps(payload, separators=(",", ":")))

    async def _sync_subscriptions(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async with self._lock:
            desired = set(self.desired_symbols)
        for sym in sorted(desired - self._subscribed):
            await self._send_json(ws, {"method": "sub.depth", "param": {"symbol": sym}})
            self._subscribed.add(sym)
            log_debug("ws_subscribe_depth", symbol=sym)
            await asyncio.sleep(0.01)
        for sym in sorted(self._subscribed - desired):
            await self._send_json(ws, {"method": "unsub.depth", "param": {"symbol": sym}})
            self._subscribed.discard(sym)
            log_debug("ws_unsubscribe_depth", symbol=sym)
            await asyncio.sleep(0.01)

    def _apply_depth_message(self, msg: dict[str, Any]) -> None:
        channel = str(msg.get("channel") or "")
        if channel and not channel.startswith("push.depth"):
            return
        symbol = self._sid(msg.get("symbol") or "")
        if not symbol:
            return
        data = msg.get("data") or {}
        if not isinstance(data, dict):
            return
        book = self._books.setdefault(symbol, {"bids": {}, "asks": {}, "ts": 0.0, "version": 0})
        for side_key in ("bids", "asks"):
            levels = data.get(side_key) or []
            side = book.setdefault(side_key, {})
            for row in levels:
                try:
                    if isinstance(row, dict):
                        price = float(row.get("price") or row.get("p") or row.get("px") or 0)
                        qty = float(row.get("vol") or row.get("volume") or row.get("quantity") or row.get("q") or row.get("qty") or 0)
                    elif isinstance(row, (list, tuple)) and len(row) >= 2:
                        price = float(row[0])
                        # MEXC futures depth rows are usually [price, order_count, quantity],
                        # but some snapshots use [price, quantity].
                        qty = float(row[2] if len(row) >= 3 else row[1])
                    else:
                        continue
                except Exception:
                    continue
                if price <= 0:
                    continue
                if qty <= 0:
                    side.pop(price, None)
                else:
                    side[price] = qty
        book["ts"] = time.time()
        book["version"] = data.get("version") or book.get("version")
        self.last_message_ts = book["ts"]

    async def _run(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None))
                log_event("ws_connect_start", endpoint=self.endpoint)
                async with self._session.ws_connect(self.endpoint, heartbeat=None, autoping=False, compress=0, max_msg_size=2**22) as ws:
                    self.last_connect_ts = time.time()
                    log_event("ws_connected", endpoint=self.endpoint)
                    self._subscribed.clear()
                    backoff = 1.0
                    last_ping = 0.0
                    last_sub_sync = 0.0
                    while self._running and not ws.closed:
                        now = time.time()
                        if now - last_ping >= 15.0:
                            await self._send_json(ws, {"method": "ping"})
                            last_ping = now
                        if now - last_sub_sync >= 0.5:
                            await self._sync_subscriptions(ws)
                            last_sub_sync = now
                        try:
                            item = await ws.receive(timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        if item.type == aiohttp.WSMsgType.TEXT:
                            try:
                                msg = json.loads(item.data)
                            except Exception:
                                continue
                            self._apply_depth_message(msg)
                        elif item.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                            log_event("ws_closed_or_error", type=str(item.type), data=str(getattr(item, "data", ""))[:240])
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.last_error = str(e)[:240]
                log_error("ws_run_error", e)
            finally:
                self.reconnects += 1
                log_event("ws_reconnect_or_cleanup", reconnects=self.reconnects, last_error=self.last_error)
                self._subscribed.clear()
                if self._session and not self._session.closed:
                    await self._session.close()
                self._session = None
            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(15.0, backoff * 1.5)
