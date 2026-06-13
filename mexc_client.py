from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import math
import os
import time
from collections import deque
from typing import Any
from urllib.parse import urlencode

import aiohttp

from full_logger import log_event, log_debug, log_error


class MexcFuturesClient:
    """Native MEXC futures client extracted from the working bot mechanism.

    Important side/type codes used by MEXC futures /api/v1/private/order/create:
    side 1 = open long, 3 = open short, 4 = close long, 2 = close short
    type 1 = limit, 2 = post only / maker only, 5 = market
    openType 1 = isolated, 2 = cross
    """

    def __init__(self, api_key: str = "", api_secret: str = "", base_url: str | None = None, settings: dict[str, Any] | None = None):
        self.api_key = str(api_key or "").strip()
        self.api_secret = str(api_secret or "").strip()
        self.settings: dict[str, Any] = dict(settings or {})
        self.base_url = (base_url or self._setting("mexc_rest_base", "MEXC_REST_BASE", "https://api.mexc.com")).rstrip("/")
        self.time_difference_ms = 0
        self._private_request_times: deque[float] = deque()
        self._private_lock = asyncio.Lock()
        self._details_cache: dict[str, dict[str, Any]] = {}
        self._details_cache_ts = 0.0

    def update_settings(self, settings: dict[str, Any] | None = None) -> None:
        self.settings = dict(settings or {})
        self.base_url = str(self._setting("mexc_rest_base", "MEXC_REST_BASE", "https://api.mexc.com") or "https://api.mexc.com").rstrip("/")

    def _setting(self, key: str, env_key: str, default: Any) -> Any:
        if isinstance(self.settings, dict) and key in self.settings and self.settings.get(key) not in (None, ""):
            return self.settings.get(key)
        return os.getenv(env_key, str(default))

    def _int_setting(self, key: str, env_key: str, default: int) -> int:
        try:
            return int(float(self._setting(key, env_key, default)))
        except Exception:
            return int(default)

    def _float_setting(self, key: str, env_key: str, default: float) -> float:
        try:
            return float(self._setting(key, env_key, default))
        except Exception:
            return float(default)

    def _bool_setting(self, key: str, env_key: str, default: bool) -> bool:
        value = self._setting(key, env_key, default)
        if isinstance(value, bool):
            return value
        return str(value).lower() in {"1", "true", "yes", "on", "да", "вкл"}

    # ---------- symbol / precision helpers ----------

    @staticmethod
    def contract_id(symbol: str) -> str:
        raw = str(symbol or "").strip().upper().replace("-", "_").replace("/", "_")
        if raw.endswith(":USDT"):
            raw = raw[:-5]
        if "_" not in raw and raw.endswith("USDT"):
            raw = raw[:-4] + "_USDT"
        if "_" not in raw and raw:
            raw = raw + "_USDT"
        return raw

    @staticmethod
    def display_symbol(symbol: str) -> str:
        sid = MexcFuturesClient.contract_id(symbol)
        if "_" in sid:
            base, quote = sid.split("_", 1)
            return f"{base}/{quote}:USDT"
        return sid

    @staticmethod
    def _rows(data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            for key in ("list", "result", "resultList", "data", "rows", "items"):
                value = data.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]
            if data and all(isinstance(v, dict) for v in data.values()):
                return list(data.values())
            return [data]
        return []

    @staticmethod
    def _float(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return default

    @staticmethod
    def _int(v: Any, default: int = 0) -> int:
        try:
            return int(float(v))
        except Exception:
            return default

    async def _contract_details_all(self) -> dict[str, dict[str, Any]]:
        now = time.time()
        if self._details_cache and now - self._details_cache_ts < 900:
            return self._details_cache
        out = await self.public("GET", "/api/v1/contract/detail")
        rows = self._rows(out.get("data") if isinstance(out, dict) else out)
        details: dict[str, dict[str, Any]] = {}
        for r in rows:
            sid = self.contract_id(r.get("symbol") or r.get("contract") or r.get("contractName") or "")
            if sid:
                details[sid] = r
        self._details_cache = details
        self._details_cache_ts = now
        return details

    async def contract_detail(self, symbol: str) -> dict[str, Any]:
        sid = self.contract_id(symbol)
        all_details = await self._contract_details_all()
        if sid in all_details:
            return all_details[sid]
        try:
            out = await self.public("GET", "/api/v1/contract/detail", query={"symbol": sid})
            rows = self._rows(out.get("data") if isinstance(out, dict) else out)
            if rows:
                self._details_cache[sid] = rows[0]
                return rows[0]
        except Exception:
            pass
        return {"symbol": sid}

    async def price_tick(self, symbol: str) -> float:
        d = await self.contract_detail(symbol)
        for key in ("priceUnit", "tickSize", "priceTick"):
            val = d.get(key)
            try:
                f = float(val)
                if f > 0:
                    return f
            except Exception:
                pass
        sid = self.contract_id(symbol)
        if sid.startswith("BTC_"):
            return 0.1
        if sid.startswith("ETH_"):
            return 0.01
        return 0.0001

    async def contract_size(self, symbol: str) -> float:
        d = await self.contract_detail(symbol)
        for key in ("contractSize", "contract_size", "contract_size_unit"):
            val = d.get(key)
            try:
                f = float(val)
                if f > 0:
                    return f
            except Exception:
                pass
        sid = self.contract_id(symbol)
        if sid.startswith("BTC_"):
            return 0.0001
        if sid.startswith("ETH_"):
            return 0.01
        # Safe last fallback: unknown contracts are treated as 1 base unit per contract.
        return 1.0

    async def round_price(self, symbol: str, price: float, mode: str = "nearest") -> float:
        tick = await self.price_tick(symbol)
        price = float(price or 0)
        if price <= 0 or tick <= 0:
            return price
        q = price / tick
        if mode == "floor":
            rounded = math.floor(q) * tick
        elif mode == "ceil":
            rounded = math.ceil(q) * tick
        else:
            rounded = round(q) * tick
        decimals = max(0, min(12, int(round(-math.log10(tick))) if tick < 1 else 0))
        return float(f"{rounded:.{decimals}f}")

    async def vol_from_margin(self, symbol: str, margin_usdt: float, leverage: int, price: float) -> int:
        cs = await self.contract_size(symbol)
        notional = max(0.0, float(margin_usdt or 0) * max(1, int(leverage or 1)))
        amount_base = notional / max(float(price or 0), 1e-12)
        vol = int(math.floor(amount_base / max(cs, 1e-12)))
        d = await self.contract_detail(symbol)
        min_vol = self._int(d.get("minVol") or d.get("minVolume") or 1, 1)
        max_vol = self._int(d.get("maxVol") or d.get("maxVolume") or 0, 0)
        vol = max(min_vol, vol)
        if max_vol > 0:
            vol = min(max_vol, vol)
        return max(1, vol)

    async def amount_from_contracts(self, symbol: str, contracts: float) -> float:
        return abs(float(contracts or 0)) * await self.contract_size(symbol)

    # ---------- HTTP / signing ----------

    async def close(self) -> None:
        return None

    async def sync_time(self) -> int:
        try:
            out = await self.public("GET", "/api/v1/contract/ping")
            server = self._int((out or {}).get("data") or (out or {}).get("timestamp") or 0, 0)
            if server > 0:
                self.time_difference_ms = server - int(time.time() * 1000)
        except Exception:
            pass
        return self.time_difference_ms

    async def _private_rate_limit(self) -> None:
        limit = self._int_setting("mexc_private_rate_limit", "MEXC_PRIVATE_RATE_LIMIT", 18)
        window = 2.0
        async with self._private_lock:
            now = time.monotonic()
            while self._private_request_times and now - self._private_request_times[0] >= window:
                self._private_request_times.popleft()
            if len(self._private_request_times) >= limit:
                await asyncio.sleep(window - (now - self._private_request_times[0]) + 0.05)
            self._private_request_times.append(time.monotonic())

    def _request_time(self) -> str:
        return str(int(time.time() * 1000) + int(self.time_difference_ms or 0))

    def _signature(self, req_time: str, payload: str) -> str:
        raw = f"{self.api_key}{req_time}{payload}"
        return hmac.new(self.api_secret.encode(), raw.encode(), hashlib.sha256).hexdigest()

    def _recv_window(self) -> str:
        try:
            value = self._int_setting("mexc_recv_window", "MEXC_RECV_WINDOW", 20000)
        except Exception:
            value = 20000
        if value > 1000:
            value = int((value + 999) // 1000)
        return str(max(1, min(60, value)))

    async def public(self, method: str, path: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
        method = str(method or "GET").upper()
        query = dict(query or {})
        qs = urlencode(sorted((k, v) for k, v in query.items() if v is not None))
        url = f"{self.base_url}{path}" + (f"?{qs}" if qs else "")
        timeout = aiohttp.ClientTimeout(total=self._float_setting("mexc_public_timeout", "MEXC_PUBLIC_TIMEOUT", 6.0))
        started = time.perf_counter()
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(method, url, headers={"User-Agent": "Mozilla/5.0"}) as r:
                    text = await r.text()
                    try:
                        out = json.loads(text)
                    except Exception:
                        out = {"raw": text}
                    ms = (time.perf_counter() - started) * 1000.0
                    if r.status >= 400 or (isinstance(out, dict) and out.get("success") is False):
                        log_error("mexc_public_http_error", None, method=method, path=path, query=query, status=r.status, ms=round(ms, 1), response=out)
                        raise RuntimeError(f"MEXC public HTTP {r.status}: {str(out)[:260]}")
                    # Log only selected public successes to keep log useful without flooding every depth tick.
                    if path not in {"/api/v1/contract/depth"} and not path.startswith("/api/v1/contract/depth/"):
                        log_debug("mexc_public_ok", method=method, path=path, query=query, status=r.status, ms=round(ms, 1))
                    return out if isinstance(out, dict) else {"data": out}
        except Exception as e:
            log_error("mexc_public_exception", e, method=method, path=path, query=query)
            raise

    async def private(self, method: str, path: str, body: Any = None, query: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.api_key or not self.api_secret:
            raise RuntimeError("MEXC API key/secret is missing. Use /api set KEY SECRET in Telegram.")
        await self._private_rate_limit()
        method = str(method or "GET").upper()
        query = dict(query or {})
        if body is None:
            body = {}
        elif isinstance(body, dict):
            body = dict(body)
            if body.get("symbol") not in (None, ""):
                body["symbol"] = self.contract_id(body.get("symbol"))
        elif isinstance(body, list):
            body = [dict(x) if isinstance(x, dict) else x for x in body]
            for item in body:
                if isinstance(item, dict) and item.get("symbol") not in (None, ""):
                    item["symbol"] = self.contract_id(item.get("symbol"))
        if query.get("symbol") not in (None, ""):
            query["symbol"] = self.contract_id(query.get("symbol"))

        if method == "GET":
            payload = urlencode(sorted((k, v) for k, v in query.items() if v is not None))
            url = f"{self.base_url}{path}" + (f"?{payload}" if payload else "")
            data = None
        else:
            payload = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
            url = f"{self.base_url}{path}"
            data = payload
        req_time = self._request_time()
        headers = {
            "ApiKey": self.api_key,
            "Request-Time": req_time,
            "Signature": self._signature(req_time, payload),
            "Recv-Window": self._recv_window(),
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        }
        timeout = aiohttp.ClientTimeout(total=self._float_setting("mexc_private_timeout", "MEXC_PRIVATE_TIMEOUT", 15.0))
        started = time.perf_counter()
        log_debug("mexc_private_request", method=method, path=path, query=query, body=body)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(method, url, data=data, headers=headers) as r:
                    text = await r.text()
                    try:
                        out = json.loads(text)
                    except Exception:
                        out = {"raw": text}
                    if r.status in (401, 403) or str((out or {}).get("code")) in {"401", "403", "602", "603"}:
                        log_event("mexc_private_retry_time_sync", method=method, path=path, status=r.status, response=out)
                        await self.sync_time()
                        req_time = self._request_time()
                        headers["Request-Time"] = req_time
                        headers["Signature"] = self._signature(req_time, payload)
                        async with session.request(method, url, data=data, headers=headers) as r2:
                            text = await r2.text()
                            try:
                                out = json.loads(text)
                            except Exception:
                                out = {"raw": text}
                            ms = (time.perf_counter() - started) * 1000.0
                            if r2.status >= 400 or (isinstance(out, dict) and out.get("success") is False):
                                log_error("mexc_private_http_error", None, method=method, path=path, query=query, body=body, status=r2.status, ms=round(ms, 1), response=out)
                                raise RuntimeError(f"MEXC private HTTP {r2.status}: {str(out)[:320]}")
                            log_debug("mexc_private_ok", method=method, path=path, status=r2.status, ms=round(ms, 1), response=out)
                            return out if isinstance(out, dict) else {"data": out}
                    ms = (time.perf_counter() - started) * 1000.0
                    if r.status >= 400 or (isinstance(out, dict) and out.get("success") is False):
                        log_error("mexc_private_http_error", None, method=method, path=path, query=query, body=body, status=r.status, ms=round(ms, 1), response=out)
                        raise RuntimeError(f"MEXC private HTTP {r.status}: {str(out)[:320]}")
                    log_debug("mexc_private_ok", method=method, path=path, status=r.status, ms=round(ms, 1), response=out)
                    return out if isinstance(out, dict) else {"data": out}
        except Exception as e:
            log_error("mexc_private_exception", e, method=method, path=path, query=query, body=body)
            raise

    # ---------- exchange reads ----------

    async def fetch_balance(self) -> dict[str, Any]:
        out = await self.private("GET", "/api/v1/private/account/assets")
        rows = self._rows(out.get("data"))
        usdt = {}
        for r in rows:
            ccy = str(r.get("currency") or r.get("asset") or "").upper()
            if ccy != "USDT":
                continue
            total = self._float(r.get("equity") or r.get("totalEquity") or r.get("cashBalance") or r.get("balance"))
            free = self._float(r.get("availableBalance") or r.get("available") or r.get("availableOpen") or r.get("cashBalance"))
            used = max(0.0, total - free)
            usdt = {
                "total": total,
                "free": free,
                "used": used,
                "positionMargin": self._float(r.get("positionMargin")),
                "frozenBalance": self._float(r.get("frozenBalance")),
                "unrealized": self._float(r.get("unrealized")),
                "raw": r,
            }
            break
        return {"USDT": usdt or {"total": 0.0, "free": 0.0, "used": 0.0}, "info": out}

    async def ticker(self, symbol: str) -> dict[str, Any]:
        sid = self.contract_id(symbol)
        out = await self.public("GET", "/api/v1/contract/ticker", query={"symbol": sid})
        data = out.get("data")
        row = data[0] if isinstance(data, list) and data else data
        row = row if isinstance(row, dict) else {}
        last = self._float(row.get("lastPrice") or row.get("last") or row.get("fairPrice") or row.get("indexPrice"))
        bid = self._float(row.get("bid1") or row.get("bid") or row.get("bidPrice"), last)
        ask = self._float(row.get("ask1") or row.get("ask") or row.get("askPrice"), last)
        return {"symbol": sid, "last": last, "bid": bid, "ask": ask, "quoteVolume": self._float(row.get("amount24") or row.get("volume24")), "raw": row}

    async def all_tickers(self) -> dict[str, dict[str, Any]]:
        """Return futures tickers keyed by MEXC contract id.

        Used only as a lightweight pre-sort for the zero-fee universe. If MEXC
        changes the shape of this public endpoint, the scanner still works; it
        simply falls back to alphabetical zero-fee order.
        """
        out = await self.public("GET", "/api/v1/contract/ticker", query={})
        data = out.get("data")
        rows = data if isinstance(data, list) else self._rows(data)
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            sid = self.contract_id(row.get("symbol") or row.get("contract") or row.get("contractName") or "")
            if not sid:
                continue
            last = self._float(row.get("lastPrice") or row.get("last") or row.get("fairPrice") or row.get("indexPrice"))
            bid = self._float(row.get("bid1") or row.get("bid") or row.get("bidPrice"), last)
            ask = self._float(row.get("ask1") or row.get("ask") or row.get("askPrice"), last)
            result[sid] = {
                "symbol": sid,
                "last": last,
                "bid": bid,
                "ask": ask,
                "quoteVolume": self._float(row.get("amount24") or row.get("volume24")),
                "raw": row,
            }
        return result

    async def depth(self, symbol: str, limit: int = 20) -> dict[str, Any]:
        sid = self.contract_id(symbol)
        limit = max(5, min(int(limit or 20), 100))
        try:
            out = await self.public("GET", f"/api/v1/contract/depth/{sid}", query={"limit": limit})
        except Exception:
            out = await self.public("GET", "/api/v1/contract/depth", query={"symbol": sid, "limit": limit})
        data = out.get("data") if isinstance(out, dict) else out
        data = data if isinstance(data, dict) else {}

        def rows(key: str) -> list[list[float]]:
            result: list[list[float]] = []
            for row in data.get(key) or []:
                try:
                    if isinstance(row, dict):
                        p = row.get("price") or row.get("p")
                        q = row.get("vol") or row.get("volume") or row.get("quantity") or row.get("q")
                    else:
                        p, q = row[0], row[1]
                    result.append([float(p), float(q)])
                except Exception:
                    continue
            return result

        return {"symbol": sid, "bids": rows("bids"), "asks": rows("asks")}

    async def fetch_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        queries = [{}]
        if symbol:
            queries.append({"symbol": self.contract_id(symbol)})
        rows: list[dict[str, Any]] = []
        for q in queries:
            try:
                out = await self.private("GET", "/api/v1/private/position/open_positions", query=q)
                rows.extend(self._rows(out.get("data")))
            except Exception:
                if symbol:
                    continue
                raise
        parsed: list[dict[str, Any]] = []
        seen = set()
        for r in rows:
            sid = self.contract_id(r.get("symbol") or r.get("contract") or "")
            pos_type = str(r.get("positionType") or r.get("holdSide") or r.get("side") or "").lower()
            key = (str(r.get("positionId") or r.get("id") or ""), sid, pos_type)
            if key in seen:
                continue
            seen.add(key)
            contracts = self._float(r.get("holdVol") or r.get("vol") or r.get("positionVol") or r.get("contracts"))
            if contracts <= 0:
                continue
            side = "short" if pos_type in {"2", "short", "sell"} or "short" in pos_type else "long"
            entry = self._float(r.get("holdAvgPrice") or r.get("openAvgPrice") or r.get("entryPrice") or r.get("avgPrice"))
            mark = self._float(r.get("markPrice") or r.get("fairPrice") or r.get("lastPrice"))
            parsed.append({
                "symbol": sid,
                "side": side,
                "contracts": contracts,
                "entryPrice": entry,
                "markPrice": mark,
                "positionId": r.get("positionId") or r.get("id"),
                "raw": r,
            })
        if symbol:
            want = self.contract_id(symbol)
            parsed = [p for p in parsed if self.contract_id(p.get("symbol")) == want]
        return parsed

    async def find_position(self, symbol: str, side: str | None = None) -> dict[str, Any] | None:
        positions = await self.fetch_positions(symbol)
        for p in positions:
            if side and p.get("side") != side:
                continue
            return p
        return None

    async def fetch_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        candidates: list[tuple[str, dict[str, Any]]] = []
        if symbol:
            sid = self.contract_id(symbol)
            candidates = [
                (f"/api/v1/private/order/list/open_orders/{sid}", {}),
                ("/api/v1/private/order/list/open_orders", {"symbol": sid}),
                ("/api/v1/private/planorder/list/orders", {"symbol": sid, "state": 1, "page_num": 1, "page_size": 100}),
                ("/api/v1/private/stoporder/open_orders", {"symbol": sid}),
            ]
        else:
            candidates = [
                ("/api/v1/private/order/list/open_orders", {}),
                ("/api/v1/private/planorder/list/orders", {"state": 1, "page_num": 1, "page_size": 100}),
                ("/api/v1/private/stoporder/open_orders", {}),
            ]
        orders: list[dict[str, Any]] = []
        for path, query in candidates:
            try:
                out = await self.private("GET", path, query=query)
                for r in self._rows(out.get("data")):
                    oid = str(r.get("orderId") or r.get("id") or r.get("planOrderId") or r.get("stopOrderId") or "")
                    if not oid:
                        continue
                    orders.append({"id": oid, "symbol": self.contract_id(r.get("symbol") or r.get("contract") or symbol or ""), "raw": r, "source": path})
            except Exception:
                continue
        return orders

    # ---------- orders ----------

    async def set_leverage(self, symbol: str, leverage: int, open_type: int = 1) -> dict[str, Any]:
        sid = self.contract_id(symbol)
        leverage = max(1, int(leverage or 1))
        endpoint = "/api/v1/private/position/change_leverage"
        payloads = [
            {"symbol": sid, "leverage": leverage, "openType": int(open_type or 1), "positionType": 1},
            {"symbol": sid, "leverage": leverage, "openType": int(open_type or 1), "positionType": 2},
            {"symbol": sid, "leverage": leverage, "openType": int(open_type or 1)},
        ]
        results, errors = [], []
        ok = False
        for body in payloads:
            try:
                res = await self.private("POST", endpoint, body=body)
                results.append(res)
                log_debug("mexc_set_leverage_ok", body=body, response=res)
                ok = True
            except Exception as e:
                errors.append(str(e)[:240])
                log_error("mexc_set_leverage_error", e, body=body)
        if not ok and self._bool_setting("mexc_strict_leverage", "MEXC_STRICT_LEVERAGE", True):
            raise RuntimeError("MEXC leverage setup failed: " + " | ".join(errors[:2]))
        return {"ok": ok, "results": results, "errors": errors, "leverage": leverage}

    async def place_order(
        self,
        symbol: str,
        side_code: int,
        order_type: int,
        vol: int,
        price: float = 0.0,
        leverage: int = 5,
        open_type: int = 1,
        external_oid: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "symbol": self.contract_id(symbol),
            "price": price if order_type != 5 else 0,
            "vol": int(vol),
            "side": int(side_code),
            "type": int(order_type),
            "openType": int(open_type or 1),
            "leverage": int(leverage or 1),
        }
        if external_oid:
            body["externalOid"] = str(external_oid)[:32]
        if extra:
            body.update(extra)
        log_event("mexc_order_create_request", body=body)
        out = await self.private("POST", "/api/v1/private/order/create", body=body)
        log_event("mexc_order_create_response", body=body, response=out)
        data = out.get("data") if isinstance(out, dict) else None
        oid = data.get("orderId") if isinstance(data, dict) else data
        return {"id": str(oid or ""), "symbol": self.contract_id(symbol), "body": body, "raw": out}

    async def open_post_only(self, symbol: str, direction: str, vol: int, price: float, leverage: int, open_type: int = 1) -> dict[str, Any]:
        # 1 open long, 3 open short; type 2 post-only maker-only.
        side_code = 1 if direction == "long" else 3
        await self.set_leverage(symbol, leverage, open_type)
        px = await self.round_price(symbol, price, "floor" if direction == "long" else "ceil")
        return await self.place_order(symbol, side_code, 2, vol, px, leverage, open_type, external_oid=f"mm_open_{int(time.time()*1000)%10**10}")

    async def close_limit(self, symbol: str, position_side: str, vol: int, price: float, leverage: int, open_type: int = 1, post_only: bool = True) -> dict[str, Any]:
        # 4 closes long, 2 closes short. type 2 post-only close can be disabled by settings.
        side_code = 4 if position_side == "long" else 2
        order_type = 2 if post_only else 1
        px = await self.round_price(symbol, price, "ceil" if position_side == "long" else "floor")
        return await self.place_order(symbol, side_code, order_type, vol, px, leverage, open_type, external_oid=f"mm_close_{int(time.time()*1000)%10**10}")

    async def close_market(self, position: dict[str, Any], leverage: int = 5, open_type: int = 1) -> dict[str, Any]:
        side = position.get("side")
        side_code = 4 if side == "long" else 2
        vol = int(round(float(position.get("contracts") or 0)))
        if vol <= 0:
            raise RuntimeError(f"empty position volume: {position}")
        return await self.place_order(position.get("symbol"), side_code, 5, vol, 0, leverage, open_type)

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> dict[str, Any]:
        oid = str(order_id or "").split(":", 1)[0].strip()
        if not oid:
            return {"ok": False, "reason": "empty order_id"}
        body: dict[str, Any] = {"orderId": int(oid) if oid.isdigit() else oid}
        if symbol:
            body["symbol"] = self.contract_id(symbol)
        log_event("mexc_cancel_order_request", body=body)
        res = await self.private("POST", "/api/v1/private/order/cancel", body=body)
        log_event("mexc_cancel_order_response", body=body, response=res)
        return res

    async def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        results, errors = [], []
        symbols = [symbol] if symbol else [None]
        for sym in symbols:
            sid = self.contract_id(sym) if sym else None
            candidates: list[tuple[str, Any]] = [
                ("/api/v1/private/order/cancel_all", {"symbol": sid} if sid else {}),
                ("/api/v1/private/planorder/cancel_all", {"symbol": sid} if sid else {}),
                ("/api/v1/private/stoporder/cancel_all", {"symbol": sid} if sid else {}),
            ]
            for path, body in candidates:
                try:
                    results.append({"endpoint": path, "result": await self.private("POST", path, body=body)})
                except Exception as e:
                    errors.append({"endpoint": path, "error": str(e)[:220]})
            try:
                for o in await self.fetch_open_orders(sym):
                    try:
                        results.append({"endpoint": "order/cancel", "order": o.get("id"), "result": await self.cancel_order(o.get("id"), o.get("symbol"))})
                    except Exception as e:
                        errors.append({"endpoint": "order/cancel", "order": o.get("id"), "error": str(e)[:220]})
            except Exception as e:
                errors.append({"endpoint": "fetch_open_orders", "error": str(e)[:220]})
        return {"ok": bool(results) or not errors, "results": results, "errors": errors}

    async def hard_close_all(self, symbols: list[str] | None = None, leverage: int = 5, open_type: int = 1) -> dict[str, Any]:
        """Emergency cleanup: cancel all active orders, close all positions by market, cancel again.

        Close All behavior is intentionally aggressive for the Telegram Close All button:
        - remove active/limit/plan/stop orders first so nothing new can fill;
        - market-close every open position;
        - run cancel_all again after closes to clear any leftover reduce-only/close limits.
        """
        results, errors = [], []
        symbols = [self.contract_id(s) for s in (symbols or []) if s]
        log_event("mexc_hard_close_all_start", symbols=symbols, leverage=leverage, open_type=open_type)
        try:
            # 1) Cancel global orders first. This is the fastest broad cleanup path.
            try:
                results.append({"stage": "cancel_all_before", "result": await self.cancel_all_orders(None)})
            except Exception as e:
                errors.append({"stage": "cancel_all_before", "error": str(e)[:260]})

            # 2) Fetch positions after canceling so no new entry limit can fill during cleanup.
            positions: list[dict[str, Any]] = []
            if symbols:
                for sym in symbols:
                    try:
                        positions.extend(await self.fetch_positions(sym))
                    except Exception as e:
                        errors.append({"stage": "fetch_positions", "symbol": sym, "error": str(e)[:260]})
            else:
                positions = await self.fetch_positions()
                symbols = sorted({self.contract_id(p.get("symbol")) for p in positions if p.get("symbol")})

            # 3) Close every open position by market.
            for p in positions:
                try:
                    results.append({"stage": "close_market", "position": p, "result": await self.close_market(p, leverage, open_type)})
                except Exception as e:
                    errors.append({"stage": "close_market", "position": p, "error": str(e)[:260]})

            # 4) Cancel again globally and per symbol to clear leftover reduce-only/limit/plan orders.
            try:
                results.append({"stage": "cancel_all_after", "result": await self.cancel_all_orders(None)})
            except Exception as e:
                errors.append({"stage": "cancel_all_after", "error": str(e)[:260]})
            for sym in symbols:
                try:
                    results.append({"stage": "cancel_symbol_after", "symbol": sym, "result": await self.cancel_all_orders(sym)})
                except Exception as e:
                    errors.append({"stage": "cancel_symbol_after", "symbol": sym, "error": str(e)[:260]})
        except Exception as e:
            errors.append({"stage": "hard_close_all", "error": str(e)[:260]})
        res = {"ok": bool(results) or not errors, "results": results, "errors": errors}
        log_event("mexc_hard_close_all_done", result=res)
        return res

    # ---------- fee guard ----------

    async def _contract_id_to_symbol_map(self) -> dict[str, str]:
        """Map MEXC numeric contractId -> SYMBOL_USDT using public contract detail.

        The private zero-fee endpoint returns contractId values, not symbols.
        The scanner trades symbols, so we translate through /contract/detail.
        """
        details = await self._contract_details_all()
        mapping: dict[str, str] = {}
        for sym, row in details.items():
            if not isinstance(row, dict):
                continue
            for key in ("contractId", "contract_id", "id"):
                value = row.get(key)
                if value in (None, ""):
                    continue
                try:
                    mapping[str(int(float(value)))] = sym
                except Exception:
                    mapping[str(value)] = sym
        return mapping

    async def fetch_fee_rates(self) -> dict[str, dict[str, Any]]:
        """Legacy/account-level fee endpoints.

        Kept as a fallback only. Some MEXC responses from tiered_fee_rate contain
        a single account-level example/default symbol and are not a zero-fee
        universe. The scanner should prefer the contract-specific endpoints below.
        """
        endpoints = [
            "/api/v1/private/account/tiered_fee_rate",
            "/api/v1/private/account/tiered_fee_rate/v2",
            "/api/v1/private/account/fee_rate",
            "/api/v1/private/account/feeRate",
        ]
        rates: dict[str, dict[str, Any]] = {}
        for ep in endpoints:
            try:
                out = await self.private("GET", ep, query={})
                rows = self._rows(out.get("data"))
                for r in rows:
                    sid = self.contract_id(r.get("symbol") or r.get("contract") or r.get("contractName") or "")
                    maker = r.get("makerFeeRate", r.get("makerFee", r.get("maker", r.get("openMakerFee"))))
                    taker = r.get("takerFeeRate", r.get("takerFee", r.get("taker", r.get("openTakerFee"))))
                    try:
                        rates[sid] = {"maker": float(maker), "taker": float(taker), "source": ep, "raw": r}
                    except Exception:
                        continue
                if rates:
                    return rates
            except Exception:
                continue
        return rates

    async def fetch_contract_fee_rates(self, symbol: str | None = None) -> dict[str, dict[str, Any]]:
        """Contract-level fee details keyed by SYMBOL_USDT.

        Official MEXC futures docs expose GET
        /api/v1/private/account/contract/fee_rate with optional symbol.
        It returns fields like isZeroFeeRate, makerFeeRate and takerFeeRate.
        """
        query = {"symbol": self.contract_id(symbol)} if symbol else {}
        rates: dict[str, dict[str, Any]] = {}
        try:
            out = await self.private("GET", "/api/v1/private/account/contract/fee_rate", query=query)
            rows = self._rows(out.get("data"))
            id_map: dict[str, str] | None = None
            for r in rows:
                if not isinstance(r, dict):
                    continue
                sid = self.contract_id(r.get("symbol") or r.get("contract") or r.get("contractName") or "")
                if (not sid or sid == "_USDT") and r.get("contractId") not in (None, ""):
                    if id_map is None:
                        id_map = await self._contract_id_to_symbol_map()
                    try:
                        sid = id_map.get(str(int(float(r.get("contractId")))), "")
                    except Exception:
                        sid = id_map.get(str(r.get("contractId")), "")
                if not sid or sid == "_USDT":
                    continue
                maker = r.get("makerFeeRate", r.get("realMakerFee", r.get("makerFee", r.get("maker", r.get("openMakerFee")))))
                taker = r.get("takerFeeRate", r.get("realTakerFee", r.get("takerFee", r.get("taker", r.get("openTakerFee")))))
                try:
                    maker_f = float(maker)
                except Exception:
                    maker_f = 1.0
                try:
                    taker_f = float(taker)
                except Exception:
                    taker_f = 1.0
                is_zero_raw = r.get("isZeroFeeRate")
                is_zero = None if is_zero_raw is None else bool(is_zero_raw)
                rates[sid] = {
                    "maker": maker_f,
                    "taker": taker_f,
                    "is_zero": is_zero,
                    "source": "/api/v1/private/account/contract/fee_rate",
                    "raw": r,
                }
        except Exception as e:
            log_error("mexc_contract_fee_rate_failed", e, symbol=symbol or "")
        return rates

    async def fetch_zero_fee_symbols(self, symbol: str | None = None) -> list[str]:
        """Return symbols from MEXC's dedicated zero-fee contracts endpoint."""
        query = {"symbol": self.contract_id(symbol)} if symbol else {}
        try:
            out = await self.private("GET", "/api/v1/private/account/contract/zero_fee_rate", query=query)
            data = out.get("data") if isinstance(out, dict) else out
            if isinstance(data, dict):
                contracts = data.get("contracts") or data.get("list") or data.get("rows") or []
            else:
                contracts = data or []
            if isinstance(contracts, dict):
                contracts = [contracts]
            id_map: dict[str, str] | None = None
            symbols: list[str] = []
            for item in contracts if isinstance(contracts, list) else []:
                if not isinstance(item, dict):
                    continue
                sid = self.contract_id(item.get("symbol") or item.get("contract") or item.get("contractName") or "")
                if (not sid or sid == "_USDT") and item.get("contractId") not in (None, ""):
                    if id_map is None:
                        id_map = await self._contract_id_to_symbol_map()
                    try:
                        sid = id_map.get(str(int(float(item.get("contractId")))), "")
                    except Exception:
                        sid = id_map.get(str(item.get("contractId")), "")
                if sid and sid != "_USDT":
                    symbols.append(sid)
            return sorted(set(symbols))
        except Exception as e:
            log_error("mexc_zero_fee_rate_failed", e, symbol=symbol or "")
            return []

    async def verified_zero_fee_symbols(self, max_symbols: int = 0) -> list[str]:
        """Return API-confirmed zero-fee futures symbols.

        Preferred source is the dedicated MEXC contract zero-fee endpoint.
        Fallbacks use contract fee_rate.isZeroFeeRate / maker+taker==0 and then
        the old account-level endpoints. max_symbols <= 0 returns the full
        zero-fee universe.
        """
        zeros = await self.fetch_zero_fee_symbols()

        if not zeros:
            rates = await self.fetch_contract_fee_rates()
            for sym, fr in rates.items():
                try:
                    if fr.get("is_zero") is True or (abs(float(fr.get("maker", 1))) <= 1e-12 and abs(float(fr.get("taker", 1))) <= 1e-12):
                        zeros.append(sym)
                except Exception:
                    pass

        if not zeros:
            rates = await self.fetch_fee_rates()
            for sym, fr in rates.items():
                try:
                    if abs(float(fr.get("maker", 1))) <= 1e-12 and abs(float(fr.get("taker", 1))) <= 1e-12:
                        zeros.append(sym)
                except Exception:
                    pass

        zeros = sorted(set(zeros))
        try:
            tickers = await self.all_tickers()
            if tickers:
                zeros.sort(key=lambda x: float((tickers.get(x) or {}).get("quoteVolume") or 0.0), reverse=True)
        except Exception:
            pass
        limit = int(max_symbols or 0)
        out = zeros[:limit] if limit > 0 else zeros
        log_event("mexc_verified_zero_fee_symbols", total_zero_fee=len(zeros), returned=len(out), max_symbols=max_symbols, first_symbols=out[:30])
        return out
