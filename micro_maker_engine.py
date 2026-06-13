from __future__ import annotations

import asyncio
import math
import random
import time
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Any

from config_store import ConfigStore, parse_symbols
from mexc_client import MexcFuturesClient
from mexc_ws import MexcDepthWebSocket
from full_logger import log_event, log_debug, log_error

Notify = Callable[[str], Awaitable[None]]


@dataclass
class EngineStats:
    started_ts: float = 0.0
    start_equity: float = 0.0
    estimated_pnl: float = 0.0
    trades: int = 0
    wins: int = 0
    losses: int = 0
    consecutive_losses: int = 0
    api_errors: int = 0
    last_action: str = "idle"
    last_error: str = ""
    trade_timestamps: list[float] = field(default_factory=list)
    current_symbols: list[str] = field(default_factory=list)
    last_scan_ts: float = 0.0
    last_scan_rows: list[dict[str, Any]] = field(default_factory=list)
    last_scan_reject_counts: dict[str, int] = field(default_factory=dict)
    open_position_symbols: list[str] = field(default_factory=list)
    market_data_source: str = "REST"
    ws_books: int = 0
    ws_fresh_books: int = 0
    zero_fee_universe_count: int = 0
    ignored_symbols_count: int = 0


class MicroMakerEngine:
    def __init__(self, store: ConfigStore, notify: Notify | None = None):
        self.store = store
        self.notify = notify or (lambda text: asyncio.sleep(0))
        self.client: MexcFuturesClient | None = None
        self.task: asyncio.Task | None = None
        self.running = False
        self.active_tasks: dict[str, asyncio.Task] = {}
        self.zero_fee_cache: list[str] = []
        self.zero_fee_ts = 0.0
        self.last_selected_symbols: list[str] = []
        self.last_symbol_switch_ts = 0.0
        self.stats = EngineStats()
        self.depth_ws: MexcDepthWebSocket | None = None
        self._last_logged_scan_ts = 0.0
        self.cooldown_until_ts = 0.0
        self.last_trade_closed_ts = 0.0
        log_event("engine_init", version=self._settings().get("bot_version"))

    def _log_event(self, event: str, **data: Any) -> None:
        if bool(self._settings().get("full_log_enabled", True)):
            log_event(event, **data)

    def _log_debug(self, event: str, **data: Any) -> None:
        if bool(self._settings().get("full_log_enabled", True)):
            log_debug(event, **data)

    def _log_error(self, event: str, exc: BaseException | None = None, **data: Any) -> None:
        if bool(self._settings().get("full_log_enabled", True)):
            log_error(event, exc, **data)

    def is_running(self) -> bool:
        return bool(self.running and self.task and not self.task.done())

    async def _notify(self, text: str) -> None:
        self.stats.last_action = str(text or "")[:240]
        self._log_event("notify", text=self.stats.last_action)
        try:
            await self.notify(text)
        except Exception:
            pass

    def _settings(self) -> dict[str, Any]:
        return self.store.load()

    def _ignored_symbols(self, s: dict[str, Any] | None = None) -> dict[str, Any]:
        raw = (s or self._settings()).get("ignored_symbols") or {}
        return raw if isinstance(raw, dict) else {}

    def _is_ignored_symbol(self, symbol: str, s: dict[str, Any] | None = None) -> bool:
        sid = MexcFuturesClient.contract_id(symbol)
        return sid in self._ignored_symbols(s)

    @staticmethod
    def _blocked_symbol(symbol: str) -> bool:
        # Per strategy rule: symbols containing STOCK are blocked. Metals, oil,
        # indexes and tokenized tickers without this substring remain allowed.
        return "STOCK" in str(symbol or "").upper()

    def _ignore_symbol(self, symbol: str, reason: str) -> None:
        """Persistently remove a bad symbol from scanner/trading.

        Used for regional restrictions, unsupported contracts and min/max
        margin/volume rejects. The entry stays until manual Clear ignore.
        """
        sid = MexcFuturesClient.contract_id(symbol)
        if not sid:
            return
        s = self._settings()
        ignored = dict(self._ignored_symbols(s))
        ignored[sid] = {"ts": time.time(), "reason": str(reason or "unknown")[:220]}
        max_items = max(50, int(s.get("max_ignored_symbols") or 1000))
        if len(ignored) > max_items:
            ordered = sorted(ignored.items(), key=lambda kv: float((kv[1] or {}).get("ts") or 0), reverse=True)[:max_items]
            ignored = dict(ordered)
        try:
            self.store.set("ignored_symbols", ignored)
        except Exception:
            pass
        self.stats.ignored_symbols_count = len(ignored)
        self.stats.last_action = f"ignored {sid}: {str(reason)[:120]}"
        self._log_event("symbol_ignored", symbol=sid, reason=reason, ignored_count=len(ignored))
        self.zero_fee_cache = [x for x in self.zero_fee_cache if x != sid]
        self.stats.last_scan_rows = [r for r in self.stats.last_scan_rows if r.get("symbol") != sid]

    @staticmethod
    def _is_symbol_reject_error(exc: Exception | str) -> bool:
        msg = str(exc).lower()
        keywords = (
            "region", "regional", "restricted", "restrict", "forbidden", "prohibit",
            "not support", "not supported", "not allowed", "not allow", "cannot trade",
            "contract not", "symbol not", "not exist", "does not exist",
            "min vol", "minimum vol", "min volume", "minimum volume",
            "max vol", "maximum vol", "max volume", "maximum volume",
            "min amount", "minimum amount", "max amount", "maximum amount",
            "min margin", "minimum margin", "max margin", "maximum margin",
            "leverage not", "max leverage", "minimum order", "maximum order",
        )
        return any(k in msg for k in keywords)

    def ignored_symbols_text(self, limit: int = 30) -> str:
        ignored = self._ignored_symbols()
        if not ignored:
            return "🚫 Ignored symbols: 0"
        rows = sorted(ignored.items(), key=lambda kv: float((kv[1] or {}).get("ts") or 0), reverse=True)[:limit]
        lines = [f"🚫 Ignored symbols: {len(ignored)}"]
        for sym, meta in rows:
            reason = str((meta or {}).get("reason") or "-")[:90]
            lines.append(f"- {sym}: {reason}")
        if len(ignored) > limit:
            lines.append(f"... ещё {len(ignored) - limit}")
        return "\n".join(lines)

    def clear_ignored_symbols(self) -> str:
        self.store.set("ignored_symbols", {})
        self.stats.ignored_symbols_count = 0
        self.zero_fee_ts = 0.0
        self.stats.last_action = "ignored symbols cleared"
        self._log_event("ignored_symbols_cleared")
        return "✅ Ignored symbols очищен. На следующем rescan бот снова проверит zero-fee universe."

    def _counter_value(self, key: str, default: float = 0.0) -> float:
        try:
            return float(self.store.load().get(key) or default)
        except Exception:
            return float(default)

    async def _read_usdt_total(self, client: MexcFuturesClient) -> float | None:
        """Return live total USDT equity, or None on read failure."""
        try:
            bal = await client.fetch_balance()
            usdt = bal.get("USDT") or {}
            total = float(usdt.get("total") or 0.0)
            return total if total > 0 else 0.0
        except Exception as e:
            self._log_error("real_pnl_balance_read_error", e)
            return None

    @staticmethod
    def _position_fee_usdt(pos: dict[str, Any]) -> float:
        """Extract actual fee already reported by MEXC for an open position."""
        raw = pos.get("raw") if isinstance(pos, dict) else {}
        if not isinstance(raw, dict):
            raw = {}
        for key in ("totalFee", "fee", "holdFee"):
            try:
                val = abs(float(raw.get(key) or 0.0))
                if val > 0:
                    return val
            except Exception:
                pass
        return 0.0

    async def _fee_aware_target_ticks(self, symbol: str, contracts: int, tick: float, base_target_ticks: int, pos: dict[str, Any], s: dict[str, Any], client: MexcFuturesClient) -> tuple[int, dict[str, Any]]:
        """Lift target ticks if real MEXC fees make 1 tick unprofitable."""
        info: dict[str, Any] = {"enabled": bool(s.get("fee_aware_target", True)), "base_target_ticks": base_target_ticks}
        if not bool(s.get("fee_aware_target", True)):
            return base_target_ticks, info
        try:
            amount = await client.amount_from_contracts(symbol, contracts)
            tick_value = abs(float(tick or 0.0) * float(amount or 0.0))
            entry_fee = self._position_fee_usdt(pos)
            # If the entry fee is visible, assume the close maker fee will be similar.
            # With actual zero-fee this stays 0 and the target remains one tick.
            estimated_round_fee = entry_fee * 2.0
            min_net = max(0.0, float(s.get("min_net_profit_usdt") or 0.0))
            min_gross = max(0.0, float(s.get("min_gross_profit_usdt") or 0.0))
            needed_ticks = base_target_ticks
            if tick_value > 0:
                # Require enough ticks to make the trade meaningful even when fees are zero.
                # On SOL one tick can be only about 0.001 USDT; that is too small and gets
                # eaten by balance noise / close execution. v0027 raises TP to a real amount.
                gross_ticks = int(math.ceil(min_gross / tick_value)) if min_gross > 0 else base_target_ticks
                fee_ticks = int(math.ceil((estimated_round_fee + min_net) / tick_value)) if estimated_round_fee > 0 else base_target_ticks
                needed_ticks = max(base_target_ticks, gross_ticks, fee_ticks)
            max_ticks = max(base_target_ticks, int(s.get("max_fee_target_ticks") or 18))
            target_ticks = max(base_target_ticks, min(max_ticks, needed_ticks))
            info.update({
                "amount": amount,
                "tick_value": tick_value,
                "entry_fee": entry_fee,
                "estimated_round_fee": estimated_round_fee,
                "min_net_profit_usdt": min_net,
                "min_gross_profit_usdt": min_gross,
                "needed_ticks": needed_ticks,
                "max_fee_target_ticks": max_ticks,
                "target_ticks": target_ticks,
            })
            return target_ticks, info
        except Exception as e:
            info.update({"error": str(e)[:180]})
            return base_target_ticks, info

    @staticmethod
    def _cancel_response_has_order_closed(res: Any) -> bool:
        try:
            rows = res.get("data") if isinstance(res, dict) else None
            if isinstance(rows, dict):
                rows = [rows]
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                code = int(row.get("errorCode") or 0)
                msg = str(row.get("errorMsg") or "").lower()
                if code == 2041 or "state cannot be cancelled" in msg:
                    return True
        except Exception:
            pass
        return False

    def _increment_total_trade_counters(self, pnl: float, is_win: bool | None = None) -> None:
        """Persist total closed-trade counters across restarts."""
        try:
            s = self.store.load()
            total = int(s.get("total_trades_count") or 0) + 1
            wins = int(s.get("total_wins_count") or 0)
            losses = int(s.get("total_losses_count") or 0)
            if is_win is None:
                is_win = pnl > 0
            if is_win:
                wins += 1
            else:
                losses += 1
            total_pnl = float(s.get("total_estimated_pnl_usdt") or 0.0) + float(pnl)
            self.store.update({
                "total_trades_count": total,
                "total_wins_count": wins,
                "total_losses_count": losses,
                "total_estimated_pnl_usdt": total_pnl,
            })
            self._log_event("trade_counter_updated", pnl=pnl, total=total, wins=wins, losses=losses, total_pnl=total_pnl)
        except Exception as e:
            self.stats.last_error = f"counter: {str(e)[:180]}"
            self._log_error("trade_counter_error", e, pnl=pnl)

    def trades_counter_text(self) -> str:
        s = self._settings()
        total = int(s.get("total_trades_count") or 0)
        total_wins = int(s.get("total_wins_count") or 0)
        total_losses = int(s.get("total_losses_count") or 0)
        total_pnl = float(s.get("total_estimated_pnl_usdt") or 0.0)
        session_wr = (self.stats.wins / self.stats.trades * 100.0) if self.stats.trades else 0.0
        total_wr = (total_wins / total * 100.0) if total else 0.0
        return (
            "📒 Trade counter\n"
            f"Session closed trades: {self.stats.trades} | + / -: {self.stats.wins}/{self.stats.losses} | WR: {session_wr:.1f}%\n"
            f"Total closed trades: {total} | + / -: {total_wins}/{total_losses} | WR: {total_wr:.1f}%\n"
            f"Session Real/Real/Approx PnL: {self.stats.estimated_pnl:.5f} USDT\n"
            f"Total Real/Real/Approx PnL: {total_pnl:.5f} USDT\n"
            f"Loss streak: {self.stats.consecutive_losses} | API errors: {self.stats.api_errors}"
        )

    async def _ensure_client(self) -> MexcFuturesClient:
        s = self._settings()
        if self.client:
            self.client.update_settings(s)
            return self.client
        key, secret = str(s.get("mexc_api_key") or "").strip(), str(s.get("mexc_api_secret") or "").strip()
        if not key or not secret:
            raise RuntimeError("MEXC API не задан. Отправь: /api set API_KEY API_SECRET")
        self.client = MexcFuturesClient(key, secret, settings=s)
        await self.client.sync_time()
        self._log_event("mexc_client_ready", key_saved=bool(key), time_diff_ms=self.client.time_difference_ms, rest_base=self.client.base_url)
        return self.client

    async def _ensure_market_ws(self, symbols: list[str], s: dict[str, Any]) -> None:
        """Start/refresh WS subscriptions for fast depth scanning."""
        if str(s.get("market_data_mode") or "websocket").lower() != "websocket" or not bool(s.get("ws_depth_enabled")):
            return
        limit = max(1, int(s.get("ws_depth_max_symbols") or s.get("max_zero_fee_scan_symbols") or 80))
        symbols = [MexcFuturesClient.contract_id(x) for x in symbols if x][:limit]
        if not symbols:
            return
        if self.depth_ws is None:
            self.depth_ws = MexcDepthWebSocket(settings=s)
            self._log_event("ws_depth_created", endpoint=self.depth_ws.endpoint)
        else:
            self.depth_ws.update_settings(s)
        await self.depth_ws.set_symbols(symbols)
        self._log_debug("ws_depth_symbols_set", count=len(symbols), symbols=symbols[:20])
        self.stats.market_data_source = "WS"

    async def _stop_market_ws(self) -> None:
        if self.depth_ws:
            try:
                self._log_event("ws_depth_stopping", stats=self.depth_ws.stats())
                await self.depth_ws.close()
            except Exception as e:
                self._log_error("ws_depth_stop_error", e)
        self.depth_ws = None
        self.stats.market_data_source = "REST"

    async def _depth(self, symbol: str, limit: int = 20, *, allow_rest_fallback: bool = True) -> dict[str, Any]:
        """Return freshest available order book: WS cache first, REST fallback second."""
        s = self._settings()
        max_age_ms = int(float(s.get("ws_book_stale_ms") or 700))
        if str(s.get("market_data_mode") or "websocket").lower() == "websocket" and bool(s.get("ws_depth_enabled")) and self.depth_ws:
            book = self.depth_ws.get_book(symbol, limit=limit, max_age_ms=max_age_ms)
            if book:
                self.stats.market_data_source = f"WS {book.get('age_ms', 0):.0f}ms"
                return book
        if not allow_rest_fallback or not bool(s.get("rest_depth_fallback")):
            return {"symbol": MexcFuturesClient.contract_id(symbol), "bids": [], "asks": [], "source": "none"}
        client = await self._ensure_client()
        self._log_debug("depth_rest_fallback", symbol=symbol, limit=limit)
        book = await client.depth(symbol, limit=limit)
        book["source"] = "rest"
        self.stats.market_data_source = "REST fallback"
        if self.depth_ws:
            try:
                self.depth_ws.seed_book(symbol, book)
            except Exception as e:
                self._log_error("risk_guard_balance_error", e)
                pass
        return book

    def _market_data_status(self) -> str:
        if not self.depth_ws:
            return str(self.stats.market_data_source or "REST")
        st = self.depth_ws.stats()
        self.stats.ws_books = int(st.get("books") or 0)
        self.stats.ws_fresh_books = int(st.get("fresh_books") or 0)
        err = str(st.get("last_error") or "")[:60]
        base = f"WS {st.get('fresh_books')}/{st.get('subscribed')} fresh, books {st.get('books')}, msg age {float(st.get('last_msg_age') or 0):.1f}s"
        if err:
            base += f", err: {err}"
        return base

    async def _position_margin_usdt(self, s: dict[str, Any]) -> tuple[float, str]:
        """Return margin for one new trade.

        Default behavior: one trade uses position_margin_percent of TOTAL USDT equity.
        If available balance is lower than calculated margin, cap to 95% of available
        to avoid order rejections when other positions/orders already reserve margin.
        """
        mode = str(s.get("position_size_mode") or "balance_percent").lower()
        if mode == "fixed_usdt":
            margin = max(0.0, float(s.get("margin_per_position_usdt") or 0))
            self._log_debug("position_margin_calc", mode=mode, margin=margin)
            return margin, f"fixed {margin:.4f} USDT"
        client = await self._ensure_client()
        bal = await client.fetch_balance()
        usdt = bal.get("USDT") or {}
        total = float(usdt.get("total") or 0)
        free = float(usdt.get("free") or 0)
        percent = max(0.0, float(s.get("position_margin_percent") or 10.0))
        desired = total * percent / 100.0
        if desired <= 0:
            return 0.0, f"{percent:g}% of total, but total={total:.4f}"
        margin = desired
        capped = False
        if free > 0 and margin > free * 0.95:
            margin = free * 0.95
            capped = True
        note = f"{percent:g}% of total equity: total={total:.4f}, free={free:.4f}, margin={margin:.4f} USDT"
        if capped:
            note += " (capped by available balance)"
        self._log_debug("position_margin_calc", mode=mode, total=total, free=free, percent=percent, margin=margin, note=note)
        return max(0.0, margin), note

    async def start(self) -> str:
        self._log_event("start_requested")
        if self.is_running():
            self._log_event("start_skipped_already_running")
            return "Micro Maker уже работает."
        try:
            self.client = None
            await self._ensure_client()
        except Exception as e:
            self._log_error("start_failed_ensure_client", e)
            return f"❌ {e}"
        self.running = True
        self.store.set("live_enabled", True)
        self.stats = EngineStats(started_ts=time.time())
        self.last_selected_symbols = []
        self.last_symbol_switch_ts = 0.0
        self.cooldown_until_ts = 0.0
        self.last_trade_closed_ts = 0.0
        try:
            bal = await self.client.fetch_balance() if self.client else {}
            self.stats.start_equity = float((bal.get("USDT") or {}).get("total") or 0)
        except Exception as e:
            self.stats.last_error = f"balance: {e}"
            self._log_error("start_balance_error", e)
        self.task = asyncio.create_task(self._run_loop(), name="micro_maker_loop")
        self._log_event("start_success", start_equity=self.stats.start_equity)
        return "▶️ Micro Maker LIVE v0028 запущен. Basket Harvest: 3 позиции по 10%, стопов нет, закрытие только по +$0.01."

    async def stop(self, close_positions: bool = False) -> str:
        self._log_event("stop_requested", close_positions=close_positions, active_tasks=list(self.active_tasks.keys()))
        self.running = False
        self.store.set("live_enabled", False)
        for t in list(self.active_tasks.values()):
            if not t.done():
                t.cancel()
        self.active_tasks.clear()
        self.stats.open_position_symbols.clear()
        if self.task and not self.task.done():
            self.task.cancel()
        client = self.client
        if client:
            try:
                if close_positions:
                    s = self._settings()
                    await client.hard_close_all(leverage=int(s.get("leverage") or 5), open_type=int(s.get("open_type") or 1))
                else:
                    await client.cancel_all_orders(None)
            except Exception as e:
                self.stats.last_error = str(e)[:240]
                self._log_error("stop_cleanup_error", e, close_positions=close_positions)
        await self._stop_market_ws()
        if close_positions:
            self._log_event("stop_done", close_positions=True)
            return "🚨 Risk Stop: позиции закрыты market + ордера отменены."
        self._log_event("stop_done", close_positions=False)
        return "⏸ Stop: торговля и фоновый скан остановлены. Активные ордера отменены, позиции market не закрывались."

    async def close_all(self) -> str:
        """Stop strategy, cancel all active/limit orders and close every open position by market."""
        self._log_event("close_all_requested", active_tasks=list(self.active_tasks.keys()))
        self.running = False
        self.store.set("live_enabled", False)
        for t in list(self.active_tasks.values()):
            if not t.done():
                t.cancel()
        self.active_tasks.clear()
        self.stats.open_position_symbols.clear()
        if self.task and not self.task.done():
            self.task.cancel()
        try:
            client = await self._ensure_client()
        except Exception as e:
            self._log_error("close_all_no_client", e)
            return f"❌ Close All не выполнен: {e}"
        s = self._settings()
        try:
            res = await client.hard_close_all(leverage=int(s.get("leverage") or 5), open_type=int(s.get("open_type") or 1))
            self._log_event("close_all_result", result=res)
            await self._stop_market_ws()
            errs = res.get("errors") or []
            if errs:
                self.stats.last_error = str(errs[:3])[:240]
                return f"⚠️ Close All выполнен частично: позиции/ордера обработаны, ошибок={len(errs)}. Последняя: {self.stats.last_error}"
            return "✅ Close All выполнен: все лимитные/активные ордера отменены, все открытые позиции закрыты market."
        except Exception as e:
            self.stats.last_error = str(e)[:240]
            self._log_error("close_all_error", e)
            return f"❌ Close All error: {self.stats.last_error}"

    def _local_time_text(self, s: dict[str, Any] | None = None) -> str:
        try:
            settings = s or self._settings()
            off = float(settings.get("telegram_time_offset_hours", 3.0) or 0)
            return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=off))).strftime("%H:%M:%S")
        except Exception:
            return time.strftime("%H:%M:%S")

    def quick_status_text(self) -> str:
        """Fast status for the Telegram live panel. No REST balance/positions calls here."""
        s = self._settings()
        uptime = 0.0
        if self.stats.started_ts:
            uptime = max(0.0, time.time() - self.stats.started_ts)
        h = int(uptime // 3600)
        m = int((uptime % 3600) // 60)
        sec = int(uptime % 60)
        age = time.time() - self.stats.last_scan_ts if self.stats.last_scan_ts else 0.0
        top = self._format_scan_rows(limit=5)
        rejects = self._format_reject_counts()
        current = ", ".join(self.stats.current_symbols) or "-"
        opened = ", ".join(self.stats.open_position_symbols) or "-"
        state = "RUNNING" if self.is_running() else "STOPPED"
        last_update = self._local_time_text(s)
        total_trades = int(s.get("total_trades_count") or 0)
        total_wins = int(s.get("total_wins_count") or 0)
        total_losses = int(s.get("total_losses_count") or 0)
        total_pnl = float(s.get("total_estimated_pnl_usdt") or 0.0)
        cooldown_left = max(0.0, self.cooldown_until_ts - time.time())
        cooldown_txt = f" | Cooldown: {cooldown_left:.0f}s" if cooldown_left > 0 else ""
        return (
            f"🤖 MEXC Micro Maker LIVE {s.get('bot_version', 'v0028')}\n"
            f"State: {state} | Updated: {last_update}{cooldown_txt}\n"
            f"Uptime: {h:02d}:{m:02d}:{sec:02d}\n\n"
            f"⚙️ {s.get('leverage')}x | Size: {s.get('position_margin_percent', 10)}% total | "
            f"Pos: {s.get('max_positions')} | Symbols: {s.get('symbols_limit')}\n" +
            (f"🎯 Basket: {s.get('basket_positions', s.get('max_positions'))} pos × {s.get('position_margin_percent', 10)}% | Target: +${float(s.get('basket_target_profit_usdt') or 0.01):.3f} | Stop: OFF | "
             if bool(s.get('basket_harvest_enabled')) else
             f"🎯 TP/SL: {s.get('target_ticks')}/{s.get('stop_ticks')} ticks | ")
            + f"Emergency: {'ON' if s.get('emergency_market_close') else 'OFF'} | Profile: {s.get('trade_profile', '-')}\n"
            f"🔎 Scanner: {'AUTO' if s.get('auto_select_symbols') else 'MANUAL'} | "
            f"ZeroFee: {'ON' if s.get('only_zero_fee') else 'OFF'} | age {age:.1f}s | rescan {s.get('zero_fee_rescan_sec')}s\n"
            f"🌐 Universe: {self.stats.zero_fee_universe_count or len(self.zero_fee_cache)} zero-fee | "
            f"active {s.get('max_zero_fee_scan_symbols')} | ignored {self.stats.ignored_symbols_count or len(self._ignored_symbols(s))}\n"
            f"⚡ Market data: {self._market_data_status()}\n"
            f"📌 Current: {current} | Open: {opened}\n\n"
            f"📈 Session: {self.stats.trades} trades | + / -: {self.stats.wins}/{self.stats.losses} | "
            f"Real/Approx PnL: {self.stats.estimated_pnl:.5f} USDT\n"
            f"📒 Total: {total_trades} trades | + / -: {total_wins}/{total_losses} | "
            f"Real/Approx PnL: {total_pnl:.5f} USDT\n"
            f"Loss streak: {self.stats.consecutive_losses} | API errors: {self.stats.api_errors}\n"
            f"Last: {self.stats.last_action or '-'}\n"
            f"Error: {self.stats.last_error or '-'}\n\n"
            f"🏆 Top scan:\n{top}\n"
            f"Rejects: {rejects}"
        )

    async def status_text(self) -> str:
        s = self._settings()
        bal_txt = "balance: n/a"
        pos_txt = "positions: n/a"
        client = self.client
        if client:
            try:
                bal = await client.fetch_balance()
                usdt = bal.get("USDT") or {}
                bal_txt = f"USDT total={float(usdt.get('total') or 0):.4f} free={float(usdt.get('free') or 0):.4f} used={float(usdt.get('used') or 0):.4f}"
            except Exception as e:
                bal_txt = f"balance error: {str(e)[:120]}"
            try:
                pos = await client.fetch_positions()
                if pos:
                    pos_txt = "\n".join([f"{p['symbol']} {p['side']} contracts={p['contracts']} entry={p['entryPrice']}" for p in pos[:8]])
                else:
                    pos_txt = "positions: none"
            except Exception as e:
                pos_txt = f"positions error: {str(e)[:120]}"
        top = self._format_scan_rows(limit=5)
        rejects = self._format_reject_counts()
        age = time.time() - self.stats.last_scan_ts if self.stats.last_scan_ts else 0
        total_trades = int(s.get("total_trades_count") or 0)
        total_wins = int(s.get("total_wins_count") or 0)
        total_losses = int(s.get("total_losses_count") or 0)
        total_pnl = float(s.get("total_estimated_pnl_usdt") or 0.0)
        cooldown_left = max(0.0, self.cooldown_until_ts - time.time())
        cooldown_txt = f" | Cooldown: {cooldown_left:.0f}s" if cooldown_left > 0 else ""
        return (
            "📊 Micro Maker Status\n\n"
            f"State: {'RUNNING' if self.is_running() else 'STOPPED'}\n"
            f"Active tasks: {len(self.active_tasks)} | Current: {', '.join(self.stats.current_symbols) or '-'}\n"
            f"Version: {s.get('bot_version', 'v0028')}\n"
            f"Leverage: {s.get('leverage')}x | One trade size: {s.get('position_margin_percent', 10)}% of TOTAL USDT equity\n"
            f"Max positions: {s.get('max_positions')} | Symbols limit: {s.get('symbols_limit')}\n"
            f"Scanner: {'AUTO' if s.get('auto_select_symbols') else 'MANUAL'} | ZeroFee: {'ON' if s.get('only_zero_fee') else 'OFF'} | scan age: {age:.1f}s | rescan: {s.get('zero_fee_rescan_sec')}s\n"
            f"Zero-fee universe: {self.stats.zero_fee_universe_count or len(self.zero_fee_cache)} | active candidates: {s.get('max_zero_fee_scan_symbols')} | ignored: {self.stats.ignored_symbols_count or len(self._ignored_symbols(s))}\n"
            f"Market data: {self._market_data_status()} | mode={s.get('market_data_mode')}\n" +
            (f"Basket target: +${float(s.get('basket_target_profit_usdt') or 0.01):.3f} | Stop: OFF | Emergency: {'ON' if s.get('emergency_market_close') else 'OFF'}\n"
             if bool(s.get('basket_harvest_enabled')) else
             f"Target/Stop: {s.get('target_ticks')}/{s.get('stop_ticks')} ticks | Emergency: {'ON' if s.get('emergency_market_close') else 'OFF'}\n") +
            f"Session trades: {self.stats.trades} | + / -: {self.stats.wins}/{self.stats.losses} | Real/Approx PnL: {self.stats.estimated_pnl:.5f} USDT\n"
            f"Total trades: {total_trades} | + / -: {total_wins}/{total_losses} | Total Real/Real/Approx PnL: {total_pnl:.5f} USDT\n"
            f"Consecutive losses: {self.stats.consecutive_losses} | API errors: {self.stats.api_errors}\n"
            f"Last: {self.stats.last_action}\n"
            f"Error: {self.stats.last_error or '-'}\n\n"
            f"Top scan:\n{top}\n\n"
            f"{bal_txt}\n{pos_txt}"
        )

    async def scan_now_text(self) -> str:
        self._log_event("scan_now_requested")
        try:
            s = self._settings()
            await self._ensure_client()
            await self._refresh_market_scan(s, force=True)
            rows = self._format_scan_rows(limit=10)
            universe = self.stats.zero_fee_universe_count or len(self.zero_fee_cache)
            ignored = self.stats.ignored_symbols_count or len(self._ignored_symbols(s))
            header = (
                "🔍 Scan Now\n"
                f"Zero-fee universe: {universe} | active candidates: {s.get('max_zero_fee_scan_symbols')} | ignored: {ignored}\n"
                f"Rescan every: {s.get('zero_fee_rescan_sec')} sec | Market data: {self._market_data_status()}\n\n"
            )
            return header + self.trades_counter_text() + "\n\n🏆 Top scan:\n" + (rows if rows.strip() else "Подходящих монет не найдено.")
        except Exception as e:
            self.stats.last_error = str(e)[:240]
            self._log_error("scan_now_error", e)
            return f"❌ Scan error: {self.stats.last_error}"

    def _format_scan_rows(self, limit: int = 5) -> str:
        rows = self.stats.last_scan_rows[:limit]
        if not rows:
            return "-"
        lines = []
        for i, r in enumerate(rows, 1):
            lines.append(
                f"{i}. {r['symbol']} score={r['score']:.1f} side={r['bias']} "
                f"spr={r['spread_ticks']:.1f}t depth={r['depth_min']:.0f}$ imb={r['imbalance']:.2f} src={r.get('source','-')}"
            )
        return "\n".join(lines)

    def _format_reject_counts(self) -> str:
        counts = self.stats.last_scan_reject_counts or {}
        if not counts:
            return "-"
        return ", ".join(f"{k}={v}" for k, v in sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[:5])

    async def _run_loop(self) -> None:
        self._log_event("run_loop_started")
        await self._notify("✅ LIVE loop v0028 started. Basket Harvest активен: 3 позиции по 10%, стопов нет, закрытие только по +$0.01.")
        while self.running:
            try:
                s = self._settings()
                await self._risk_guard(s)
                await self._cleanup_tasks()
                # Background scan runs even when all slots are busy. A better coin will be used as soon as capacity frees.
                await self._refresh_market_scan(s, force=False)
                capacity = max(0, min(int(s.get("max_positions") or 1), int(s.get("symbols_limit") or 1)) - len(self.active_tasks))
                if capacity > 0:
                    symbols = await self._select_symbols(s)
                    for sym in symbols:
                        if capacity <= 0:
                            break
                        if sym in self.active_tasks:
                            continue
                        task = asyncio.create_task(self._trade_cycle(sym), name=f"trade_{sym}")
                        self.active_tasks[sym] = task
                        capacity -= 1
                await asyncio.sleep(max(0.05, float(s.get("cycle_sleep_ms") or 250) / 1000.0))
            except asyncio.CancelledError:
                self._log_event("run_loop_cancelled")
                break
            except Exception as e:
                self.stats.api_errors += 1
                self.stats.last_error = str(e)[:240]
                self._log_error("run_loop_error", e, api_errors=self.stats.api_errors)
                if self.stats.api_errors >= int(self._settings().get("stop_on_api_errors") or 8):
                    self._log_event("risk_stop_api_errors", api_errors=self.stats.api_errors, last_error=self.stats.last_error)
                    await self._notify(f"🚨 Too many API errors. Risk stop. Last: {self.stats.last_error}")
                    await self.stop(close_positions=True)
                    break
                await asyncio.sleep(1.0)

    async def _cleanup_tasks(self) -> None:
        for sym, task in list(self.active_tasks.items()):
            if task.done():
                self.active_tasks.pop(sym, None)
                if sym in self.stats.open_position_symbols:
                    self.stats.open_position_symbols.remove(sym)
                try:
                    task.result()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    self.stats.api_errors += 1
                    self.stats.last_error = f"{sym}: {str(e)[:180]}"
                    self._log_error("trade_task_error", e, symbol=sym)

    async def _risk_guard(self, s: dict[str, Any]) -> None:
        if self.stats.consecutive_losses >= int(s.get("max_consecutive_losses") or 5):
            self._log_event("risk_stop_consecutive_losses", consecutive_losses=self.stats.consecutive_losses)
            await self._notify("🚨 Max consecutive losses reached. Risk stop.")
            await self.stop(close_positions=True)
            return
        if self.stats.start_equity > 0 and self.client:
            try:
                bal = await self.client.fetch_balance()
                equity = float((bal.get("USDT") or {}).get("total") or 0)
                if equity > 0 and self.stats.start_equity - equity >= float(s.get("daily_loss_limit_usdt") or 2):
                    self._log_event("risk_stop_daily_loss", start_equity=self.stats.start_equity, equity=equity, limit=s.get("daily_loss_limit_usdt"))
                    await self._notify(f"🛑 Daily loss limit hit: start={self.stats.start_equity:.4f}, now={equity:.4f}")
                    await self.stop(close_positions=True)
            except Exception:
                pass
        now = time.time()
        self.stats.trade_timestamps = [x for x in self.stats.trade_timestamps if now - x < 3600]

    async def _symbol_pool(self, s: dict[str, Any]) -> list[str]:
        client = await self._ensure_client()
        allowed = parse_symbols(str(s.get("allowed_symbols") or ""))
        allowed_set = set(allowed)

        # Manual whitelist mode without zero-fee filter. Still remove ignored symbols.
        if not (s.get("auto_select_symbols") and s.get("only_zero_fee")):
            pool = [x for x in allowed if not self._is_ignored_symbol(x, s)]
            out = pool[: max(1, int(s.get("max_zero_fee_scan_symbols") or 80))]
            self._log_event("symbol_pool_manual", allowed=len(allowed), out_count=len(out), symbols=out[:30])
            return out

        now = time.time()
        rescan_sec = max(15.0, float(s.get("zero_fee_rescan_sec") or 60.0))
        should_rescan = not self.zero_fee_cache or now - self.zero_fee_ts >= rescan_sec
        if should_rescan:
            previous = list(self.zero_fee_cache)
            self._log_event("zero_fee_rescan_start", previous_count=len(previous), rescan_sec=rescan_sec)
            try:
                universe_limit = int(s.get("zero_fee_universe_max_symbols") or 0)
                # 0 means full API-confirmed zero-fee universe. verified_zero_fee_symbols
                # pre-sorts by 24h volume when public ticker data is available.
                fresh = await client.verified_zero_fee_symbols(universe_limit)
                ignored = self._ignored_symbols(s)
                blocked = [x for x in fresh if self._blocked_symbol(x)]
                self.zero_fee_cache = [x for x in fresh if x and x not in ignored and not self._blocked_symbol(x)]
                self.zero_fee_ts = now
                self.stats.zero_fee_universe_count = len(fresh)
                self.stats.ignored_symbols_count = len(ignored)
                self.stats.last_action = (
                    f"zero-fee universe rebuilt: all={len(fresh)}, "
                    f"ignored={len(ignored)}, blocked={len(blocked)}, usable={len(self.zero_fee_cache)}"
                )
                self._log_event("zero_fee_rescan_done", all_count=len(fresh), ignored=len(ignored), blocked=len(blocked), usable=len(self.zero_fee_cache), first_symbols=self.zero_fee_cache[:30])
            except Exception as e:
                # Good cache behavior: never destroy a working universe just because
                # one rescan failed. Keep the previous cache and wait until the next
                # rescan window instead of hammering API every trade loop.
                self.zero_fee_ts = now
                self.stats.last_error = f"zero_fee rescan failed, kept cache: {str(e)[:160]}"
                self._log_error("zero_fee_rescan_failed", e, previous_count=len(previous))
                if previous:
                    self.zero_fee_cache = previous
                else:
                    self.zero_fee_cache = []

        if not self.zero_fee_cache and not s.get("allow_manual_fee_fallback"):
            self.stats.last_action = "idle: no API-confirmed zero-fee symbols"
            return []

        ignored = self._ignored_symbols(s)
        pool = [x for x in self.zero_fee_cache if x not in ignored and not self._blocked_symbol(x) and (not allowed_set or x in allowed_set)]
        self.stats.ignored_symbols_count = len(ignored)
        self._log_debug("symbol_pool_active", zero_fee_cache=len(self.zero_fee_cache), pool_count=len(pool), ignored=len(ignored), allowed_filter=bool(allowed_set), first_symbols=pool[:30])
        # Active fast scan is intentionally capped; the full universe is rebuilt every
        # zero_fee_rescan_sec, while WS subscriptions stay on the fastest current window.
        return pool[: max(1, int(s.get("max_zero_fee_scan_symbols") or 80))]

    async def _refresh_market_scan(self, s: dict[str, Any], force: bool = False) -> list[dict[str, Any]]:
        now = time.time()
        interval = max(0.2, float(s.get("scan_interval_sec") or 1.0))
        if not force and self.stats.last_scan_rows and now - self.stats.last_scan_ts < interval:
            return self.stats.last_scan_rows
        client = await self._ensure_client()
        pool = await self._symbol_pool(s)
        if not pool:
            self.stats.last_scan_ts = now
            self.stats.last_scan_rows = []
            self._log_event("scan_no_pool", force=force)
            return []
        await self._ensure_market_ws(pool, s)
        if self.depth_ws:
            ws_st = self.depth_ws.stats()
            self.stats.ws_books = int(ws_st.get("books") or 0)
            self.stats.ws_fresh_books = int(ws_st.get("fresh_books") or 0)
            if self.stats.ws_books == 0:
                await asyncio.sleep(max(0.0, float(s.get("ws_warmup_ms") or 350) / 1000.0))

        try:
            margin_usdt, _ = await self._position_margin_usdt(s)
        except Exception:
            margin_usdt = 0.0
        leverage = max(1, int(s.get("leverage") or 5))
        notional = max(0.0, margin_usdt * leverage)
        depth_multiplier = max(1.0, float(s.get("min_depth_multiplier") or 3.0))
        required_depth = max(float(s.get("min_depth_usdt") or 0), notional * depth_multiplier)
        levels = max(1, min(20, int(s.get("score_top_levels") or 5)))
        min_volume = float(s.get("min_24h_volume_usdt") or 0)
        min_imbalance = float(s.get("min_imbalance_ratio") or 1.04)

        scan_details: list[dict[str, Any]] = []
        reject_counts: dict[str, int] = {}
        detail_limit = max(0, int(s.get("full_log_scan_symbol_limit") or 120))

        def add_scan_detail(sym: str, status: str, reason: str = "", **extra: Any) -> None:
            if status != "valid":
                reject_counts[reason or status] = reject_counts.get(reason or status, 0) + 1
            if bool(s.get("full_log_scan_details", True)) and len(scan_details) < detail_limit:
                row = {"symbol": sym, "status": status}
                if reason:
                    row["reason"] = reason
                row.update(extra)
                scan_details.append(row)

        self._log_debug("scan_start", force=force, pool_count=len(pool), pool_first=pool[:30], required_depth=required_depth, margin_usdt=margin_usdt, leverage=leverage, levels=levels, min_trade_score=s.get("min_trade_score"), ws_scan_mode=(str(s.get("market_data_mode") or "websocket").lower() == "websocket" and bool(s.get("ws_depth_enabled"))), ws_scan_rest_fallback_limit=s.get("ws_scan_rest_fallback_limit"))
        scored: list[dict[str, Any]] = []
        ws_scan_mode = (
            str(s.get("market_data_mode") or "websocket").lower() == "websocket"
            and bool(s.get("ws_depth_enabled"))
        )
        rest_fallback_budget = int(s.get("ws_scan_rest_fallback_limit") or 0) if ws_scan_mode else len(pool)
        for sym in pool:
            try:
                allow_scan_rest = (not ws_scan_mode) or rest_fallback_budget > 0
                book = await self._depth(sym, limit=max(10, levels), allow_rest_fallback=allow_scan_rest)
                if ws_scan_mode and book.get("source") == "rest" and rest_fallback_budget > 0:
                    rest_fallback_budget -= 1
                if not book["bids"] or not book["asks"]:
                    add_scan_detail(sym, "reject", "no_book", source=book.get("source"))
                    continue
                bid, ask = book["bids"][0][0], book["asks"][0][0]
                if bid <= 0 or ask <= 0 or ask <= bid:
                    add_scan_detail(sym, "reject", "bad_top_of_book", bid=bid, ask=ask, source=book.get("source"))
                    continue
                tick = await client.price_tick(sym)
                spread_ticks = (ask - bid) / max(tick, 1e-12)
                min_spread = float(s.get("min_spread_ticks") or 1)
                max_spread = float(s.get("max_spread_ticks") or 4)
                # Floating math can turn a true 1-tick spread into 0.999999999999.
                # Use a tiny epsilon so valid one-tick books are not rejected.
                if spread_ticks + 1e-9 < min_spread or spread_ticks > max_spread + 1e-9:
                    add_scan_detail(sym, "reject", "spread", bid=bid, ask=ask, tick=tick, spread_ticks=spread_ticks, min_spread=s.get("min_spread_ticks"), max_spread=s.get("max_spread_ticks"), source=book.get("source"))
                    continue
                contract_size = await client.contract_size(sym)
                depth_b = sum(p * q * contract_size for p, q in book["bids"][:levels])
                depth_a = sum(p * q * contract_size for p, q in book["asks"][:levels])
                depth_min = min(depth_a, depth_b)
                if depth_min < required_depth:
                    add_scan_detail(sym, "reject", "depth", bid=bid, ask=ask, spread_ticks=spread_ticks, depth_bid=depth_b, depth_ask=depth_a, depth_min=depth_min, required_depth=required_depth, source=book.get("source"))
                    continue
                imbalance = max(depth_b / max(depth_a, 1e-9), depth_a / max(depth_b, 1e-9))
                if imbalance < min_imbalance:
                    add_scan_detail(sym, "reject", "imbalance", bid=bid, ask=ask, spread_ticks=spread_ticks, depth_bid=depth_b, depth_ask=depth_a, depth_min=depth_min, imbalance=imbalance, min_imbalance=min_imbalance, source=book.get("source"))
                    continue
                bias = await self._choose_direction(sym, s, book)
                if not bias:
                    try:
                        em = await self._edge_metrics(sym, s, book)
                    except Exception:
                        em = {}
                    add_scan_detail(sym, "reject", "edge", bid=bid, ask=ask, spread_ticks=spread_ticks, depth_bid=depth_b, depth_ask=depth_a, depth_min=depth_min, imbalance=imbalance, source=book.get("source"), **em)
                    continue
                quote_volume = 0.0
                if min_volume > 0:
                    try:
                        t = await client.ticker(sym)
                        quote_volume = float(t.get("quoteVolume") or 0)
                    except Exception:
                        pass
                    if quote_volume > 0 and quote_volume < min_volume:
                        add_scan_detail(sym, "reject", "volume", quote_volume=quote_volume, min_volume=min_volume, source=book.get("source"))
                        continue

                depth_score = min(depth_min / max(required_depth, 1.0), 10.0) * 10.0
                spread_score = max(0.0, 12.0 - spread_ticks * 2.5)
                imbalance_score = min((imbalance - 1.0) * 40.0, 25.0)
                volume_score = min(math.log10(max(quote_volume, 1.0)) * 1.5, 12.0) if quote_volume > 0 else 0.0
                try:
                    em = await self._edge_metrics(sym, s, book)
                    top_score = min((float(em.get("top_imbalance") or 1.0) - 1.0) * 12.0, 8.0)
                    micro_score = min(abs(float(em.get("micro_ticks") or 0.0)) * 8.0, 6.0)
                except Exception:
                    em, top_score, micro_score = {}, 0.0, 0.0
                score = depth_score + spread_score + imbalance_score + volume_score + top_score + micro_score
                scored.append({
                    "symbol": sym,
                    "score": score,
                    "bias": bias,
                    "spread_ticks": spread_ticks,
                    "depth_bid": depth_b,
                    "depth_ask": depth_a,
                    "depth_min": depth_min,
                    "required_depth": required_depth,
                    "imbalance": imbalance,
                    "quote_volume": quote_volume,
                    "bid": bid,
                    "ask": ask,
                    "tick": tick,
                    "top_imbalance": em.get("top_imbalance"),
                    "micro_ticks": em.get("micro_ticks"),
                    "source": book.get("source", "rest"),
                })
                add_scan_detail(sym, "valid", score=score, bias=bias, bid=bid, ask=ask, spread_ticks=spread_ticks, depth_min=depth_min, required_depth=required_depth, imbalance=imbalance, quote_volume=quote_volume, source=book.get("source", "rest"))
            except Exception as e:
                add_scan_detail(sym, "error", "exception", error=str(e)[:240])
                if self._is_symbol_reject_error(e):
                    self._ignore_symbol(sym, f"scan reject: {str(e)[:160]}")
                    self._log_error("scan_symbol_reject_error", e, symbol=sym)
                else:
                    self._log_error("scan_symbol_error", e, symbol=sym)
                continue

        scored.sort(key=lambda r: float(r.get("score") or 0), reverse=True)
        all_valid_scored = list(scored)
        min_score = float(s.get("min_trade_score") or 0)
        if min_score > 0:
            before_count = len(scored)
            scored = [r for r in scored if float(r.get("score") or 0) >= min_score]
            if before_count > len(scored):
                reject_counts["score"] = reject_counts.get("score", 0) + (before_count - len(scored))
        self.stats.last_scan_ts = now
        self.stats.last_scan_rows = scored
        self.stats.last_scan_reject_counts = dict(reject_counts)
        if scored:
            self.stats.last_action = f"scan: best {scored[0]['symbol']} score={scored[0]['score']:.1f}"
        elif all_valid_scored:
            self.stats.last_action = f"scan: valid books below min_score={min_score:g} ({self._format_reject_counts()})"
        else:
            self.stats.last_action = f"scan: no symbol passed filters ({self._format_reject_counts()})"
        self._log_event("scan_summary", force=force, pool_count=len(pool), valid_count=len(scored), raw_valid_count=len(all_valid_scored), min_trade_score=min_score, reject_counts=reject_counts, top=scored[:10], raw_top=all_valid_scored[:10], details_logged=len(scan_details), details=scan_details)
        return scored

    def _apply_switch_guard(self, rows: list[dict[str, Any]], s: dict[str, Any]) -> list[dict[str, Any]]:
        if not rows or not self.last_selected_symbols:
            return rows
        now = time.time()
        min_hold = max(0.0, float(s.get("min_symbol_hold_sec") or 0))
        threshold = max(0.0, float(s.get("switch_score_improvement_pct") or 0)) / 100.0
        previous = self.last_selected_symbols[0]
        best = rows[0]
        if best["symbol"] == previous:
            return rows
        prev_row = next((r for r in rows if r["symbol"] == previous), None)
        if not prev_row:
            return rows
        hold_not_expired = now - self.last_symbol_switch_ts < min_hold
        improvement_not_enough = float(best["score"]) < float(prev_row["score"]) * (1.0 + threshold)
        if hold_not_expired or improvement_not_enough:
            reordered = [prev_row] + [r for r in rows if r["symbol"] != previous]
            return reordered
        return rows

    async def _select_symbols(self, s: dict[str, Any]) -> list[str]:
        rows = await self._refresh_market_scan(s, force=False)
        if not bool(s.get("basket_harvest_enabled", False)):
            rows = self._apply_switch_guard(rows, s)
        if not rows:
            self._log_debug("select_symbols_empty")
            return []

        limit = max(1, int(s.get("symbols_limit") or 1))
        active = set(self.active_tasks.keys()) | set(self.stats.open_position_symbols)
        candidates = [r for r in rows if r.get("symbol") not in active]
        if not candidates:
            self._log_debug("select_symbols_no_free_candidates", active=list(active), row_symbols=[r.get("symbol") for r in rows[:10]])
            return []

        if bool(s.get("basket_harvest_enabled", False)) and bool(s.get("basket_semi_random", True)):
            top_n = max(limit, int(s.get("basket_random_top_n") or 25))
            basket = candidates[:top_n]
            random.shuffle(basket)
            picks = [r["symbol"] for r in basket[:limit]]
        else:
            picks = [r["symbol"] for r in candidates[:limit]]

        shown = list(active) + picks
        if shown != self.last_selected_symbols:
            old_picks = self.last_selected_symbols[:]
            self.last_symbol_switch_ts = time.time()
            self.last_selected_symbols = shown[:]
            self._log_event("symbol_switch", old=old_picks, new=shown, picks=picks, active=list(active), top_rows=rows[:5])
            await self._notify("🔁 Basket symbols: " + ", ".join(shown[:10]))
        self.stats.current_symbols = shown[:]
        return picks

    async def _edge_metrics(self, symbol: str, s: dict[str, Any], book: dict[str, Any]) -> dict[str, Any]:
        """Cheap live edge metrics from the current book.

        v0027 deliberately avoids clever paper-only signals. It uses only values that
        exist at order time: top-level depth, 5-level depth, spread and microprice.
        The goal is to avoid toxic maker fills where our order is filled because the
        book is already moving against us.
        """
        client = await self._ensure_client()
        levels = max(1, min(20, int(s.get("score_top_levels") or 5)))
        tick = await client.price_tick(symbol)
        contract_size = await client.contract_size(symbol)
        bid = float(book["bids"][0][0])
        ask = float(book["asks"][0][0])
        bid_top = float(book["bids"][0][0]) * float(book["bids"][0][1]) * contract_size
        ask_top = float(book["asks"][0][0]) * float(book["asks"][0][1]) * contract_size
        depth_b = sum(float(p) * float(q) * contract_size for p, q in book["bids"][:levels])
        depth_a = sum(float(p) * float(q) * contract_size for p, q in book["asks"][:levels])
        mid = (bid + ask) / 2.0
        # Microprice closer to ask = buy pressure; closer to bid = sell pressure.
        microprice = (bid * ask_top + ask * bid_top) / max(bid_top + ask_top, 1e-12)
        micro_ticks = (microprice - mid) / max(tick, 1e-12)
        return {
            "bid": bid, "ask": ask, "tick": tick,
            "bid_top": bid_top, "ask_top": ask_top,
            "depth_bid": depth_b, "depth_ask": depth_a,
            "depth_min": min(depth_b, depth_a),
            "top_imbalance": max(bid_top / max(ask_top, 1e-9), ask_top / max(bid_top, 1e-9)),
            "depth_imbalance": max(depth_b / max(depth_a, 1e-9), depth_a / max(depth_b, 1e-9)),
            "microprice": microprice, "micro_ticks": micro_ticks,
        }

    async def _choose_direction(self, symbol: str, s: dict[str, Any], book: dict[str, Any]) -> str | None:
        mode = str(s.get("direction_mode") or "both").lower()
        if mode in {"long", "buy"}:
            forced = "long"
        elif mode in {"short", "sell"}:
            forced = "short"
        else:
            forced = None

        # v0028 Basket Harvest: semi-random basket entries. We still avoid totally
        # dead books in the scanner, but direction is deliberately simple: follow
        # the current book pressure; if the book is nearly balanced, randomize.
        if bool(s.get("basket_harvest_enabled", False)):
            if forced:
                return forced
            try:
                m0 = await self._edge_metrics(symbol, s, book)
                db = float(m0.get("depth_bid") or 0.0)
                da = float(m0.get("depth_ask") or 0.0)
                if db > da * 1.01:
                    return "long"
                if da > db * 1.01:
                    return "short"
            except Exception:
                pass
            return random.choice(["long", "short"])

        m = await self._edge_metrics(symbol, s, book)
        ratio = float(s.get("min_imbalance_ratio") or 1.04)
        top_ratio = float(s.get("entry_top_imbalance_ratio") or 1.0)
        micro_min = float(s.get("entry_microprice_min_ticks") or 0.0)

        long_ok = (
            m["depth_bid"] >= m["depth_ask"] * ratio
            and m["bid_top"] >= m["ask_top"] * top_ratio
            and m["micro_ticks"] >= micro_min
        )
        short_ok = (
            m["depth_ask"] >= m["depth_bid"] * ratio
            and m["ask_top"] >= m["bid_top"] * top_ratio
            and m["micro_ticks"] <= -micro_min
        )
        if forced == "long":
            return "long" if long_ok or not bool(s.get("edge_filter_enabled", True)) else None
        if forced == "short":
            return "short" if short_ok or not bool(s.get("edge_filter_enabled", True)) else None
        if not bool(s.get("edge_filter_enabled", True)):
            if m["depth_bid"] >= m["depth_ask"] * ratio:
                return "long"
            if m["depth_ask"] >= m["depth_bid"] * ratio:
                return "short"
            return None
        if long_ok:
            return "long"
        if short_ok:
            return "short"
        self._log_debug("edge_direction_reject", symbol=symbol, **m, min_depth_ratio=ratio, min_top_ratio=top_ratio, min_micro_ticks=micro_min)
        return None

    async def _pretrade_fee_guard(self, symbol: str, s: dict[str, Any], client: MexcFuturesClient) -> bool:
        """Return True only when this exact contract is cheap enough to scalp.

        The dedicated zero-fee universe can include symbols that still produce
        real fees on this API account. Live SOL showed this: virtual +ticks but
        balance decreased. This guard queries the exact contract fee endpoint
        right before placing a real order and blocks any non-zero maker/taker
        fee when require_contract_zero_fee_on_entry is enabled.
        """
        if not bool(s.get("require_contract_zero_fee_on_entry", True)):
            return True
        max_maker = float(s.get("max_entry_maker_fee_rate") or 0.0)
        max_taker = float(s.get("max_entry_taker_fee_rate") or 0.0)
        eps = 1e-12
        try:
            rates = await client.fetch_contract_fee_rates(symbol)
            row = rates.get(MexcFuturesClient.contract_id(symbol)) if isinstance(rates, dict) else None
            if not row:
                self.stats.last_action = f"{symbol}: fee guard skip, contract fee not verified"
                self._log_event("pretrade_fee_guard_skip", symbol=symbol, reason="fee_rate_missing", rates=rates)
                return False
            maker = float(row.get("maker") if row.get("maker") is not None else 1.0)
            taker = float(row.get("taker") if row.get("taker") is not None else 1.0)
            is_zero = row.get("is_zero")
            ok = (maker <= max_maker + eps) and (taker <= max_taker + eps) and (is_zero is not False)
            if ok:
                self._log_debug("pretrade_fee_guard_ok", symbol=symbol, maker=maker, taker=taker, is_zero=is_zero, source=row.get("source"))
                return True
            reason = f"fee guard: maker={maker:g}, taker={taker:g}, is_zero={is_zero}"
            self.stats.last_action = f"{symbol}: skipped, {reason}"
            self._log_event("pretrade_fee_guard_reject", symbol=symbol, maker=maker, taker=taker, is_zero=is_zero, source=row.get("source"), raw=row.get("raw"))
            if bool(s.get("fee_guard_ignore_symbol", True)):
                self._ignore_symbol(symbol, reason)
            return False
        except Exception as e:
            self.stats.last_action = f"{symbol}: fee guard error, skipped"
            self.stats.last_error = str(e)[:220]
            self._log_error("pretrade_fee_guard_error", e, symbol=symbol)
            return False

    async def _trade_cycle(self, symbol: str) -> None:
        self._log_event("trade_cycle_start", symbol=symbol)
        if self._is_ignored_symbol(symbol):
            self.stats.last_action = f"{symbol}: skipped, ignored"
            self._log_event("trade_cycle_skip_ignored", symbol=symbol)
            return
        client = await self._ensure_client()
        s = self._settings()
        now = time.time()
        if now < self.cooldown_until_ts:
            left = self.cooldown_until_ts - now
            self.stats.last_action = f"cooldown after loss/trade: {left:.0f}s"
            self._log_debug("trade_cycle_skip_cooldown", symbol=symbol, left_sec=left)
            return
        after_trade = max(0.0, float(s.get("cooldown_after_trade_sec") or 0))
        if after_trade > 0 and self.last_trade_closed_ts > 0 and now - self.last_trade_closed_ts < after_trade:
            left = after_trade - (now - self.last_trade_closed_ts)
            self.stats.last_action = f"cooldown after trade: {left:.0f}s"
            self._log_debug("trade_cycle_skip_after_trade_cooldown", symbol=symbol, left_sec=left)
            return
        if len(self.stats.trade_timestamps) >= int(s.get("max_trades_per_hour") or 120):
            self.stats.last_action = "hourly trade limit reached"
            self._log_event("trade_cycle_skip_hourly_limit", symbol=symbol, trade_timestamps=len(self.stats.trade_timestamps))
            return
        book = await self._depth(symbol, limit=10)
        if not book["bids"] or not book["asks"]:
            self._log_debug("trade_cycle_no_book", symbol=symbol, source=book.get("source"))
            return
        bid, ask = book["bids"][0][0], book["asks"][0][0]
        tick = await client.price_tick(symbol)
        spread_ticks = (ask - bid) / max(tick, 1e-12)
        min_spread = float(s.get("min_spread_ticks") or 1)
        max_spread = float(s.get("max_spread_ticks") or 4)
        if spread_ticks + 1e-9 < min_spread or spread_ticks > max_spread + 1e-9:
            self._log_debug("trade_cycle_spread_reject", symbol=symbol, bid=bid, ask=ask, spread_ticks=spread_ticks, min_spread=s.get("min_spread_ticks"), max_spread=s.get("max_spread_ticks"))
            return
        direction = await self._choose_direction(symbol, s, book)
        if not direction:
            self.stats.last_action = f"{symbol}: no imbalance"
            self._log_debug("trade_cycle_no_imbalance", symbol=symbol, bid=bid, ask=ask)
            return
        # v0025 zero-fee-guard mode: require a quick recheck of spread and imbalance
        # direction on several checks. This reduces trades, but avoids flickering books.
        recheck_ms = int(float(s.get("entry_recheck_ms") or 0))
        recheck_count = max(1, int(float(s.get("entry_recheck_count") or 1)))
        if bool(s.get("entry_recheck_required", False)) and recheck_ms > 0:
            for idx in range(recheck_count):
                await asyncio.sleep(max(0.0, recheck_ms / 1000.0))
                book2 = await self._depth(symbol, limit=10)
                if not book2["bids"] or not book2["asks"]:
                    self.stats.last_action = f"{symbol}: recheck no book"
                    self._log_debug("trade_cycle_recheck_no_book", symbol=symbol, check=idx + 1, source=book2.get("source"))
                    return
                bid2, ask2 = book2["bids"][0][0], book2["asks"][0][0]
                spread_ticks2 = (ask2 - bid2) / max(tick, 1e-12)
                if spread_ticks2 + 1e-9 < min_spread or spread_ticks2 > max_spread + 1e-9:
                    self.stats.last_action = f"{symbol}: recheck spread reject"
                    self._log_debug("trade_cycle_recheck_spread_reject", symbol=symbol, check=idx + 1, bid=bid2, ask=ask2, spread_ticks=spread_ticks2, min_spread=min_spread, max_spread=max_spread)
                    return
                direction2 = await self._choose_direction(symbol, s, book2)
                if direction2 != direction:
                    self.stats.last_action = f"{symbol}: recheck direction changed {direction}->{direction2}"
                    self._log_debug("trade_cycle_recheck_direction_changed", symbol=symbol, check=idx + 1, old_direction=direction, new_direction=direction2, bid=bid2, ask=ask2)
                    return
                bid, ask, book, spread_ticks = bid2, ask2, book2, spread_ticks2

        if not await self._pretrade_fee_guard(symbol, s, client):
            return

        entry_price = bid if direction == "long" else ask
        leverage = int(s.get("leverage") or 5)
        open_type = int(s.get("open_type") or 1)
        margin_usdt, margin_note = await self._position_margin_usdt(s)
        if margin_usdt <= 0:
            self.stats.last_action = f"{symbol}: no margin available ({margin_note})"
            self._log_event("trade_cycle_no_margin", symbol=symbol, margin_note=margin_note)
            return
        try:
            vol = await client.vol_from_margin(symbol, margin_usdt, leverage, entry_price)
            actual_margin = (await client.amount_from_contracts(symbol, vol)) * entry_price / max(leverage, 1)
        except Exception as e:
            if self._is_symbol_reject_error(e):
                self._ignore_symbol(symbol, f"volume/margin reject: {str(e)[:160]}")
                self._log_error("volume_margin_reject", e, symbol=symbol, margin_usdt=margin_usdt, leverage=leverage, price=entry_price)
                return
            raise
        if str(s.get("position_size_mode") or "balance_percent").lower() == "balance_percent" and actual_margin > margin_usdt * 1.05:
            reason = (
                f"min order too large for 10% rule: desired_margin={margin_usdt:.4f}, "
                f"min_actual_margin={actual_margin:.4f}"
            )
            # v0023: if margin was capped by available balance, this is not a bad symbol.
            # It only means the account is busy: old/manual positions or live orders have
            # reserved margin. Do not add BTC/SOL/ONDO/etc. to persistent ignored list.
            if "capped by available balance" in margin_note:
                self.stats.last_action = f"{symbol}: free margin too low for min order ({reason})"
                self._log_event("trade_cycle_free_margin_too_low", symbol=symbol, reason=reason, margin_note=margin_note)
                return
            self._ignore_symbol(symbol, reason)
            self._log_event("trade_cycle_min_order_too_large", symbol=symbol, reason=reason)
            return
        equity_before = await self._read_usdt_total(client) if bool(s.get("real_pnl_enabled", True)) else None
        self.stats.last_action = f"{symbol}: entry {direction} vol={vol} px={entry_price} margin={actual_margin:.4f}/{margin_usdt:.4f} ({margin_note})"
        self._log_event("entry_order_prepare", symbol=symbol, direction=direction, vol=vol, entry_price=entry_price, leverage=leverage, open_type=open_type, actual_margin=actual_margin, desired_margin=margin_usdt, margin_note=margin_note, bid=bid, ask=ask, spread_ticks=spread_ticks, book_source=book.get("source"), equity_before=equity_before)
        try:
            order = await client.open_post_only(symbol, direction, vol, entry_price, leverage, open_type)
        except Exception as e:
            if self._is_symbol_reject_error(e):
                self._ignore_symbol(symbol, f"open reject: {str(e)[:160]}")
                return
            self.stats.last_error = f"{symbol} open error: {str(e)[:180]}"
            self._log_error("entry_order_error", e, symbol=symbol, direction=direction, vol=vol, entry_price=entry_price)
            raise
        self._log_event("entry_order_submitted", symbol=symbol, order=order)
        oid = order.get("id")
        await asyncio.sleep(max(0.05, float(s.get("order_lifetime_ms") or 700) / 1000.0))
        try:
            if oid:
                cancel_res = await client.cancel_order(oid, symbol)
                self._log_debug("entry_order_cancel_after_lifetime", symbol=symbol, order_id=oid, result=cancel_res)
        except Exception as e:
            self._log_error("entry_order_cancel_error", e, symbol=symbol, order_id=oid)
            # v0023 safety: if single-order cancel fails, immediately cancel all unfinished
            # orders for this contract so an unfilled maker order cannot keep margin frozen.
            try:
                cleanup_res = await client.cancel_all_orders(symbol)
                self._log_event("entry_order_cancel_fallback_cancel_all", symbol=symbol, order_id=oid, result=cleanup_res)
            except Exception as e2:
                self._log_error("entry_order_cancel_fallback_error", e2, symbol=symbol, order_id=oid)
        pos = await client.find_position(symbol, direction)
        if not pos:
            self.stats.last_action = f"{symbol}: entry not filled"
            self._log_event("entry_not_filled", symbol=symbol, order_id=oid)
            return
        self.stats.trade_timestamps.append(time.time())
        if symbol not in self.stats.open_position_symbols:
            self.stats.open_position_symbols.append(symbol)
        self._log_event("entry_filled", symbol=symbol, direction=direction, position=pos)
        await self._notify(f"✅ FILLED {symbol} {direction.upper()} contracts={pos.get('contracts')} entry={pos.get('entryPrice') or entry_price}")
        await self._manage_position(symbol, direction, pos, s, equity_before=equity_before)

    async def _manage_basket_position(self, symbol: str, direction: str, pos: dict[str, Any], s: dict[str, Any], equity_before: float | None = None) -> None:
        """v0028 Basket Harvest manager.

        No per-position stop. A position is closed only with a maker close order
        when the configured positive basket target is reachable. After the task
        ends the run loop immediately refills the freed slot.
        """
        client = await self._ensure_client()
        leverage = int(s.get("leverage") or 5)
        open_type = int(s.get("open_type") or 1)
        tick = await client.price_tick(symbol)
        entry = float(pos.get("entryPrice") or 0) or (await client.ticker(symbol))["last"]
        contracts = int(round(float(pos.get("contracts") or 0)))
        amount = await client.amount_from_contracts(symbol, contracts)
        tick_value = abs(float(tick or 0.0) * float(amount or 0.0))
        target_usdt = max(0.0001, float(s.get("basket_target_profit_usdt") or 0.01))
        min_proxy = max(target_usdt, float(s.get("basket_min_proxy_profit_usdt") or target_usdt))
        target_ticks = max(1, int(math.ceil(target_usdt / max(tick_value, 1e-12))))
        close_order_id: str | None = None
        close_order_px: float | None = None
        close_order_ts = 0.0
        started = time.time()
        exit_price_est = entry
        reason = "basket_wait"
        self._log_event(
            "basket_manage_start",
            symbol=symbol,
            direction=direction,
            entry=entry,
            contracts=contracts,
            amount=amount,
            tick=tick,
            tick_value=tick_value,
            target_usdt=target_usdt,
            min_proxy=min_proxy,
            target_ticks=target_ticks,
            equity_before=equity_before,
            stop="OFF",
        )
        while self.running:
            current = await client.find_position(symbol, direction)
            if not current:
                reason = "basket_target_closed"
                self._log_event("basket_position_closed", symbol=symbol, direction=direction)
                break
            book = await self._depth(symbol, limit=5)
            if not book["bids"] or not book["asks"]:
                self._log_debug("basket_no_book", symbol=symbol, direction=direction)
                await asyncio.sleep(0.2)
                continue
            bid, ask = book["bids"][0][0], book["asks"][0][0]
            exit_price_est = bid if direction == "long" else ask
            proxy_pnl = (exit_price_est - entry) * amount if direction == "long" else (entry - exit_price_est) * amount
            target_price = entry + target_ticks * tick if direction == "long" else entry - target_ticks * tick

            # Use maker-only close. If the market is already beyond target, quote
            # at the best maker side to close quickly without crossing the spread.
            if direction == "long":
                close_px = max(ask, target_price) if proxy_pnl >= min_proxy else target_price
            else:
                close_px = min(bid, target_price) if proxy_pnl >= min_proxy else target_price

            now = time.time()
            requote_s = max(0.05, float(s.get("basket_close_requote_ms") or s.get("requote_interval_ms") or 200) / 1000.0)
            px_changed = close_order_px is None or abs(float(close_px) - float(close_order_px)) >= max(tick * 0.5, 1e-12)
            should_requote = (not close_order_id) or px_changed or (now - close_order_ts >= requote_s and proxy_pnl >= min_proxy)
            if should_requote:
                try:
                    if close_order_id:
                        cancel_res = await client.cancel_order(close_order_id, symbol)
                        self._log_debug("basket_close_cancel", symbol=symbol, order_id=close_order_id, result=cancel_res)
                        if self._cancel_response_has_order_closed(cancel_res):
                            current_after_cancel = await client.find_position(symbol, direction)
                            if not current_after_cancel:
                                reason = "basket_target_closed"
                                break
                except Exception as e:
                    self._log_error("basket_close_cancel_error", e, symbol=symbol, order_id=close_order_id)
                current_before_close = await client.find_position(symbol, direction)
                if not current_before_close:
                    reason = "basket_target_closed"
                    break
                try:
                    order = await client.close_limit(symbol, direction, contracts, close_px, leverage, open_type, post_only=True)
                except Exception as e:
                    if "2009" in str(e) or "nonexistent or closed" in str(e).lower():
                        reason = "basket_target_closed"
                        self._log_event("basket_close_position_already_closed", symbol=symbol, direction=direction, close_px=close_px, error=str(e)[:220])
                        break
                    self._log_error("basket_close_submit_error", e, symbol=symbol, direction=direction, close_px=close_px, proxy_pnl=proxy_pnl)
                    await asyncio.sleep(0.25)
                    continue
                close_order_id = order.get("id")
                close_order_px = close_px
                close_order_ts = time.time()
                self.stats.last_action = f"{symbol}: basket wait +${target_usdt:.3f}, proxy={proxy_pnl:.5f}, close_px={close_px}"
                self._log_event("basket_close_submitted", symbol=symbol, direction=direction, contracts=contracts, close_px=close_px, target_price=target_price, target_ticks=target_ticks, target_usdt=target_usdt, proxy_pnl=proxy_pnl, order=order)
            await asyncio.sleep(max(0.05, float(s.get("requote_interval_ms") or 200) / 1000.0))

        await asyncio.sleep(0.25)
        still = await client.find_position(symbol, direction)
        if still:
            # No stops means no forced market close from the position manager.
            # Manual Close All remains available from Telegram.
            self._log_event("basket_position_left_open", symbol=symbol, direction=direction, still=still, reason=reason)
            if symbol in self.stats.open_position_symbols:
                self.stats.open_position_symbols.remove(symbol)
            return

        equity_after = await self._read_usdt_total(client) if bool(s.get("real_pnl_enabled", True)) else None
        real_pnl = None
        if equity_before is not None and equity_after is not None:
            real_pnl = float(equity_after) - float(equity_before)
        virtual_pnl = (exit_price_est - entry) * amount if direction == "long" else (entry - exit_price_est) * amount
        pnl = real_pnl if real_pnl is not None else virtual_pnl
        pnl_source = "real_balance" if real_pnl is not None else "virtual_price"
        self.stats.estimated_pnl += pnl
        self.stats.trades += 1
        win_min = max(0.0, float(s.get("real_win_min_usdt") or 0.0))
        is_win = pnl > win_min
        self._increment_total_trade_counters(pnl, is_win=is_win)
        self.last_trade_closed_ts = time.time()
        if is_win:
            self.stats.wins += 1
            self.stats.consecutive_losses = 0
        else:
            self.stats.losses += 1
            self.stats.consecutive_losses += 1
        self.stats.last_action = f"{symbol}: basket closed, pnl={pnl:.6f} ({pnl_source})"
        self._log_event("basket_trade_closed", symbol=symbol, direction=direction, reason=reason, entry=entry, exit_price_est=exit_price_est, contracts=contracts, amount=amount, pnl=pnl, pnl_source=pnl_source, virtual_pnl=virtual_pnl, equity_before=equity_before, equity_after=equity_after, session_trades=self.stats.trades, session_wins=self.stats.wins, session_losses=self.stats.losses, target_usdt=target_usdt, target_ticks=target_ticks, elapsed=time.time()-started, is_win=is_win)
        if symbol in self.stats.open_position_symbols:
            self.stats.open_position_symbols.remove(symbol)
        await self._notify(f"🏁 BASKET CLOSED {symbol} {direction.upper()} pnl={pnl:.6f} USDT ({pnl_source})")

    async def _manage_position(self, symbol: str, direction: str, pos: dict[str, Any], s: dict[str, Any], equity_before: float | None = None) -> None:
        if bool(s.get("basket_harvest_enabled", False)):
            await self._manage_basket_position(symbol, direction, pos, s, equity_before=equity_before)
            return
        client = await self._ensure_client()
        leverage = int(s.get("leverage") or 5)
        open_type = int(s.get("open_type") or 1)
        tick = await client.price_tick(symbol)
        entry = float(pos.get("entryPrice") or 0) or (await client.ticker(symbol))["last"]
        contracts = int(round(float(pos.get("contracts") or 0)))
        base_target_ticks = int(s.get("target_ticks") or 1)
        stop_ticks = int(s.get("stop_ticks") or 3)
        target_ticks, fee_target_info = await self._fee_aware_target_ticks(symbol, contracts, tick, base_target_ticks, pos, s, client)
        max_life = float(s.get("max_position_lifetime_sec") or 15)
        close_order_id: str | None = None
        close_order_ts = 0.0
        started = time.time()
        exit_price_est = entry
        reason = "unknown"
        self._log_event("manage_position_start", symbol=symbol, direction=direction, entry=entry, contracts=contracts, base_target_ticks=base_target_ticks, target_ticks=target_ticks, stop_ticks=stop_ticks, max_life=max_life, equity_before=equity_before, fee_target=fee_target_info)
        while self.running:
            current = await client.find_position(symbol, direction)
            if not current:
                reason = "target/closed"
                self._log_event("position_disappeared_or_closed", symbol=symbol, direction=direction)
                break
            book = await self._depth(symbol, limit=5)
            if not book["bids"] or not book["asks"]:
                self._log_debug("manage_position_no_book", symbol=symbol, direction=direction)
                await asyncio.sleep(0.2)
                continue
            bid, ask = book["bids"][0][0], book["asks"][0][0]
            exit_price_est = bid if direction == "long" else ask
            stop_hit = (direction == "long" and bid <= entry - stop_ticks * tick) or (direction == "short" and ask >= entry + stop_ticks * tick)
            elapsed = time.time() - started
            time_hit = elapsed >= max_life
            hard_life = max(max_life + 5.0, float(s.get("max_position_hard_lifetime_sec") or (max_life * 3)))
            hard_time_hit = elapsed >= hard_life
            if stop_hit or hard_time_hit:
                reason = "virtual_stop" if stop_hit else "hard_time_stop"
                self._log_event("position_exit_trigger", symbol=symbol, direction=direction, reason=reason, entry=entry, bid=bid, ask=ask, exit_est=exit_price_est, elapsed=elapsed)
                try:
                    if close_order_id:
                        cancel_res = await client.cancel_order(close_order_id, symbol)
                        self._log_debug("close_order_cancel_before_emergency", symbol=symbol, order_id=close_order_id, result=cancel_res)
                except Exception as e:
                    self._log_error("close_order_cancel_before_emergency_error", e, symbol=symbol, order_id=close_order_id)
                allow_time_market = bool(s.get("emergency_market_close_on_time_stop", False))
                if bool(s.get("emergency_market_close")) and (stop_hit or hard_time_hit or allow_time_market):
                    market_res = await client.close_market(current, leverage, open_type)
                    self._log_event("emergency_market_close_sent", symbol=symbol, result=market_res)
                break
            target = entry + target_ticks * tick if direction == "long" else entry - target_ticks * tick
            maker_time_exit = bool(time_hit and not s.get("emergency_market_close_on_time_stop", False))
            if maker_time_exit and reason != "time_maker_exit":
                reason = "time_maker_exit"
                self._log_event("position_time_maker_exit_mode", symbol=symbol, direction=direction, entry=entry, bid=bid, ask=ask, elapsed=elapsed)
            if not close_order_id or time.time() - close_order_ts >= max(0.1, float(s.get("order_lifetime_ms") or 700) / 1000.0):
                try:
                    if close_order_id:
                        cancel_res = await client.cancel_order(close_order_id, symbol)
                        self._log_debug("close_order_requote_cancel", symbol=symbol, order_id=close_order_id, result=cancel_res)
                        if self._cancel_response_has_order_closed(cancel_res):
                            current_after_cancel = await client.find_position(symbol, direction)
                            if not current_after_cancel:
                                reason = "target/closed"
                                self._log_event("close_order_already_filled_on_cancel", symbol=symbol, order_id=close_order_id, cancel_result=cancel_res)
                                break
                except Exception as e:
                    self._log_error("close_order_requote_cancel_error", e, symbol=symbol, order_id=close_order_id)
                # Re-check right before a new close order. The previous maker close can fill
                # between cancel and re-quote; MEXC then returns code 2009, which is not a
                # real API failure for us.
                current_before_close = await client.find_position(symbol, direction)
                if not current_before_close:
                    reason = "target/closed"
                    self._log_event("position_closed_before_requote", symbol=symbol, direction=direction)
                    break
                if maker_time_exit:
                    # After soft lifetime, stop insisting on TP and work a maker exit at the best opposite quote.
                    close_px = ask if direction == "long" else bid
                else:
                    close_px = max(ask, target) if direction == "long" else min(bid, target)
                try:
                    order = await client.close_limit(symbol, direction, contracts, close_px, leverage, open_type, post_only=bool(s.get("post_only_close")))
                except Exception as e:
                    if "2009" in str(e) or "nonexistent or closed" in str(e).lower():
                        reason = "target/closed"
                        self._log_event("close_order_position_already_closed", symbol=symbol, direction=direction, close_px=close_px, error=str(e)[:220])
                        break
                    raise
                self._log_event("close_order_submitted", symbol=symbol, direction=direction, contracts=contracts, close_px=close_px, target=target, order=order)
                close_order_id = order.get("id")
                close_order_ts = time.time()
                self.stats.last_action = f"{symbol}: close {direction} px={close_px} oid={close_order_id}"
            await asyncio.sleep(max(0.05, float(s.get("requote_interval_ms") or 300) / 1000.0))
        await asyncio.sleep(0.25)
        still = await client.find_position(symbol, direction)
        if still:
            final_market_allowed = bool(s.get("emergency_market_close")) and (reason in {"virtual_stop", "hard_time_stop"} or bool(s.get("emergency_market_close_on_time_stop", False)) or not self.running)
            if final_market_allowed:
                try:
                    market_res = await client.close_market(still, leverage, open_type)
                    self._log_event("final_market_close_sent", symbol=symbol, still=still, result=market_res, reason=reason)
                except Exception as e:
                    self._log_error("final_market_close_error", e, symbol=symbol, still=still)
            else:
                self._log_event("final_market_close_skipped", symbol=symbol, still=still, reason=reason)
        amount = await client.amount_from_contracts(symbol, contracts)
        virtual_pnl = (exit_price_est - entry) * amount if direction == "long" else (entry - exit_price_est) * amount
        equity_after = await self._read_usdt_total(client) if bool(s.get("real_pnl_enabled", True)) else None
        real_pnl = None
        if equity_before is not None and equity_after is not None:
            real_pnl = float(equity_after) - float(equity_before)
        pnl = real_pnl if real_pnl is not None else virtual_pnl
        pnl_source = "real_balance" if real_pnl is not None else "virtual_price"
        self.stats.estimated_pnl += pnl
        self.stats.trades += 1
        win_min = max(0.0, float(s.get("real_win_min_usdt") or 0.0))
        is_win = pnl > win_min
        self._increment_total_trade_counters(pnl, is_win=is_win)
        self.last_trade_closed_ts = time.time()
        if is_win:
            self.stats.wins += 1
            self.stats.consecutive_losses = 0
        else:
            self.stats.losses += 1
            self.stats.consecutive_losses += 1
            pause = max(0.0, float(s.get("cooldown_after_loss_sec") or 0))
            if pause > 0:
                self.cooldown_until_ts = max(self.cooldown_until_ts, time.time() + pause)
                self._log_event("loss_cooldown_started", symbol=symbol, pause_sec=pause, cooldown_until=self.cooldown_until_ts)
            if bool(s.get("ignore_symbol_after_real_loss", True)) and pnl_source == "real_balance":
                self._ignore_symbol(symbol, f"real pnl negative: {pnl:.6f} USDT; virtual={virtual_pnl:.6f}")
        self.stats.last_action = f"{symbol}: closed {reason}, pnl={pnl:.6f} ({pnl_source})"
        self._log_event("trade_closed", symbol=symbol, direction=direction, reason=reason, entry=entry, exit_price_est=exit_price_est, contracts=contracts, amount=amount, pnl=pnl, pnl_source=pnl_source, virtual_pnl=virtual_pnl, equity_before=equity_before, equity_after=equity_after, session_trades=self.stats.trades, session_wins=self.stats.wins, session_losses=self.stats.losses, win_min=win_min, is_win=is_win)
        if symbol in self.stats.open_position_symbols:
            self.stats.open_position_symbols.remove(symbol)
        await self._notify(f"🏁 CLOSED {symbol} {direction.upper()} reason={reason} pnl={pnl:.6f} USDT ({pnl_source})")
