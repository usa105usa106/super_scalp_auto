from __future__ import annotations

import asyncio
import json
import math
import random
import time
from pathlib import Path
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
    live_equity: float = 0.0
    live_unrealized: float = 0.0
    live_used_margin: float = 0.0
    net_equity_pnl: float = 0.0
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
    # Universe diagnostics shown in Telegram.
    # total_count is the raw list before strategy filters; universe_count is the
    # usable/tradeable scan pool after quote/STOCK/ignore filters.
    zero_fee_total_count: int = 0
    zero_fee_blocked_count: int = 0
    zero_fee_ignored_count: int = 0
    zero_fee_universe_count: int = 0
    zero_fee_scan_count: int = 0
    ignored_symbols_count: int = 0
    wave_state: dict[str, Any] = field(default_factory=dict)


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
        # v0064: freeze guards / cheap scan caches.
        self._last_loop_tick_ts = 0.0
        self._run_loop_tick_count = 0
        self._last_risk_balance_ts = 0.0
        self._last_existing_positions_ts = 0.0
        self._cached_existing_positions: list[dict[str, Any]] = []
        self.cooldown_until_ts = 0.0
        self.last_trade_closed_ts = 0.0
        self.mid_history: dict[str, list[tuple[float, float, float]]] = {}
        self.wave_candidate_side: str | None = None
        self.wave_candidate_count: int = 0
        # v0064: market signal hold/stability guard. A one-tick acceleration spike
        # must not fire a basket. The signal is sampled over a time window,
        # so one noisy failed check does not fully reset a valid wave.
        self.wave_signal_hold_key: str | None = None
        self.wave_signal_hold_count: int = 0
        self.wave_signal_hold_since: float = 0.0
        self.wave_signal_hold_samples: list[tuple[float, str]] = []
        self.wave_signal_hold_last_sample_ts: float = 0.0
        self.wave_cooldown_until_ts: float = 0.0
        self.wave_dominance_history: list[tuple[float, float, float]] = []  # ts, long_dom, short_dom
        self.wave_signal_mode_last: str = "all_zero_total"
        self.last_wave_vote_rows: list[dict[str, Any]] = []
        self.last_wave_vote_summary: dict[str, Any] = {}
        # v0064: optional TOP10 leader signal mode. Leaders decide only market
        # direction; entries are still picked from the full zero-fee universe.
        self.last_wave_leader_symbols: list[str] = []
        self.last_wave_leader_vote_rows: list[dict[str, Any]] = []
        self.last_wave_leader_vote_summary: dict[str, Any] = {}
        # v0064 Mirror Lab: virtual-only replay buffer. No real orders are sent.
        self.mirror_lab_snapshots: list[dict[str, Any]] = []
        self.mirror_lab_last_report: dict[str, Any] = {}
        self.mirror_lab_log_path = Path("logs/mirror_lab_events.jsonl")
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

    def reset_signal_state(self) -> None:
        """Reset market signal state without touching positions/orders.

        Used after ALL/TOP10 mode changes and presets so old hold samples or
        dominance history cannot trigger a stale basket in the new mode.
        """
        self.wave_candidate_side = None
        self.wave_candidate_count = 0
        self.wave_signal_hold_key = None
        self.wave_signal_hold_count = 0
        self.wave_signal_hold_since = 0.0
        self.wave_signal_hold_samples = []
        self.wave_signal_hold_last_sample_ts = 0.0
        self.wave_dominance_history = []
        self.stats.wave_state = {}

    def _friendly_error(self, err: Any) -> str:
        txt = str(err or "").strip()
        low = txt.lower()
        if not txt:
            return ""
        # MEXC often returns HTTP 200 with an exchange error code inside JSON.
        # Users do not need to see the noisy HTTP wrapper in the live panel.
        if "code': 510" in txt or '"code": 510' in txt or "requests are too frequent" in low:
            return "MEXC rate limit 510: opening basket too fast; retry/throttle active"
        if "code': 2005" in txt or '"code": 2005' in txt or "balance insufficient" in low:
            return "MEXC 2005: not enough free margin for one basket slot"
        if "code': 2009" in txt or '"code": 2009' in txt or "nonexistent" in low or "closed" in low:
            return "MEXC 2009: position already closed/nonexistent"
        if "code': 2019" in txt or '"code": 2019' in txt or "leverage adjustment" in low:
            return "MEXC 2019: leverage cannot be changed while orders/positions are open"
        txt = txt.replace("MEXC private HTTP 200:", "MEXC:").replace("MEXC public HTTP 200:", "MEXC:")
        return txt[:180]

    async def _notify(self, text: str) -> None:
        self.stats.last_action = str(text or "")[:240]
        self._log_event("notify", text=self.stats.last_action)
        try:
            await self.notify(text)
        except Exception:
            pass

    def _remember_wave_open_skip(self, symbol: str, reason: str, **data: Any) -> dict[str, Any]:
        """Keep the real reason why a wave slot was not opened for the panel/log."""
        item: dict[str, Any] = {"symbol": symbol, "reason": reason, "ts": time.time()}
        item.update({k: v for k, v in data.items() if v is not None})
        skips = list(self.stats.wave_state.get("open_skips") or [])
        skips.append(item)
        self.stats.wave_state["last_open_skip"] = item
        self.stats.wave_state["open_skips"] = skips[-12:]
        return item

    def _short_wave_skip_reason(self, reason: str) -> str:
        mapping = {
            "ignored": "ignored",
            "no_book": "no book",
            "spread": "spread",
            "side_flip": "flip",
            "fee_guard": "fee",
            "no_margin": "margin",
            "min_order_too_large": "min size",
            "order_error": "order error",
            "not_filled": "not filled",
            "invalid_fee_abort": "fee abort",
            "symbol_reject": "reject",
            "volume_margin_reject": "vol reject",
        }
        return mapping.get(str(reason or ""), str(reason or "skip")[:18])

    def _format_wave_skips(self, skips: list[dict[str, Any]] | None, limit: int = 5) -> str:
        parts: list[str] = []
        seen: set[str] = set()
        for item in list(skips or [])[-12:]:
            sym = str(item.get("symbol") or "?")
            reason = self._short_wave_skip_reason(str(item.get("reason") or "skip"))
            key = f"{sym}:{reason}"
            if key in seen:
                continue
            seen.add(key)
            if str(item.get("reason")) == "spread" and item.get("spread_ticks") is not None:
                try:
                    reason = f"spread {float(item.get('spread_ticks')):.1f}t"
                except Exception:
                    pass
            elif str(item.get("reason")) == "side_flip" and item.get("now"):
                reason = f"flip->{str(item.get('now')).upper()}"
            parts.append(f"{sym}:{reason}")
            if len(parts) >= limit:
                break
        return ", ".join(parts)

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
        sym = str(symbol or "").upper().strip()
        # Per strategy rule: symbols containing STOCK are blocked. Metals, oil,
        # indexes and tokenized tickers without this substring remain allowed.
        if "STOCK" in sym:
            return True
        # v0064: the account balance/margin shown by MEXC is USDT. Contracts like
        # SOL_USDC/BTC_USDC require USDC collateral, so MEXC returns
        # "Balance insufficient" with available=0 even when USDT is free.
        # Basket mode must therefore trade only *_USDT contracts by default.
        return not sym.endswith("_USDT")

    def _stable_symbol_set(self, s: dict[str, Any] | None = None) -> set[str]:
        """Stable/near-stable symbols excluded from TOP10 leader signal.

        They can still be part of the raw MEXC zero-fee list, but they should not
        vote as leaders because stable pairs do not represent market direction.
        """
        settings = s or self._settings()
        base_stables = {
            "USDT", "USDC", "USDE", "USD1", "DAI", "FDUSD", "TUSD",
            "PYUSD", "USDD", "USDP", "USDS", "BUSD", "EURC", "EURT",
        }
        raw = str(settings.get("wave_top10_excluded_symbols") or "")
        for item in raw.split(","):
            sym = MexcFuturesClient.contract_id(item.strip())
            if not sym:
                continue
            base_stables.add(sym)
            base_stables.add(sym.split("_", 1)[0])
        return {x.upper() for x in base_stables if x}

    def _is_top10_excluded_symbol(self, symbol: str, s: dict[str, Any] | None = None) -> bool:
        sym = MexcFuturesClient.contract_id(symbol)
        if not sym:
            return True
        base = sym.split("_", 1)[0].upper()
        excluded = self._stable_symbol_set(s)
        return sym.upper() in excluded or base in excluded

    def _top_liquid_leader_symbols(self, pool: list[str], s: dict[str, Any]) -> list[str]:
        """Return TOP-N liquid leader symbols from the already volume-sorted pool.

        verified_zero_fee_symbols() returns the zero-fee universe sorted by 24h
        liquidity when ticker data is available, so the first N non-stable USDT
        symbols become the market-direction leaders.
        """
        n = max(1, int(s.get("wave_top10_leader_count") or 10))
        out: list[str] = []
        seen: set[str] = set()
        for raw in pool:
            sym = MexcFuturesClient.contract_id(raw)
            if not sym or sym in seen or self._blocked_symbol(sym) or self._is_top10_excluded_symbol(sym, s):
                continue
            out.append(sym)
            seen.add(sym)
            if len(out) >= n:
                break
        return out

    def _build_leader_vote_rows(self, leader_symbols: list[str], vote_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_symbol = {MexcFuturesClient.contract_id(r.get("symbol")): r for r in vote_rows if r.get("symbol")}
        rows: list[dict[str, Any]] = []
        for sym in leader_symbols:
            row = by_symbol.get(MexcFuturesClient.contract_id(sym))
            if row:
                rows.append(dict(row) | {"leader": True})
            else:
                rows.append({"symbol": sym, "vote": "neutral", "move_pct": None, "move_pct_age": 0.0, "source": "no_fresh_price", "leader": True})
        return rows

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

    @staticmethod
    def _raw_float_first(raw: dict[str, Any], keys: tuple[str, ...]) -> float | None:
        for key in keys:
            try:
                if key in raw and raw.get(key) not in (None, ""):
                    return float(raw.get(key) or 0.0)
            except Exception:
                pass
        return None

    async def _position_unrealized_usdt(self, client: MexcFuturesClient, pos: dict[str, Any]) -> float | None:
        """Best-effort per-slot floating PnL for the panel.

        The basket close decision still uses real equity delta. Slot values are
        informational: prefer MEXC-reported unrealized profit, otherwise compute
        a proxy from entry/mark and contract size.
        """
        raw = pos.get("raw") if isinstance(pos, dict) else {}
        if not isinstance(raw, dict):
            raw = {}
        val = self._raw_float_first(raw, (
            "unrealized", "unRealized", "unrealizedProfit", "unrealised",
            "unrealisedProfit", "profit", "holdProfit", "positionProfit",
            "floatingPL", "floatingPnl", "unrealizedPnl", "unrealisedPnl",
        ))
        if val is not None:
            return float(val)
        try:
            entry = float(pos.get("entryPrice") or 0.0)
            mark = float(pos.get("markPrice") or 0.0)
            if entry <= 0 or mark <= 0:
                return None
            amount = await client.amount_from_contracts(str(pos.get("symbol") or ""), float(pos.get("contracts") or 0.0))
            if str(pos.get("side") or "long").lower() == "short":
                return (entry - mark) * amount
            return (mark - entry) * amount
        except Exception:
            return None

    async def _build_wave_slots(self, client: MexcFuturesClient, opened: list[dict[str, Any]], positions: list[dict[str, Any]], side: str, target_slots: int) -> list[dict[str, Any]]:
        order = [MexcFuturesClient.contract_id(p.get("symbol")) for p in opened if p.get("symbol")]
        by_symbol: dict[str, dict[str, Any]] = {}
        for p in positions:
            sym = MexcFuturesClient.contract_id(p.get("symbol"))
            if sym:
                by_symbol[sym] = p
        slots: list[dict[str, Any]] = []
        for idx in range(max(1, int(target_slots or 5))):
            sym = order[idx] if idx < len(order) else ""
            pos = by_symbol.get(sym) if sym else None
            if pos:
                pnl = await self._position_unrealized_usdt(client, pos)
                slots.append({"slot": idx + 1, "symbol": sym, "side": str(pos.get("side") or side).lower(), "pnl": pnl, "status": "open"})
            else:
                slots.append({"slot": idx + 1, "symbol": sym, "side": str(side or "").lower(), "pnl": None, "status": "empty"})
        return slots

    @staticmethod
    def _position_has_nonzero_fee(pos: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        """Return True if the already-open live position proves this contract is not fee-free.

        MEXC zero-fee lists/fee endpoints may disagree with the actual position.
        The position object is the final truth: if fee/totalFee or feeRates are non-zero,
        a 1-cent basket harvest must not keep this contract for hours.
        """
        raw = pos.get("raw") if isinstance(pos, dict) else {}
        if not isinstance(raw, dict):
            raw = {}
        info: dict[str, Any] = {"fee": 0.0, "totalFee": 0.0, "holdFee": 0.0, "minRate": 0.0, "maxRate": 0.0}
        for key in ("fee", "totalFee", "holdFee"):
            try:
                info[key] = abs(float(raw.get(key) or 0.0))
            except Exception:
                info[key] = 0.0
        rates = raw.get("feeRates") if isinstance(raw.get("feeRates"), dict) else {}
        try:
            info["minRate"] = abs(float(rates.get("min") or 0.0))
        except Exception:
            pass
        try:
            info["maxRate"] = abs(float(rates.get("max") or 0.0))
        except Exception:
            pass
        bad = any(float(info.get(k) or 0.0) > 1e-12 for k in ("fee", "totalFee", "holdFee", "minRate", "maxRate"))
        return bad, info

    async def _refresh_equity_snapshot(self, client: MexcFuturesClient | None = None) -> None:
        """Update live account equity snapshot used by the fast Telegram panel."""
        c = client or self.client
        if not c:
            return
        try:
            bal = await c.fetch_balance()
            usdt = bal.get("USDT") or {}
            equity = float(usdt.get("total") or 0.0)
            self.stats.live_equity = equity
            self.stats.live_unrealized = float(usdt.get("unrealized") or 0.0)
            self.stats.live_used_margin = float(usdt.get("used") or 0.0)
            self.stats.net_equity_pnl = (equity - float(self.stats.start_equity or 0.0)) if self.stats.start_equity else 0.0
        except Exception as e:
            self._log_error("equity_snapshot_error", e)

    async def _abort_invalid_fee_position(self, client: MexcFuturesClient, symbol: str, direction: str, pos: dict[str, Any], s: dict[str, Any], equity_before: float | None, reason_info: dict[str, Any]) -> bool:
        """Immediately exit positions that prove they are not zero-fee after fill.

        This is not a strategy stop; it is an invalid-contract abort. Holding such
        positions makes the panel look green on closed trades while account equity bleeds.
        """
        if not bool(s.get("abort_nonzero_fee_position", True)):
            return False
        leverage = int(s.get("leverage") or 5)
        open_type = int(s.get("open_type") or 1)
        self.stats.last_action = f"{symbol}: invalid fee after fill, aborting"
        self._log_event("invalid_fee_position_abort_start", symbol=symbol, direction=direction, fee_info=reason_info, position=pos)
        try:
            if bool(s.get("emergency_market_close_invalid_fee", True)):
                order = await client.close_market(pos, leverage=leverage, open_type=open_type)
            else:
                book = await self._depth(symbol, limit=5)
                if book.get("bids") and book.get("asks"):
                    px = book["bids"][0][0] if direction == "long" else book["asks"][0][0]
                else:
                    px = float(pos.get("entryPrice") or 0.0)
                order = await client.close_limit(symbol, direction, int(round(float(pos.get("contracts") or 0))), px, leverage, open_type, post_only=False)
            self._log_event("invalid_fee_position_abort_order", symbol=symbol, direction=direction, order=order)
            for _ in range(10):
                await asyncio.sleep(0.35)
                if not await client.find_position(symbol, direction):
                    break
        except Exception as e:
            self._log_error("invalid_fee_position_abort_error", e, symbol=symbol, direction=direction, fee_info=reason_info)
            return False
        equity_after = await self._read_usdt_total(client) if bool(s.get("real_pnl_enabled", True)) else None
        pnl = 0.0
        if equity_before is not None and equity_after is not None:
            pnl = float(equity_after) - float(equity_before)
        self.stats.estimated_pnl += pnl
        self.stats.trades += 1
        self.stats.losses += 1
        self.stats.consecutive_losses += 1
        self._increment_total_trade_counters(pnl, is_win=False)
        self.last_trade_closed_ts = time.time()
        self._ignore_symbol(symbol, f"actual position fee not zero: {reason_info}")
        await self._refresh_equity_snapshot(client)
        self._log_event("invalid_fee_position_aborted", symbol=symbol, direction=direction, pnl=pnl, equity_before=equity_before, equity_after=equity_after, fee_info=reason_info)
        await self._notify(f"⛔ {symbol} aborted: actual fee not zero, pnl={pnl:.6f} USDT")
        return True

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
        """Start/refresh WS subscriptions for fast depth scanning.

        v0064 rule: 0 means ALL. The price vote must be based on the real
        active zero-fee universe count, not on an arbitrary 250-symbol cap.
        """
        if str(s.get("market_data_mode") or "websocket").lower() != "websocket" or not bool(s.get("ws_depth_enabled")):
            return
        ws_limit = int(s.get("ws_depth_max_symbols") or 0)
        scan_limit = int(s.get("max_zero_fee_scan_symbols") or 0)
        limit = ws_limit if ws_limit > 0 else (scan_limit if scan_limit > 0 else 0)
        symbols = [MexcFuturesClient.contract_id(x) for x in symbols if x]
        if limit > 0:
            symbols = symbols[:limit]
        if not symbols:
            return
        if self.depth_ws is None:
            self.depth_ws = MexcDepthWebSocket(settings=s)
            self._log_event("ws_depth_created", endpoint=self.depth_ws.endpoint)
        else:
            self.depth_ws.update_settings(s)
        await self.depth_ws.set_symbols(symbols)
        self._log_debug("ws_depth_symbols_set", count=len(symbols), limit=(limit or "ALL"), symbols=symbols[:20])
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
        self.reset_signal_state()
        self.stats.started_ts = time.time()
        self.last_selected_symbols = []
        self.last_symbol_switch_ts = 0.0
        self.cooldown_until_ts = 0.0
        self.last_trade_closed_ts = 0.0
        try:
            bal = await asyncio.wait_for(self.client.fetch_balance(), timeout=max(4.0, float(self._settings().get("mexc_private_timeout") or 15.0))) if self.client else {}
            self.stats.start_equity = float((bal.get("USDT") or {}).get("total") or 0)
        except Exception as e:
            self.stats.last_error = f"balance: {e}"
            self._log_error("start_balance_error", e)
        # v0064: do not repeat slow private balance/positions checks immediately
        # on the very first scan tick; start() just did the balance read.
        now = time.time()
        self._last_risk_balance_ts = now
        self._last_existing_positions_ts = now
        self.task = asyncio.create_task(self._run_loop(), name="micro_maker_loop")
        self._log_event("start_success", start_equity=self.stats.start_equity)
        return "▶️ Price Tsunami v0064 запущен. Схема: price-scan 10s по активным zero-fee монетам → LONG/SHORT/NEUTRAL проценты → 5 сделок одной стороной → закрытие всей корзины по REAL NET PnL."

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
        # STOP is a hard PAUSE only. It must not touch exchange orders or positions.
        # Full exchange cleanup is reserved for Close All.
        if close_positions:
            client = self.client
            if client:
                try:
                    s = self._settings()
                    await client.hard_close_all(leverage=int(s.get("leverage") or 5), open_type=int(s.get("open_type") or 1))
                except Exception as e:
                    self.stats.last_error = str(e)[:240]
                    self._log_error("stop_cleanup_error", e, close_positions=close_positions)
            await self._stop_market_ws()
            self._log_event("stop_done", close_positions=True)
            return "🚨 Risk Stop: позиции закрыты market + ордера отменены."
        await self._stop_market_ws()
        self._log_event("stop_done", close_positions=False)
        return "⏸ Stop: скан и новые сделки поставлены на жёсткую паузу. Ордера и позиции на бирже НЕ тронуты."

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

    @staticmethod
    def _fmt_pct(value: Any) -> str:
        try:
            return f"{float(value) * 100.0:.0f}%"
        except Exception:
            return "0%"

    @staticmethod
    def _fmt_pp(value: Any) -> str:
        try:
            return f"{float(value) * 100.0:+.0f}п.п."
        except Exception:
            return "+0п.п."

    def _fmt_accel_flow(self, old_value: Any, new_value: Any, acc_value: Any, label: str) -> str:
        """Human Telegram text: LONG было 50% → стало 65% (+15п.п.)."""
        try:
            old_pct = float(old_value) * 100.0
            new_pct = float(new_value) * 100.0
            acc_pct = float(acc_value) * 100.0
            return f"{label}: было {old_pct:.0f}% → стало {new_pct:.0f}% ({acc_pct:+.0f}п.п.)"
        except Exception:
            return f"{label}: было 0% → стало 0% (+0п.п.)"

    def _wave_accel_lines(self, data: dict[str, Any]) -> tuple[str, str]:
        old_long = data.get("old_long_pct", data.get("long_pct", 0.0))
        old_short = data.get("old_short_pct", data.get("short_pct", 0.0))
        return (
            self._fmt_accel_flow(old_long, data.get("long_pct", 0.0), data.get("long_acceleration", data.get("long_acc", 0.0)), "LONG"),
            self._fmt_accel_flow(old_short, data.get("short_pct", 0.0), data.get("short_acceleration", data.get("short_acc", 0.0)), "SHORT"),
        )


    def _top10_accel_lines(self, v: dict[str, Any]) -> tuple[str, str]:
        total = max(1, int(v.get("active") or v.get("top10_leader_count") or 10))
        old_long = int(v.get("old_long_count") or 0)
        old_short = int(v.get("old_short_count") or 0)
        long_n = int(v.get("long") or 0)
        short_n = int(v.get("short") or 0)
        return (
            f"LONG: было {old_long}/{total} → стало {long_n}/{total} ({long_n - old_long:+d} мон.)",
            f"SHORT: было {old_short}/{total} → стало {short_n}/{total} ({short_n - old_short:+d} мон.)",
        )

    def _mode_title(self, mode: Any) -> str:
        m = str(mode or "wait").lower()
        return {
            "early": "EARLY WAVE",
            "normal": "NORMAL WAVE",
            "tsunami": "TSUNAMI",
            "wait": "ЗАСАДА",
        }.get(m, m.upper())

    def _wave_view(self, s: dict[str, Any]) -> dict[str, Any]:
        """One clean view model for Telegram panel/status text."""
        w = dict(self.stats.wave_state or {})
        if str(s.get("wave_market_signal_mode") or "all_zero_total") == "top10_leaders":
            summary = dict(getattr(self, "last_wave_leader_vote_summary", {}) or {})
        else:
            summary = dict(getattr(self, "last_wave_vote_summary", {}) or {})
        for key in ("active", "price_ready", "no_fresh_price", "long", "short", "neutral", "long_pct", "short_pct", "neutral_pct"):
            if w.get(key) is None and summary.get(key) is not None:
                w[key] = summary.get(key)
        mode = str(w.get("mode") or "wait").lower()
        side = str(w.get("side") or "-").upper()
        target = float(w.get("target") or (s.get("wave_tsunami_target_profit_usdt") if mode == "tsunami" else s.get("wave_normal_target_profit_usdt") or s.get("wave_target_profit_usdt") or 0.05))
        lev = int(w.get("leverage") or (s.get("wave_tsunami_leverage") if mode == "tsunami" else s.get("wave_normal_leverage") or s.get("leverage") or 5))
        opened = int(w.get("open_count") or len(self.stats.open_position_symbols))
        target_slots = int(w.get("open_target") or s.get("wave_positions") or 5)
        active = int(w.get("active") or 0)
        price_ready = int(w.get("price_ready") or 0)
        no_fresh_price = int(w.get("no_fresh_price") or 0)
        long_pct = float(w.get("long_pct") or 0.0)
        short_pct = float(w.get("short_pct") or 0.0)
        neutral_pct = float(w.get("neutral_pct") or 0.0)
        long_acc = float(w.get("long_acceleration") or 0.0)
        short_acc = float(w.get("short_acceleration") or 0.0)
        old_long_pct = float(w.get("old_long_pct", long_pct))
        old_short_pct = float(w.get("old_short_pct", short_pct))
        acc = float(w.get("acceleration") or (long_acc if side == "LONG" else short_acc if side == "SHORT" else 0.0))
        conclusion = "сидим в засаде"
        if mode == "early":
            conclusion = f"ранняя волна {side}"
        elif mode == "normal":
            conclusion = f"рынок {side}"
        elif mode == "tsunami":
            conclusion = f"TSUNAMI {side}"
        selected = [str(x) for x in (w.get("selected") or self.stats.current_symbols or []) if x]
        opened_symbols = [str(x) for x in (self.stats.open_position_symbols or []) if x]
        skip_txt = self._format_wave_skips(list(w.get("open_skips") or []), limit=4)
        return {
            "signal_mode": str(w.get("signal_mode") or s.get("wave_market_signal_mode") or "all_zero_total"),
            "leader_symbols": list(w.get("leader_symbols") or getattr(self, "last_wave_leader_symbols", []) or []),
            "top10_normal_count": int(w.get("top10_normal_count") or s.get("wave_top10_normal_count") or 7),
            "top10_tsunami_count": int(w.get("top10_tsunami_count") or s.get("wave_top10_tsunami_count") or 8),
            "top10_accel_count": int(w.get("top10_accel_count") or s.get("wave_top10_accel_count") or 2),
            "top10_tsunami_requires_accel": bool(w.get("top10_tsunami_requires_accel") if w.get("top10_tsunami_requires_accel") is not None else s.get("wave_top10_tsunami_requires_accel", False)),
            "old_long_count": int(w.get("old_long_count") or round(old_long_pct * max(1, active))),
            "old_short_count": int(w.get("old_short_count") or round(old_short_pct * max(1, active))),
            "long_count_accel": int(w.get("long_count_accel") or 0),
            "short_count_accel": int(w.get("short_count_accel") or 0),
            "mode": mode,
            "title": self._mode_title(mode),
            "side": side,
            "target": target,
            "leverage": lev,
            "opened": opened,
            "target_slots": target_slots,
            "active": active,
            "price_ready": price_ready,
            "no_fresh_price": no_fresh_price,
            "long": int(w.get("long") or 0),
            "short": int(w.get("short") or 0),
            "neutral": int(w.get("neutral") or 0),
            "long_pct": long_pct,
            "short_pct": short_pct,
            "neutral_pct": neutral_pct,
            "long_acc": long_acc,
            "short_acc": short_acc,
            "old_long_pct": old_long_pct,
            "old_short_pct": old_short_pct,
            "acceleration": acc,
            "conclusion": conclusion,
            "selected": selected,
            "opened_symbols": opened_symbols,
            "skip_txt": skip_txt,
            "net": float(w.get("net") or self.stats.net_equity_pnl or 0.0),
            "peak": float(w.get("peak") or 0.0),
            "slots": list(w.get("slots") or []),
            "pending_mode": str(w.get("pending_mode") or w.get("detected_mode") or "").lower(),
            "pending_side": str(w.get("pending_side") or w.get("side") or "-").upper(),
            "hold_count": int(w.get("signal_hold_count") or 0),
            "hold_need": int(w.get("signal_hold_need") or 0),
            "hold_checks": int(w.get("signal_hold_checks") or w.get("signal_hold_need") or 0),
            "hold_for": float(w.get("signal_hold_for") or 0.0),
            "hold_sec": float(w.get("signal_hold_sec") or 0.0),
            "hold_reason": str(w.get("reason") or ""),
        }

    def _format_wave_status(self, s: dict[str, Any]) -> str:
        if not bool(s.get("wave_basket_enabled")):
            return ""
        v = self._wave_view(s)
        if v.get("signal_mode") == "top10_leaders":
            accel_long, accel_short = self._top10_accel_lines(v)
        else:
            accel_long, accel_short = self._wave_accel_lines({
                "old_long_pct": v["old_long_pct"],
                "old_short_pct": v["old_short_pct"],
                "long_pct": v["long_pct"],
                "short_pct": v["short_pct"],
                "long_acceleration": v["long_acc"],
                "short_acceleration": v["short_acc"],
            })
        return (
            f"🌊 {v['title']} | {v['conclusion']} | {v['target_slots']} {v['side']} | "
            f"lev={v['leverage']}x | REAL NET TP +${v['target']:.2f}\n"
            f"PRICE 10s: LONG {self._fmt_pct(v['long_pct'])} ({v['long']}) / "
            f"SHORT {self._fmt_pct(v['short_pct'])} ({v['short']}) / "
            f"NEUTRAL {self._fmt_pct(v['neutral_pct'])} ({v['neutral']}) | counted {v['active']} | price-ready {v['price_ready']} | no data→neutral {v['no_fresh_price']}\n"
            f"За 60с {accel_long} | {accel_short}\n"
        )

    def quick_status_text(self) -> str:
        """Clean live panel for Price Tsunami mode. No REST calls here.

        v0064 UI rule: market scan blocks are ALWAYS visible, even when the
        basket is already open. Debug/runtime details are kept out of the main
        panel so the Telegram message stays readable.
        """
        s = self._settings()
        uptime = 0.0
        if self.stats.started_ts:
            uptime = max(0.0, time.time() - self.stats.started_ts)
        h = int(uptime // 3600)
        m = int((uptime % 3600) // 60)
        sec = int(uptime % 60)
        state = "RUNNING" if self.is_running() else "STOPPED"
        last_update = self._local_time_text(s)
        cooldown_left = max(0.0, self.wave_cooldown_until_ts - time.time(), self.cooldown_until_ts - time.time())
        cooldown_txt = f" • cooldown {cooldown_left:.0f}s" if cooldown_left > 0 else ""
        v = self._wave_view(s)

        raw_total = int(getattr(self.stats, "zero_fee_total_count", 0) or 0)
        blocked_total = int(getattr(self.stats, "zero_fee_blocked_count", 0) or 0)
        ignored_total = int(getattr(self.stats, "zero_fee_ignored_count", 0) or 0)
        usable_total = int(getattr(self.stats, "zero_fee_universe_count", 0) or len(self.zero_fee_cache) or v["active"])
        if raw_total <= 0:
            raw_total = usable_total

        if v.get("signal_mode") == "top10_leaders":
            accel_long, accel_short = self._top10_accel_lines(v)
        else:
            accel_long, accel_short = self._wave_accel_lines({
                "old_long_pct": v["old_long_pct"],
                "old_short_pct": v["old_short_pct"],
                "long_pct": v["long_pct"],
                "short_pct": v["short_pct"],
                "long_acceleration": v["long_acc"],
                "short_acceleration": v["short_acc"],
            })

        # Decision text: one short human-readable block.
        if v["mode"] == "wait":
            decision_title = "ЗАСАДА"
            if v.get("signal_mode") == "top10_leaders":
                decision_reason = f"TOP10: нет {v['top10_normal_count']}/{max(1, v['active'])} direction; EARLY нужен +{v['top10_accel_count']} мон. за 60с; TSUNAMI нужен {v['top10_tsunami_count']}/{max(1, v['active'])}"
            else:
                decision_reason = "нет 65% + ускорения и нет 75% dominance"
        elif v["mode"] == "early":
            decision_title = f"EARLY {v['side']}"
            if v.get("signal_mode") == "top10_leaders":
                decision_reason = f"TOP10 >= {v['top10_normal_count']}/{max(1, v['active'])} + рост +{v['top10_accel_count']} мон. за 60с; открыть {v['target_slots']} {v['side']}, 5x, TP +${v['target']:.2f}"
            else:
                decision_reason = f"открыть {v['target_slots']} {v['side']}, 5x, TP +${v['target']:.2f}"
        elif v["mode"] == "normal":
            decision_title = f"NORMAL {v['side']}"
            if v.get("signal_mode") == "top10_leaders":
                decision_reason = f"TOP10 >= {v['top10_normal_count']}/{max(1, v['active'])}; открыть {v['target_slots']} {v['side']}, 5x, TP +${v['target']:.2f}"
            else:
                decision_reason = f"открыть {v['target_slots']} {v['side']}, 5x, TP +${v['target']:.2f}"
        else:
            decision_title = f"TSUNAMI {v['side']}"
            if v.get("signal_mode") == "top10_leaders":
                decision_reason = f"TOP10 >= {v['top10_tsunami_count']}/{max(1, v['active'])}; открыть {v['target_slots']} {v['side']}, 10x, TP +${v['target']:.2f}"
            else:
                decision_reason = f"открыть {v['target_slots']} {v['side']}, 10x, TP +${v['target']:.2f}"

        hold_line = ""
        if v.get("pending_mode") and v.get("pending_mode") != "wait" and v.get("hold_need", 0) > 1:
            hold_line = (
                f"HOLD: {self._mode_title(v['pending_mode'])} {v['pending_side']} "
                f"{v['hold_count']}/{v['hold_checks']} checks, нужно {v['hold_need']}/{v['hold_checks']} "
                f"за {v['hold_sec']:.0f}с"
            )

        # Basket block: do not print five empty slot lines when there are no positions.
        display_side = "—" if v["mode"] == "wait" else v["side"]
        basket_lines = [
            "КОРЗИНА",
            f"Открыто: {v['opened']}/{v['target_slots']}",
            f"Side: {display_side}",
            f"Leverage: {v['leverage']}x",
            f"REAL NET: {v['net']:+.5f} / +{v['target']:.2f}",
            f"Peak: {v['peak']:+.5f}",
        ]

        raw_slots = list(v.get("slots") or [])
        slot_rows: list[str] = []
        for i in range(int(v.get("target_slots") or 5)):
            item = raw_slots[i] if i < len(raw_slots) and isinstance(raw_slots[i], dict) else {"slot": i + 1, "status": "empty"}
            sym = str(item.get("symbol") or "").upper()
            side_txt = str(item.get("side") or v.get("side") or "").lower()
            status = str(item.get("status") or "")
            if status == "open" and sym:
                pnl = item.get("pnl")
                pnl_txt = "n/a" if pnl is None else f"{float(pnl):+.3f}"
                slot_rows.append(f"{i+1}. {sym} {side_txt} {pnl_txt}")
            elif sym:
                slot_rows.append(f"{i+1}. {sym} — нет позиции")
            elif v["opened"] > 0:
                slot_rows.append(f"{i+1}. — waiting")
        slots_block = ""
        if slot_rows:
            slots_block = "\n\nСЛОТЫ\n" + "\n".join(slot_rows)

        selected = ", ".join(v["selected"][:8])
        selected_block = ""
        if selected:
            selected_block = f"\n\nВЫБРАНО\nMiddle 25–60%: {selected}"
        if v["skip_txt"]:
            selected_block += f"\nSkip: {v['skip_txt']}"

        error_block = ""
        friendly_error = self._friendly_error(self.stats.last_error) or ""
        if friendly_error:
            error_block = f"\n\nОШИБКА\n{friendly_error}"
        # v0064: make a frozen scanner visible on the panel instead of silently showing an old message.
        try:
            if self.is_running():
                hb_age = time.time() - float(getattr(self, "_last_loop_tick_ts", 0.0) or 0.0)
                scan_age = time.time() - float(getattr(self.stats, "last_scan_ts", 0.0) or 0.0)
                if hb_age > 30 or scan_age > 45:
                    error_block += f"\n\nDIAG\nloop age {hb_age:.0f}s | scan age {scan_age:.0f}s | ticks {getattr(self, '_run_loop_tick_count', 0)}"
        except Exception:
            pass

        lines = [
            f"🌊 Price Tsunami {s.get('bot_version', 'v0064')}",
            f"{state} • {last_update} • up {h:02d}:{m:02d}:{sec:02d}{cooldown_txt}",
            "",
            "РЫНОК",
            f"Signal: {'TOP10 leaders' if v.get('signal_mode') == 'top10_leaders' else 'ALL zero total'}",
            f"Universe: {usable_total} / {raw_total} zero-fee USDT",
            f"Blocked: {blocked_total} | Ignored: {ignored_total}",
            f"Ready: {v['price_ready']} | No fresh price: {v['no_fresh_price']}",
            "",
            "СКАН 10с",
            f"LONG {self._fmt_pct(v['long_pct'])} ({v['long']})",
            f"SHORT {self._fmt_pct(v['short_pct'])} ({v['short']})",
            f"NEUTRAL {self._fmt_pct(v['neutral_pct'])} ({v['neutral']})",
            "",
            "ИЗМЕНЕНИЕ ЗА 60с",
            accel_long,
            accel_short,
            "",
            "РЕШЕНИЕ",
            f"Mode: {decision_title}",
            f"Причина: {decision_reason}",
        ]
        if hold_line:
            lines.append(hold_line)
        lines.extend(["", *basket_lines])
        return "\n".join(lines) + slots_block + selected_block + error_block

    async def status_text(self) -> str:
        s = self._settings()
        bal_txt = "Balance: n/a"
        pos_txt = "Positions: n/a"
        client = self.client
        if client:
            try:
                bal = await client.fetch_balance()
                usdt = bal.get("USDT") or {}
                bal_txt = (
                    f"USDT total {float(usdt.get('total') or 0):.4f} | "
                    f"free {float(usdt.get('free') or 0):.4f} | "
                    f"used {float(usdt.get('used') or 0):.4f}"
                )
            except Exception as e:
                bal_txt = f"Balance error: {str(e)[:120]}"
            try:
                pos = await client.fetch_positions()
                if pos:
                    pos_txt = "\n".join([f"{p.get('symbol')} {p.get('side')} contracts={p.get('contracts')} entry={p.get('entryPrice')}" for p in pos[:8]])
                else:
                    pos_txt = "нет открытых позиций"
            except Exception as e:
                pos_txt = f"Positions error: {str(e)[:120]}"
        return self.quick_status_text() + "\n\n💰 API\n" + bal_txt + "\n" + pos_txt

    async def scan_now_text(self) -> str:
        self._log_event("scan_now_requested")
        try:
            s = self._settings()
            await self._ensure_client()
            rows = await self._refresh_market_scan(s, force=True)
            # Build the same decision text the live loop uses, but do not open trades here.
            # v0064: scan_now must be read-only for signal/hold state; pressing the
            # Price Scan button must not help satisfy HOLD checks or reset live state.
            saved_signal_state = (
                list(self.wave_dominance_history),
                list(self.wave_signal_hold_samples),
                self.wave_signal_hold_last_sample_ts,
                self.wave_signal_hold_key,
                self.wave_signal_hold_count,
                self.wave_signal_hold_since,
                self.wave_candidate_side,
                self.wave_candidate_count,
                dict(self.stats.wave_state or {}),
                self.wave_signal_mode_last,
            )
            try:
                _side, picks, details = self._detect_wave_signal(rows, s)
                v = self._wave_view(s)
            finally:
                (
                    self.wave_dominance_history,
                    self.wave_signal_hold_samples,
                    self.wave_signal_hold_last_sample_ts,
                    self.wave_signal_hold_key,
                    self.wave_signal_hold_count,
                    self.wave_signal_hold_since,
                    self.wave_candidate_side,
                    self.wave_candidate_count,
                    self.stats.wave_state,
                    self.wave_signal_mode_last,
                ) = saved_signal_state
            raw_total = int(getattr(self.stats, "zero_fee_total_count", 0) or 0)
            blocked_total = int(getattr(self.stats, "zero_fee_blocked_count", 0) or 0)
            ignored_detail = int(getattr(self.stats, "zero_fee_ignored_count", 0) or 0)
            universe = int(self.stats.zero_fee_universe_count or len(self.zero_fee_cache) or v["active"])
            if raw_total <= 0:
                raw_total = universe
            candidates = int(details.get("trade_candidates") or 0) if isinstance(details, dict) else 0
            selected = ", ".join([str(r.get("symbol")) for r in picks[: int(s.get("wave_positions") or 5)]]) or "-"
            if v.get("signal_mode") == "top10_leaders":
                accel_long, accel_short = self._top10_accel_lines(v)
            else:
                accel_long, accel_short = self._wave_accel_lines({
                    "old_long_pct": v["old_long_pct"],
                    "old_short_pct": v["old_short_pct"],
                    "long_pct": v["long_pct"],
                    "short_pct": v["short_pct"],
                    "long_acceleration": v["long_acc"],
                    "short_acceleration": v["short_acc"],
                })
            if v["mode"] == "wait":
                decision = "ЗАСАДА — не открывать"
                if v.get("signal_mode") == "top10_leaders":
                    reason = f"TOP10: нет {v['top10_normal_count']}/{max(1, v['active'])} direction; EARLY нужен +{v['top10_accel_count']} мон. за 60с; TSUNAMI нужен {v['top10_tsunami_count']}/{max(1, v['active'])}"
                else:
                    reason = "нет 65% + ускорения и нет 75% dominance"
            elif v["mode"] == "early":
                decision = f"EARLY {v['side']} — 5 {v['side']}, 5x, REAL NET +$0.05"
                if v.get("signal_mode") == "top10_leaders":
                    reason = f"TOP10 >= {v['top10_normal_count']}/{max(1, v['active'])} + рост +{v['top10_accel_count']} мон. за 60с, после HOLD"
                else:
                    reason = "есть 65% + рост +15п.п., после HOLD"
            elif v["mode"] == "normal":
                decision = f"NORMAL {v['side']} — 5 {v['side']}, 5x, REAL NET +$0.05"
                reason = f"TOP10 >= {v['top10_normal_count']}/{max(1, v['active'])}, после HOLD" if v.get("signal_mode") == "top10_leaders" else "есть 75% dominance, после HOLD"
            else:
                decision = f"TSUNAMI {v['side']} — 5 {v['side']}, 10x, REAL NET +$0.10"
                if v.get("signal_mode") == "top10_leaders":
                    reason = f"TOP10 >= {v['top10_tsunami_count']}/{max(1, v['active'])}, после HOLD"
                else:
                    reason = "есть 75% dominance + рост +15п.п., после HOLD"
            state = "RUNNING" if self.is_running() else "STOPPED"
            uptime = 0.0
            if self.stats.started_ts:
                uptime = max(0.0, time.time() - self.stats.started_ts)
            hh = int(uptime // 3600)
            mm = int((uptime % 3600) // 60)
            ss = int(uptime % 60)
            hold_need = int(s.get('wave_signal_hold_required') or 4)
            hold_checks = int(s.get('wave_signal_hold_checks') or 5)
            hold_sec = float(s.get('wave_signal_hold_sec') or 10.0)
            return (
                f"🔍 Price Scan {s.get('bot_version', 'v0064')}\n"
                f"{state} • {self._local_time_text(s)} • up {hh:02d}:{mm:02d}:{ss:02d}\n\n"
                "РЫНОК\n"
                f"Signal: {'TOP10 leaders' if v.get('signal_mode') == 'top10_leaders' else 'ALL zero total'}\n"
                f"Universe: {universe} / {raw_total} zero-fee USDT\n"
                f"Blocked: {blocked_total} | Ignored: {ignored_detail}\n"
                f"Ready: {v['price_ready']} | No fresh price: {v['no_fresh_price']}\n\n"
                "СКАН 10с\n"
                f"LONG {self._fmt_pct(v['long_pct'])} ({v['long']})\n"
                f"SHORT {self._fmt_pct(v['short_pct'])} ({v['short']})\n"
                f"NEUTRAL {self._fmt_pct(v['neutral_pct'])} ({v['neutral']})\n\n"
                "ИЗМЕНЕНИЕ ЗА 60с\n"
                f"{accel_long}\n"
                f"{accel_short}\n\n"
                "РЕШЕНИЕ\n"
                f"Mode: {decision}\n"
                f"Причина: {reason}\n"
                f"Hold: нужно {hold_need}/{hold_checks} checks за ~{hold_sec:.0f}с\n\n"
                "КАНДИДАТЫ ДЛЯ ВХОДА\n"
                f"По цене + исполнение: {candidates} / нужно {int(s.get('wave_positions') or 5)}\n"
                "Фильтры: price-side, fresh book, spread, depth, fee; без старого tick/imbalance-edge отбора\n"
                f"Middle 25–60% picks: {selected}"
            )
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
                f"spr={r['spread_ticks']:.1f}t depth={r['depth_min']:.0f}$ imb={r['imbalance']:.2f} "
                f"move={float(r.get('move_ticks') or 0.0):+.1f}t src={r.get('source','-')}"
            )
        return "\n".join(lines)

    def _format_reject_counts(self) -> str:
        counts = self.stats.last_scan_reject_counts or {}
        if not counts:
            return "-"
        return ", ".join(f"{k}={v}" for k, v in sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[:5])

    async def _run_loop(self) -> None:
        self._log_event("run_loop_started")
        await self._notify("✅ Price Tsunami v0064 started: freeze-guard loop ON; ALL mode 65/75% +15п.п.; TOP10 7/10 normal, 7/10 +2 early, 8/10 tsunami; HOLD 4/5 за ~10s.")

        async def one_tick(s: dict[str, Any]) -> None:
            self._last_loop_tick_ts = time.time()
            self._run_loop_tick_count += 1
            await self._risk_guard(s)
            await self._cleanup_tasks()
            if bool(s.get("wave_basket_enabled", False)):
                await self._wave_loop_tick(s)
            else:
                # Background scan runs even when all slots are busy.
                await self._refresh_market_scan(s, force=False)
                active_symbols = set(self.active_tasks.keys()) | set(self.stats.open_position_symbols)
                target_slots = min(int(s.get("max_positions") or 1), int(s.get("symbols_limit") or 1))
                capacity = max(0, target_slots - len(active_symbols))
                if capacity > 0:
                    symbols = await self._select_symbols(s, capacity=capacity)
                    for sym in symbols:
                        if capacity <= 0:
                            break
                        if sym in self.active_tasks:
                            continue
                        task = asyncio.create_task(self._trade_cycle(sym), name=f"trade_{sym}")
                        self.active_tasks[sym] = task
                        capacity -= 1
            await asyncio.sleep(max(0.05, float(s.get("cycle_sleep_ms") or 250) / 1000.0))

        while self.running:
            try:
                s = self._settings()
                tick_timeout = max(5.0, float(s.get("run_loop_tick_timeout_sec") or 22.0))
                await asyncio.wait_for(one_tick(s), timeout=tick_timeout)
            except asyncio.CancelledError:
                self._log_event("run_loop_cancelled")
                break
            except asyncio.TimeoutError as e:
                self.stats.api_errors += 1
                self.stats.last_error = f"run loop tick timeout > {float(self._settings().get('run_loop_tick_timeout_sec') or 22.0):.0f}s; scan reset"
                self._log_error("run_loop_tick_timeout", e, api_errors=self.stats.api_errors, last_scan_ts=self.stats.last_scan_ts)
                # Reset WS cache on a stuck tick; next iteration will rebuild it.
                try:
                    await self._stop_market_ws()
                except Exception:
                    pass
                await asyncio.sleep(1.0)
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
        now = time.time()
        # v0064: do not call private balance API on every 100ms strategy tick.
        # A stuck/private request was enough to make the whole live scan look frozen.
        bal_interval = max(3.0, float(s.get("risk_guard_balance_interval_sec") or 12.0))
        if self.stats.start_equity > 0 and self.client and now - self._last_risk_balance_ts >= bal_interval:
            self._last_risk_balance_ts = now
            try:
                bal = await asyncio.wait_for(self.client.fetch_balance(), timeout=max(4.0, float(s.get("mexc_private_timeout") or 15.0)))
                usdt = bal.get("USDT") or {}
                equity = float(usdt.get("total") or 0)
                self.stats.live_equity = equity
                self.stats.live_unrealized = float(usdt.get("unrealized") or 0.0)
                self.stats.live_used_margin = float(usdt.get("used") or 0.0)
                self.stats.net_equity_pnl = (equity - float(self.stats.start_equity or 0.0)) if self.stats.start_equity else 0.0
                if equity > 0 and self.stats.start_equity - equity >= float(s.get("daily_loss_limit_usdt") or 2):
                    self._log_event("risk_stop_daily_loss", start_equity=self.stats.start_equity, equity=equity, limit=s.get("daily_loss_limit_usdt"))
                    await self._notify(f"🛑 Daily loss limit hit: start={self.stats.start_equity:.4f}, now={equity:.4f}")
                    await self.stop(close_positions=True)
            except Exception as e:
                self._log_error("risk_guard_balance_error", e)
        now = time.time()
        self.stats.trade_timestamps = [x for x in self.stats.trade_timestamps if now - x < 3600]

    async def _symbol_pool(self, s: dict[str, Any]) -> list[str]:
        client = await self._ensure_client()
        allowed = parse_symbols(str(s.get("allowed_symbols") or ""))
        allowed_set = set(allowed)
        ignored = self._ignored_symbols(s)

        def apply_scan_cap(pool: list[str]) -> list[str]:
            # v0064: max_zero_fee_scan_symbols <= 0 means ALL symbols, no 250 cap.
            limit = int(s.get("max_zero_fee_scan_symbols") or 0)
            return pool[:limit] if limit > 0 else pool

        # Manual whitelist: trade exactly the user's symbols, not a hidden 250 window.
        if allowed_set and not (s.get("auto_select_symbols") and s.get("only_zero_fee")):
            blocked = [x for x in allowed if self._blocked_symbol(x)]
            ignored_list = [x for x in allowed if self._is_ignored_symbol(x, s)]
            pool = [x for x in allowed if not self._is_ignored_symbol(x, s) and not self._blocked_symbol(x)]
            out = apply_scan_cap(pool)
            self.stats.zero_fee_total_count = len(allowed)
            self.stats.zero_fee_blocked_count = len(blocked)
            self.stats.zero_fee_ignored_count = len(ignored_list)
            self.stats.zero_fee_universe_count = len(pool)
            self.stats.zero_fee_scan_count = len(out)
            self.stats.ignored_symbols_count = len(ignored)
            self._log_event("symbol_pool_manual", allowed=len(allowed), blocked=len(blocked), ignored=len(ignored_list), usable=len(pool), out_count=len(out), scan_cap=(int(s.get("max_zero_fee_scan_symbols") or 0) or "ALL"), symbols=out[:30])
            return out

        # If zero-fee filter is OFF, scan all active public *_USDT contracts.
        # Default remains only_zero_fee=True, so the normal mode below scans all 0% fee contracts.
        if s.get("auto_select_symbols") and not s.get("only_zero_fee"):
            active_limit = int(s.get("zero_fee_universe_max_symbols") or 0)
            fresh = await client.active_usdt_symbols(active_limit)
            blocked = [x for x in fresh if self._blocked_symbol(x)]
            ignored_list = [x for x in fresh if x in ignored]
            pool = [x for x in fresh if x and x not in ignored and not self._blocked_symbol(x) and (not allowed_set or x in allowed_set)]
            out = apply_scan_cap(pool)
            self.stats.zero_fee_total_count = len(fresh)
            self.stats.zero_fee_blocked_count = len(blocked)
            self.stats.zero_fee_ignored_count = len(ignored_list)
            self.stats.zero_fee_universe_count = len(pool)
            self.stats.zero_fee_scan_count = len(out)
            self.stats.ignored_symbols_count = len(ignored)
            self.stats.last_action = f"active universe rebuilt: total={len(fresh)}, blocked={len(blocked)}, ignored={len(ignored_list)}, usable={len(pool)}, scan={len(out)}"
            self._log_event("active_symbol_pool", total_count=len(fresh), blocked=len(blocked), ignored=len(ignored_list), usable=len(pool), returned=len(out), scan_cap=(int(s.get("max_zero_fee_scan_symbols") or 0) or "ALL"), first_symbols=out[:30])
            return out

        # Normal Price Tsunami mode: API-confirmed zero-fee universe, no fixed 250 cap.
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
                blocked = [x for x in fresh if self._blocked_symbol(x)]
                ignored_list = [x for x in fresh if x in ignored]
                self.zero_fee_cache = [x for x in fresh if x and x not in ignored and not self._blocked_symbol(x)]
                self.zero_fee_ts = now
                self.stats.zero_fee_total_count = len(fresh)
                self.stats.zero_fee_blocked_count = len(blocked)
                self.stats.zero_fee_ignored_count = len(ignored_list)
                self.stats.zero_fee_universe_count = len(self.zero_fee_cache)
                self.stats.zero_fee_scan_count = len(self.zero_fee_cache)
                self.stats.ignored_symbols_count = len(ignored)
                self.stats.last_action = (
                    f"zero-fee universe rebuilt: total={len(fresh)}, "
                    f"blocked={len(blocked)}, ignored={len(ignored_list)}, usable={len(self.zero_fee_cache)}"
                )
                self._log_event("zero_fee_rescan_done", total_count=len(fresh), blocked=len(blocked), ignored=len(ignored_list), usable=len(self.zero_fee_cache), universe_cap=(universe_limit or "ALL"), first_symbols=self.zero_fee_cache[:30])
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

        pool = [x for x in self.zero_fee_cache if x not in ignored and not self._blocked_symbol(x) and (not allowed_set or x in allowed_set)]
        out = apply_scan_cap(pool)
        # Keep the raw/blocked diagnostics from the last rescan, but refresh the
        # usable and returned scan counts every loop because allowed/ignore/cap can change.
        if not self.stats.zero_fee_total_count:
            self.stats.zero_fee_total_count = len(self.zero_fee_cache)
        self.stats.zero_fee_universe_count = len(pool)
        self.stats.zero_fee_scan_count = len(out)
        self.stats.ignored_symbols_count = len(ignored)
        self._log_debug("symbol_pool_active", zero_fee_total=self.stats.zero_fee_total_count, blocked=self.stats.zero_fee_blocked_count, zero_fee_cache=len(self.zero_fee_cache), pool_count=len(pool), returned=len(out), ignored=len(ignored), allowed_filter=bool(allowed_set), scan_cap=(int(s.get("max_zero_fee_scan_symbols") or 0) or "ALL"), first_symbols=out[:30])
        return out

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

        # v0064: scanner depth requirement must be cheap/read-only. Do not fetch
        # private balance for every market scan; use cached/start equity instead.
        try:
            if str(s.get("position_size_mode") or "balance_percent").lower() == "fixed_usdt":
                margin_usdt = max(0.0, float(s.get("margin_per_position_usdt") or 0.0))
            else:
                equity_ref = float(self.stats.live_equity or self.stats.start_equity or 0.0)
                margin_usdt = max(0.0, equity_ref * max(0.0, float(s.get("position_margin_percent") or 10.0)) / 100.0)
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
        wave_vote_rows: list[dict[str, Any]] = []
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
                self._record_mid_price(sym, bid, ask, tick)
                vote_move_pct, vote_age = self._recent_move_pct(sym, float(s.get("wave_price_lookback_sec") or s.get("wave_lookback_sec") or 10.0))
                vote = self._classify_wave_vote(vote_move_pct, s)
                wave_vote_rows.append({
                    "symbol": sym,
                    "vote": vote,
                    "move_pct": vote_move_pct,
                    "move_pct_age": vote_age,
                    "bid": bid,
                    "ask": ask,
                    "source": book.get("source", "rest"),
                })
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
                # v0064: Price Tsunami no longer uses the old order-book
                # imbalance/edge direction filter to decide whether a coin is a
                # candidate. Direction is price-vote only: price rose over 10s =>
                # LONG, price fell => SHORT. Order book checks remain only as
                # execution safety: fresh book, acceptable spread and enough depth.
                # Keep the old bias for logs/debug, but do not reject because of it.
                try:
                    old_bias = await self._choose_direction(sym, s, book)
                except Exception:
                    old_bias = None
                bias = vote if vote in {"long", "short"} else None
                if not bias:
                    add_scan_detail(sym, "reject", "neutral_vote", bid=bid, ask=ask, spread_ticks=spread_ticks, depth_bid=depth_b, depth_ask=depth_a, depth_min=depth_min, imbalance=imbalance, source=book.get("source"), vote=vote)
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
                move_ticks, move_age = self._recent_move_ticks(sym, float(s.get("wave_lookback_sec") or s.get("basket_rebound_lookback_sec") or 20.0), tick)
                move_pct, move_pct_age = vote_move_pct, vote_age
                # v0064: score is no longer the market-direction source. Keep it only
                # as secondary display/ranking; the wave detector uses move_pct votes.
                wave_score = min(abs(float(move_pct or 0.0)) * 120.0, 30.0) if bool(s.get("wave_basket_enabled", False)) else 0.0
                score = depth_score + spread_score + imbalance_score + volume_score + top_score + micro_score + wave_score
                scored.append({
                    "symbol": sym,
                    "score": score,
                    "vote": vote,
                    # v0064: in Price Tsunami the trade side comes from the 10s
                    # price vote, not from the old order-book edge/imbalance bias.
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
                    "move_ticks": move_ticks,
                    "move_age": move_age,
                    "move_pct": move_pct,
                    "move_pct_age": move_pct_age,
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
        # v0064: denominator must be the whole selected universe. If a symbol has
        # no fresh WS book / no 10s price history / scan error, count it as NEUTRAL
        # instead of silently shrinking 377 coins into e.g. 352 votes.
        voted_symbols = {MexcFuturesClient.contract_id(r.get("symbol")) for r in wave_vote_rows if r.get("symbol")}
        for _sym in pool:
            _sid = MexcFuturesClient.contract_id(_sym)
            if _sid not in voted_symbols:
                wave_vote_rows.append({
                    "symbol": _sid,
                    "vote": "neutral",
                    "move_pct": None,
                    "move_pct_age": 0.0,
                    "source": "no_fresh_price",
                })
                voted_symbols.add(_sid)
        vote_summary = self._summarize_wave_votes(wave_vote_rows)
        self.last_wave_vote_rows = wave_vote_rows
        self.last_wave_vote_summary = vote_summary
        # v0064: precompute TOP10 leaders from the same full zero-fee pool.
        # This does not reduce the trade universe; it only gives an optional
        # market-direction signal source.
        leader_symbols = self._top_liquid_leader_symbols(pool, s)
        leader_vote_rows = self._build_leader_vote_rows(leader_symbols, wave_vote_rows)
        leader_vote_summary = self._summarize_wave_votes(leader_vote_rows)
        self.last_wave_leader_symbols = leader_symbols
        self.last_wave_leader_vote_rows = leader_vote_rows
        self.last_wave_leader_vote_summary = leader_vote_summary
        self.stats.wave_state.update({
            "active": vote_summary.get("active", 0),
            "price_ready": vote_summary.get("price_ready", 0),
            "no_fresh_price": vote_summary.get("no_fresh_price", 0),
            "long": vote_summary.get("long", 0),
            "short": vote_summary.get("short", 0),
            "neutral": vote_summary.get("neutral", 0),
            "long_pct": vote_summary.get("long_pct", 0.0),
            "short_pct": vote_summary.get("short_pct", 0.0),
            "neutral_pct": vote_summary.get("neutral_pct", 0.0),
        })
        self.stats.last_scan_ts = now
        self.stats.last_scan_rows = scored
        self.stats.last_scan_reject_counts = dict(reject_counts)
        if scored:
            self.stats.last_action = f"scan: best {scored[0]['symbol']} score={scored[0]['score']:.1f}"
        elif all_valid_scored:
            self.stats.last_action = f"scan: valid books below min_score={min_score:g} ({self._format_reject_counts()})"
        else:
            self.stats.last_action = f"scan: no symbol passed filters ({self._format_reject_counts()})"
        # v0064 Mirror Lab records only compact virtual snapshots. It does not
        # affect signal/hold state and never sends orders.
        self._record_mirror_snapshot(all_valid_scored, s)
        self._log_event("scan_summary", force=force, pool_count=len(pool), active_vote_count=vote_summary.get("active", 0), vote_summary=vote_summary, leader_symbols=leader_symbols, leader_vote_summary=leader_vote_summary, valid_count=len(scored), raw_valid_count=len(all_valid_scored), min_trade_score=min_score, reject_counts=reject_counts, top=scored[:10], raw_top=all_valid_scored[:10], details_logged=len(scan_details), details=scan_details)
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

    async def _select_symbols(self, s: dict[str, Any], capacity: int | None = None) -> list[str]:
        rows = await self._refresh_market_scan(s, force=False)
        if not bool(s.get("basket_harvest_enabled", False)):
            rows = self._apply_switch_guard(rows, s)
        if not rows:
            self._log_debug("select_symbols_empty")
            return []

        configured_limit = max(1, int(s.get("symbols_limit") or 1))
        if capacity is None:
            limit = configured_limit
        else:
            limit = max(0, min(configured_limit, int(capacity)))
        active = set(self.active_tasks.keys()) | set(self.stats.open_position_symbols)
        if limit <= 0:
            self.stats.current_symbols = list(active)
            return []
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

        shown = (list(active) + picks)[: max(1, int(s.get("symbols_limit") or 1))]
        if shown != self.last_selected_symbols:
            old_picks = self.last_selected_symbols[:]
            self.last_symbol_switch_ts = time.time()
            self.last_selected_symbols = shown[:]
            self._log_event("symbol_switch", old=old_picks, new=shown, picks=picks, active=list(active), top_rows=rows[:5])
            await self._notify("🔁 Basket symbols: " + ", ".join(shown[:10]))
        self.stats.current_symbols = shown[:]
        return picks

    def _record_mid_price(self, symbol: str, bid: float, ask: float, tick: float) -> None:
        """Keep a tiny in-memory mid-price history for rebound entries."""
        try:
            sid = MexcFuturesClient.contract_id(symbol)
            mid = (float(bid) + float(ask)) / 2.0
            now = time.time()
            arr = self.mid_history.setdefault(sid, [])
            arr.append((now, mid, float(tick or 0.0)))
            settings = self._settings()
            lookback = max(float(settings.get("basket_rebound_lookback_sec") or 25.0), float(settings.get("wave_lookback_sec") or 20.0))
            cutoff = now - max(90.0, lookback * 4.0)
            if len(arr) > 300 or (arr and arr[0][0] < cutoff):
                self.mid_history[sid] = [x for x in arr[-300:] if x[0] >= cutoff]
        except Exception:
            pass

    def _recent_move_ticks(self, symbol: str, lookback_sec: float, tick: float) -> tuple[float | None, float]:
        """Return mid-price move in ticks over the lookback window, plus sample age."""
        sid = MexcFuturesClient.contract_id(symbol)
        arr = self.mid_history.get(sid) or []
        if len(arr) < 2 or tick <= 0:
            return None, 0.0
        now = time.time()
        target_ts = now - max(1.0, float(lookback_sec or 1.0))
        old = arr[0]
        for row in arr:
            if row[0] <= target_ts:
                old = row
            else:
                break
        cur = arr[-1]
        age = cur[0] - old[0]
        if age < max(1.0, float(lookback_sec or 1.0) * 0.35):
            return None, age
        return (float(cur[1]) - float(old[1])) / max(float(tick), 1e-12), age

    def _recent_move_pct(self, symbol: str, lookback_sec: float) -> tuple[float | None, float]:
        """Return mid-price percent move over lookback window, plus sample age.

        v0064 Wave Price Tsunami Basket deliberately uses this simple fact instead of
        internal score: price now versus price N seconds ago.
        """
        sid = MexcFuturesClient.contract_id(symbol)
        arr = self.mid_history.get(sid) or []
        if len(arr) < 2:
            return None, 0.0
        now = time.time()
        target_ts = now - max(1.0, float(lookback_sec or 1.0))
        old = arr[0]
        for row in arr:
            if row[0] <= target_ts:
                old = row
            else:
                break
        cur = arr[-1]
        age = cur[0] - old[0]
        if age < max(1.0, float(lookback_sec or 1.0) * 0.55):
            return None, age
        old_px = float(old[1])
        cur_px = float(cur[1])
        if old_px <= 0:
            return None, age
        return ((cur_px - old_px) / old_px) * 100.0, age

    def _classify_wave_vote(self, move_pct: float | None, s: dict[str, Any]) -> str:
        """Classify a coin for the global 10-second price vote.

        This is intentionally independent from trade filters. The market regime is
        decided from the whole active zero-fee universe (ALL symbols, no fixed 250 cap): LONG if the
        coin's mid price rose over the lookback, SHORT if it fell, NEUTRAL if the
        move is inside the noise band or still has no 10s history.
        """
        if move_pct is None:
            return "neutral"
        band = max(0.0, float(s.get("wave_price_min_move_pct") or 0.0))
        if move_pct >= band:
            return "long"
        if move_pct <= -band:
            return "short"
        return "neutral"

    def _summarize_wave_votes(self, vote_rows: list[dict[str, Any]]) -> dict[str, Any]:
        active = [r for r in vote_rows if r.get("vote") in {"long", "short", "neutral"}]
        total = len(active)
        long_n = sum(1 for r in active if r.get("vote") == "long")
        short_n = sum(1 for r in active if r.get("vote") == "short")
        neutral_n = sum(1 for r in active if r.get("vote") == "neutral")
        # Price-ready means the coin had a usable current book and a real 10s
        # comparison. No-book / no-history symbols are still counted as NEUTRAL
        # so the percentages are always from the real universe size.
        price_ready = sum(1 for r in active if r.get("move_pct") is not None)
        no_fresh_price = max(0, total - price_ready)
        denom = max(1, total)
        return {
            "active": total,
            "price_ready": price_ready,
            "no_fresh_price": no_fresh_price,
            "long": long_n,
            "short": short_n,
            "neutral": neutral_n,
            "long_pct": long_n / denom,
            "short_pct": short_n / denom,
            "neutral_pct": neutral_n / denom,
        }

    def _wave_leader_counts(self, s: dict[str, Any]) -> dict[str, Any]:
        """Fast market-direction check using leaders, not trade candidates.

        Candidate rows can be noisy or unavailable because of min-margin/depth filters.
        Leaders are used only for direction confirmation so the bot does not fire a
        five-coin basket on one noisy altcoin pulse.
        """
        raw = str(s.get("wave_leader_symbols") or "BTC_USDT,SOL_USDT,ETH_USDT")
        leaders = [MexcFuturesClient.contract_id(x.strip()) for x in raw.split(",") if x.strip()]
        lookback = float(s.get("wave_lookback_sec") or 20.0)
        min_move = float(s.get("wave_leader_min_move_ticks") or s.get("wave_min_move_ticks") or 3.0)
        out: dict[str, Any] = {"leaders": leaders, "long": 0, "short": 0, "moves": {}}
        for sym in leaders:
            arr = self.mid_history.get(sym) or []
            tick = float(arr[-1][2]) if arr else 0.0
            mv, age = self._recent_move_ticks(sym, lookback, tick)
            if mv is None:
                out["moves"][sym] = {"move_ticks": None, "age": age}
                continue
            out["moves"][sym] = {"move_ticks": mv, "age": age}
            if mv >= min_move:
                out["long"] += 1
            elif mv <= -min_move:
                out["short"] += 1
        return out

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

        # v0064 Wave Price Tsunami Basket: market direction is not taken from the old
        # book score. A coin votes LONG when its mid-price rose over the price
        # lookback, SHORT when it fell. This makes the wave detector transparent:
        # count rose/fell coins every ~10 seconds, then fire the basket.
        if bool(s.get("wave_basket_enabled", False)) and bool(s.get("wave_price_vote_enabled", True)):
            try:
                lookback = float(s.get("wave_price_lookback_sec") or s.get("wave_lookback_sec") or 10.0)
                min_pct = max(0.0, float(s.get("wave_price_min_move_pct") or 0.0))
                move_pct, sample_age = self._recent_move_pct(symbol, lookback)
                if move_pct is None:
                    self._log_debug("wave_price_wait_history", symbol=symbol, sample_age=sample_age, lookback=lookback)
                    return None
                if forced == "long":
                    return "long" if move_pct >= min_pct else None
                if forced == "short":
                    return "short" if move_pct <= -min_pct else None
                if move_pct >= min_pct:
                    return "long"
                if move_pct <= -min_pct:
                    return "short"
                self._log_debug("wave_price_neutral", symbol=symbol, move_pct=move_pct, sample_age=sample_age, min_pct=min_pct)
                return None
            except Exception as e:
                self._log_error("wave_price_direction_error", e, symbol=symbol)
                return None

        # Legacy edge/basket logic kept as fallback when wave_price_vote_enabled is off.
        m = await self._edge_metrics(symbol, s, book)
        ratio = float(s.get("min_imbalance_ratio") or 1.04)
        top_ratio = float(s.get("entry_top_imbalance_ratio") or 1.0)
        micro_min = float(s.get("entry_microprice_min_ticks") or 0.0)
        long_ok = (m["depth_bid"] >= m["depth_ask"] * ratio and m["bid_top"] >= m["ask_top"] * top_ratio and m["micro_ticks"] >= micro_min)
        short_ok = (m["depth_ask"] >= m["depth_bid"] * ratio and m["ask_top"] >= m["bid_top"] * top_ratio and m["micro_ticks"] <= -micro_min)
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



    def _pick_wave_middle_rows(self, rows: list[dict[str, Any]], side: str, need: int, s: dict[str, Any], blocked: set[str] | None = None) -> list[dict[str, Any]]:
        """Pick tradeable same-side coins from the middle 25-60% wave range.

        The market vote is calculated from the whole universe. Entries must not
        blindly take the hottest names or top-up from the score top: both initial
        basket and replacement slots use the same middle-slice rule.
        """
        blocked = {MexcFuturesClient.contract_id(x) for x in (blocked or set()) if x}
        side_rows: list[dict[str, Any]] = []
        for r in rows:
            sym = MexcFuturesClient.contract_id(r.get("symbol"))
            if not sym or sym in blocked:
                continue
            if (r.get("vote") or r.get("bias")) != side or r.get("move_pct") is None:
                continue
            side_rows.append(r)
        if not side_rows or need <= 0:
            return []
        sorted_side = sorted(side_rows, key=lambda r: abs(float(r.get("move_pct") or 0.0)), reverse=True)
        n = len(sorted_side)
        start_pct = max(0.0, min(0.95, float(s.get("wave_pick_start_pct") or 0.25)))
        end_pct = max(start_pct + 0.01, min(1.0, float(s.get("wave_pick_end_pct") or 0.60)))
        start_i = min(n - 1, int(n * start_pct))
        end_i = max(start_i + need, int(n * end_pct))
        picked = list(sorted_side[start_i:min(n, end_i)][:need])
        if len(picked) < need:
            have = {MexcFuturesClient.contract_id(r.get("symbol")) for r in picked if r.get("symbol")}
            for r in sorted_side:
                sym = MexcFuturesClient.contract_id(r.get("symbol"))
                if not sym or sym in have:
                    continue
                picked.append(r)
                have.add(sym)
                if len(picked) >= need:
                    break
        return picked

    # ------------------------- Mirror Lab v0064 -------------------------
    def clear_mirror_lab(self) -> str:
        """Clear the virtual-only mirror lab buffer and last report."""
        self.mirror_lab_snapshots = []
        self.mirror_lab_last_report = {}
        try:
            self.mirror_lab_log_path.parent.mkdir(parents=True, exist_ok=True)
            self.mirror_lab_log_path.write_text("", encoding="utf-8")
        except Exception:
            pass
        return "🪞 Mirror Lab очищен: виртуальные снимки и отчёт сброшены."

    def _record_mirror_snapshot(self, rows: list[dict[str, Any]], s: dict[str, Any]) -> None:
        """Record a compact scan snapshot for accelerated virtual replay.

        This never opens orders and never mutates live trading decisions. It is a
        small rolling tape of already-scanned prices/books, used by /mirror_test
        to replay bad models and their inverse side quickly.
        """
        if not bool(s.get("mirror_lab_enabled", False)):
            return
        now = time.time()
        compact: list[dict[str, Any]] = []
        for r in rows or []:
            sym = MexcFuturesClient.contract_id(r.get("symbol"))
            vote = str(r.get("vote") or r.get("bias") or "").lower()
            if not sym or vote not in {"long", "short"}:
                continue
            bid = float(r.get("bid") or 0.0)
            ask = float(r.get("ask") or 0.0)
            if bid <= 0 or ask <= 0 or ask < bid:
                continue
            compact.append({
                "symbol": sym,
                "vote": vote,
                "bid": bid,
                "ask": ask,
                "mid": (bid + ask) / 2.0,
                "score": float(r.get("score") or 0.0),
                "move_pct": float(r.get("move_pct") or 0.0),
                "spread_ticks": float(r.get("spread_ticks") or 0.0),
                "depth_min": float(r.get("depth_min") or 0.0),
            })
        if not compact:
            return
        summary = self._summarize_wave_votes(getattr(self, "last_wave_vote_rows", []) or [])
        leader_summary = self._summarize_wave_votes(getattr(self, "last_wave_leader_vote_rows", []) or [])
        snap = {
            "ts": now,
            "rows": compact,
            "summary": summary,
            "leader_summary": leader_summary,
        }
        # Avoid duplicates from forced scan presses in the same second.
        if self.mirror_lab_snapshots and now - float(self.mirror_lab_snapshots[-1].get("ts") or 0.0) < 0.75:
            self.mirror_lab_snapshots[-1] = snap
        else:
            self.mirror_lab_snapshots.append(snap)
        max_snaps = max(50, int(s.get("mirror_lab_snapshot_max") or 1800))
        self.mirror_lab_snapshots = self.mirror_lab_snapshots[-max_snaps:]

    def _mirror_rows_by_symbol(self, snap: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {str(r.get("symbol")): r for r in (snap.get("rows") or []) if r.get("symbol")}

    def _mirror_pick(self, rows: list[dict[str, Any]], side: str, mode: str, need: int = 5) -> list[dict[str, Any]]:
        side_rows = [r for r in rows if str(r.get("vote")) == side and r.get("move_pct") is not None]
        if len(side_rows) < need:
            return []
        # Strength is absolute 10s price movement. These deliberately bad models
        # are virtual only; they are not used by live execution unless a future
        # mirror live gate explicitly allows it.
        ranked = sorted(side_rows, key=lambda r: abs(float(r.get("move_pct") or 0.0)), reverse=True)
        if mode in {"bad_top_chaser", "bad_late_momentum", "bad_no_hold"}:
            return ranked[:need]
        if mode == "bad_bottom_catcher":
            return list(reversed(ranked))[:need]
        if mode == "bad_random_noise":
            # deterministic pseudo-random per snapshot content, so repeated reports are stable
            return sorted(side_rows, key=lambda r: (str(r.get("symbol"))))[:need]
        return ranked[:need]

    def _mirror_bad_signals(self, snap: dict[str, Any], s: dict[str, Any]) -> list[dict[str, Any]]:
        rows = list(snap.get("rows") or [])
        summary = snap.get("summary") or {}
        leader_summary = snap.get("leader_summary") or {}
        need = max(1, int(s.get("wave_positions") or 5))
        long_n = int(summary.get("long") or 0)
        short_n = int(summary.get("short") or 0)
        side = "long" if long_n >= short_n else "short"
        opposite = "short" if side == "long" else "long"
        signals: list[dict[str, Any]] = []

        def add(model: str, model_side: str, pick_mode: str, reason: str) -> None:
            picks = self._mirror_pick(rows, model_side, pick_mode, need=need)
            if len(picks) >= need:
                signals.append({
                    "model": model,
                    "side": model_side,
                    "mirror_side": "short" if model_side == "long" else "long",
                    "symbols": [str(r.get("symbol")) for r in picks[:need]],
                    "reason": reason,
                })

        # Bad model 1: chase the hottest side immediately, without HOLD.
        if max(long_n, short_n) >= need:
            add("BAD_NO_HOLD", side, "bad_no_hold", "majority side without hold")
            add("BAD_TOP_CHASER", side, "bad_top_chaser", "hottest movers on majority side")

        # Bad model 2: late momentum only when a side is already very crowded.
        active = max(1, int(summary.get("active") or 1))
        if max(long_n, short_n) / active >= 0.45:
            add("BAD_LATE_MOMENTUM", side, "bad_late_momentum", "crowded momentum chase")

        # Bad model 3: take the weak/opposite side while market has a majority.
        if min(long_n, short_n) >= need and abs(long_n - short_n) >= 5:
            add("BAD_WEAK_SIDE", opposite, "bad_top_chaser", "weak side against majority")

        # Bad model 4: bottom-catch the weakest coins that technically vote in the majority side.
        if max(long_n, short_n) >= need:
            add("BAD_BOTTOM_CATCHER", side, "bad_bottom_catcher", "weakest confirmed movers on majority side")

        # Bad model 5: TOP10 leader chase. If TOP10 is decisive, blindly chase it.
        lead_long = int(leader_summary.get("long") or 0)
        lead_short = int(leader_summary.get("short") or 0)
        if max(lead_long, lead_short) >= 7:
            lead_side = "long" if lead_long >= lead_short else "short"
            add("BAD_TOP10_CHASER", lead_side, "bad_top_chaser", "blind TOP10 chase")
        return signals

    def _mirror_find_exit_index(self, start_i: int, horizon_sec: float) -> int | None:
        snaps = self.mirror_lab_snapshots
        if start_i >= len(snaps) - 1:
            return None
        target = float(snaps[start_i].get("ts") or 0.0) + horizon_sec
        for j in range(start_i + 1, len(snaps)):
            if float(snaps[j].get("ts") or 0.0) >= target:
                return j
        return None

    def _mirror_virtual_pnl(self, entry: dict[str, Any], exit_snap: dict[str, Any], side: str, symbols: list[str], notional: float, fee_bps: float) -> float | None:
        exit_rows = self._mirror_rows_by_symbol(exit_snap)
        total = 0.0
        fee_cost = max(0.0, fee_bps) / 10000.0 * notional * 2.0 * len(symbols)
        for sym in symbols:
            er = next((r for r in entry.get("rows", []) if r.get("symbol") == sym), None)
            xr = exit_rows.get(sym)
            if not er or not xr:
                return None
            entry_bid = float(er.get("bid") or 0.0)
            entry_ask = float(er.get("ask") or 0.0)
            exit_bid = float(xr.get("bid") or 0.0)
            exit_ask = float(xr.get("ask") or 0.0)
            if min(entry_bid, entry_ask, exit_bid, exit_ask) <= 0:
                return None
            if side == "long":
                total += ((exit_bid - entry_ask) / entry_ask) * notional
            else:
                total += ((entry_bid - exit_ask) / entry_bid) * notional
        return total - fee_cost

    def mirror_lab_report_text(self) -> str:
        """Run an accelerated virtual replay over recorded scan snapshots."""
        s = self._settings()
        enabled = bool(s.get("mirror_lab_enabled", False))
        snaps = list(self.mirror_lab_snapshots or [])
        min_snaps = max(2, int(s.get("mirror_lab_min_snapshots") or 20))
        horizon = max(5.0, float(s.get("mirror_lab_horizon_sec") or 60.0))
        notional = max(1.0, float(s.get("mirror_lab_virtual_notional_usdt") or 10.0))
        fee_bps = max(0.0, float(s.get("mirror_lab_virtual_fee_bps") or 0.0))
        if len(snaps) < min_snaps:
            return (
                f"🪞 Mirror Lab {s.get('bot_version', 'v0064')}\n"
                f"Mode: {'ON' if enabled else 'OFF'}\n\n"
                f"Нужно накопить снимки: {len(snaps)}/{min_snaps}.\n"
                f"Команда: /mirror_test start — включить сбор виртуальных снимков.\n"
                f"Это не торгует реальными деньгами."
            )
        stats: dict[str, dict[str, Any]] = {}
        events: list[dict[str, Any]] = []
        for i, snap in enumerate(snaps):
            exit_i = self._mirror_find_exit_index(i, horizon)
            if exit_i is None:
                continue
            exit_snap = snaps[exit_i]
            for sig in self._mirror_bad_signals(snap, s):
                model = str(sig.get("model"))
                symbols = list(sig.get("symbols") or [])
                original_side = str(sig.get("side"))
                mirror_side = str(sig.get("mirror_side"))
                orig_pnl = self._mirror_virtual_pnl(snap, exit_snap, original_side, symbols, notional, fee_bps)
                mir_pnl = self._mirror_virtual_pnl(snap, exit_snap, mirror_side, symbols, notional, fee_bps)
                if orig_pnl is None or mir_pnl is None:
                    continue
                st = stats.setdefault(model, {"n": 0, "orig": 0.0, "mir": 0.0, "mir_wins": 0, "orig_wins": 0, "mir_gain": 0.0, "mir_loss": 0.0})
                st["n"] += 1
                st["orig"] += orig_pnl
                st["mir"] += mir_pnl
                st["mir_wins"] += 1 if mir_pnl > 0 else 0
                st["orig_wins"] += 1 if orig_pnl > 0 else 0
                if mir_pnl > 0:
                    st["mir_gain"] += mir_pnl
                else:
                    st["mir_loss"] += abs(mir_pnl)
                if len(events) < 25:
                    events.append({
                        "ts": snap.get("ts"),
                        "model": model,
                        "original_side": original_side,
                        "mirror_side": mirror_side,
                        "symbols": symbols,
                        "original_pnl": round(orig_pnl, 6),
                        "mirror_pnl": round(mir_pnl, 6),
                    })
        ranked = []
        for model, st in stats.items():
            n = max(1, int(st["n"]))
            pf = (float(st["mir_gain"]) / float(st["mir_loss"])) if float(st["mir_loss"]) > 0 else (999.0 if float(st["mir_gain"]) > 0 else 0.0)
            ranked.append((float(st["mir"]), model, st, pf, float(st["mir_wins"]) / n, float(st["orig_wins"]) / n))
        ranked.sort(reverse=True, key=lambda x: x[0])
        self.mirror_lab_last_report = {"ts": time.time(), "stats": stats, "events": events, "horizon": horizon, "notional": notional}
        try:
            self.mirror_lab_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.mirror_lab_log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(self.mirror_lab_last_report, ensure_ascii=False, default=str) + "\n")
        except Exception:
            pass
        lines = [
            f"🪞 Mirror Lab {s.get('bot_version', 'v0064')}",
            f"Mode: {'ON' if enabled else 'OFF'} | snapshots {len(snaps)} | horizon {horizon:.0f}s",
            f"Virtual size: {notional:.2f} USDT/slot | fee {fee_bps:.2f} bps",
            "",
            "РЕЗУЛЬТАТ ORIGINAL vs MIRROR",
        ]
        if not ranked:
            lines.append("Пока нет завершённых виртуальных корзин. Подожди ещё 1–2 минуты или увеличь время сбора.")
            return "\n".join(lines)
        for mir_total, model, st, pf, mir_wr, orig_wr in ranked[:6]:
            n = int(st["n"])
            lines.append(
                f"{model}: n={n} | original {float(st['orig']):+.4f} | mirror {float(st['mir']):+.4f} | "
                f"mirror WR {mir_wr*100:.0f}% | PF {pf:.2f}"
            )
        best = ranked[0]
        need_live = int(s.get("mirror_lab_min_signals_for_live") or 50)
        best_n = int(best[2]["n"])
        verdict = "недостаточно статистики"
        if best_n >= need_live and best[0] > 0 and best[3] >= 1.2 and best[4] >= 0.55:
            verdict = "mirror edge найден, можно рассматривать ASSIST, но не автолайв без ручного подтверждения"
        elif best[0] > 0:
            verdict = f"зеркало пока лучше, но нужно минимум {need_live} сигналов; сейчас {best_n}"
        lines.extend([
            "",
            f"ЛУЧШИЙ: {best[1]} mirror {best[0]:+.4f}",
            f"Вердикт: {verdict}",
            "",
            "Команды: /mirror_test start | stop | report | clear",
        ])
        return "\n".join(lines)[:3900]

    def _detect_wave_signal(self, rows: list[dict[str, Any]], s: dict[str, Any]) -> tuple[str | None, list[dict[str, Any]], dict[str, Any]]:
        """Detect v0064 Price Tsunami from the selected market signal source.

        ALL mode:
        - Normal Wave: dominance >= 75%.
        - Early Wave: dominance >= 65% and 60s рост >= +15п.п..
        - Tsunami: dominance >= 75% and 60s рост >= +15п.п..

        TOP10 mode, максимально похожий на ALL:
        - Normal Wave: 7/10 leaders in one direction.
        - Early Wave: 7/10 leaders plus +2 leaders growth over 60s.
        - Tsunami: 8/10 leaders in one direction.

        Final 5 trade picks are always selected from the full zero-fee trade universe.
        """
        now = time.time()
        need = max(1, int(s.get("wave_positions") or s.get("max_positions") or 5))
        normal_ratio = max(0.1, min(1.0, float(s.get("wave_min_side_ratio") or 0.75)))
        early_ratio = max(0.0, min(1.0, float(s.get("wave_early_min_side_ratio") or 0.65)))
        accel_trigger = max(0.0, float(s.get("wave_accel_trigger_pct") or 15.0) / 100.0)
        accel_lookback = max(5.0, float(s.get("wave_accel_lookback_sec") or 60.0))

        signal_mode = str(s.get("wave_market_signal_mode") or "all_zero_total").lower().strip()
        if signal_mode not in {"all_zero_total", "top10_leaders"}:
            signal_mode = "all_zero_total"

        # Direction source can be switched. all_zero_total = current mode, full
        # zero-fee universe decides LONG/SHORT/NEUTRAL. top10_leaders = only the
        # 10 most liquid non-stable zero-fee symbols decide market direction, while
        # entries still use `rows` from the full zero-fee universe.
        if signal_mode != getattr(self, "wave_signal_mode_last", signal_mode):
            self.wave_dominance_history.clear()
            self.wave_signal_hold_samples.clear()
            self.wave_signal_hold_last_sample_ts = 0.0
            self.wave_signal_mode_last = signal_mode

        if signal_mode == "top10_leaders":
            signal_rows = list(getattr(self, "last_wave_leader_vote_rows", []) or [])
            signal_summary = self._summarize_wave_votes(signal_rows)
            leader_symbols = list(getattr(self, "last_wave_leader_symbols", []) or [])
        else:
            signal_rows = list(getattr(self, "last_wave_vote_rows", []) or [])
            signal_summary = self._summarize_wave_votes(signal_rows)
            leader_symbols = []

        active_n = int(signal_summary.get("active") or 0)
        if active_n <= 0:
            self.wave_candidate_side = None
            self.wave_candidate_count = 0
            details = {
                "reason": "warming_price_history",
                "signal_mode": signal_mode,
                "leader_symbols": leader_symbols,
                "active": 0,
                "price_ready": 0,
                "no_fresh_price": 0,
                "long": 0,
                "short": 0,
                "neutral": 0,
                "long_pct": 0.0,
                "short_pct": 0.0,
                "neutral_pct": 0.0,
                "mode": "wait",
            }
            self.stats.wave_state = details | {"side": "-", "open_target": need, "open_count": len(self.stats.open_position_symbols)}
            return None, [], details

        long_dom = float(signal_summary.get("long_pct") or 0.0)
        short_dom = float(signal_summary.get("short_pct") or 0.0)
        neutral_dom = float(signal_summary.get("neutral_pct") or 0.0)
        long_count = int(signal_summary.get("long") or 0)
        short_count = int(signal_summary.get("short") or 0)
        neutral_count = int(signal_summary.get("neutral") or 0)
        side = "long" if long_count >= short_count else "short"
        dominance = long_dom if side == "long" else short_dom
        opposite = short_dom if side == "long" else long_dom

        self.wave_dominance_history.append((now, long_dom, short_dom))
        cutoff = now - max(accel_lookback * 3.0, 180.0)
        self.wave_dominance_history = [x for x in self.wave_dominance_history[-600:] if x[0] >= cutoff]
        old = self.wave_dominance_history[0]
        target_ts = now - accel_lookback
        for row in self.wave_dominance_history:
            if row[0] <= target_ts:
                old = row
            else:
                break
        old_long, old_short = float(old[1]), float(old[2])
        accel_long = long_dom - old_long
        accel_short = short_dom - old_short
        accel = accel_long if side == "long" else accel_short

        old_long_count = int(round(old_long * active_n))
        old_short_count = int(round(old_short * active_n))
        count_accel_long = long_count - old_long_count
        count_accel_short = short_count - old_short_count
        count_accel = count_accel_long if side == "long" else count_accel_short

        if signal_mode == "top10_leaders":
            normal_count_need = max(1, int(s.get("wave_top10_normal_count") or 7))
            tsunami_count_need = max(normal_count_need, int(s.get("wave_top10_tsunami_count") or 8))
            accel_count_need = max(0, int(s.get("wave_top10_accel_count") or 2))
            # v0064: TOP10 is mapped to the ALL-zero logic:
            # - 7/10 current direction = NORMAL, same as broad dominance.
            # - 7/10 + growth of +2 leaders over 60s = EARLY, same as +15p.p. acceleration.
            # - 8/10 current direction = TSUNAMI, because this is a very strong leader consensus.
            # Entries are still selected from the full zero-fee trade universe.
            side_count = long_count if side == "long" else short_count
            is_tsunami = side_count >= tsunami_count_need
            is_early = (
                not is_tsunami
                and side_count >= normal_count_need
                and count_accel >= accel_count_need
            )
            is_normal = side_count >= normal_count_need and not is_tsunami and not is_early
            raw_mode = "tsunami" if is_tsunami else ("early" if is_early else ("normal" if is_normal else "wait"))
            tsunami_requires_accel = False
        else:
            normal_count_need = 0
            tsunami_count_need = 0
            accel_count_need = 0
            tsunami_requires_accel = False
            is_tsunami = dominance >= normal_ratio and accel >= accel_trigger
            is_normal = dominance >= normal_ratio and accel < accel_trigger
            is_early = dominance >= early_ratio and dominance < normal_ratio and dominance > opposite and accel >= accel_trigger
            raw_mode = "tsunami" if is_tsunami else ("normal" if is_normal else ("early" if is_early else "wait"))

        vote_summary = signal_summary

        # v0064 HOLD rule: +15p.p. must be stable, not a one-scan spike.
        # Default rule: 4 of the last 5 sampled checks over about 10 seconds.
        # This is stronger than the old 3/3s hold and tolerant to one noisy failed check.
        hold_checks = max(1, int(s.get("wave_signal_hold_checks") or 5))
        hold_required = max(1, min(hold_checks, int(s.get("wave_signal_hold_required") or max(1, hold_checks - 1))))
        hold_sec = max(0.0, float(s.get("wave_signal_hold_sec") or 10.0))
        hold_key = f"{raw_mode}:{side}" if raw_mode != "wait" else None
        sample_key = hold_key or "wait"
        # Spread 5 hold samples across the configured hold window. The run loop is faster
        # than the market scan, so sampling every tick would make check counts meaningless.
        sample_gap = hold_sec / max(1, hold_checks - 1) if hold_sec > 0 else max(0.2, float(s.get("scan_interval_sec") or 1.0))
        sample_gap = max(0.2, sample_gap)
        last_sample_key = self.wave_signal_hold_samples[-1][1] if self.wave_signal_hold_samples else None
        if (not self.wave_signal_hold_samples) or (sample_key != last_sample_key) or (now - self.wave_signal_hold_last_sample_ts >= sample_gap):
            self.wave_signal_hold_samples.append((now, sample_key))
            self.wave_signal_hold_last_sample_ts = now
        retention = max(hold_sec * 3.0, 60.0)
        self.wave_signal_hold_samples = [x for x in self.wave_signal_hold_samples[-100:] if x[0] >= now - retention]
        recent_samples = self.wave_signal_hold_samples[-hold_checks:]
        recent_keys = [x[1] for x in recent_samples]
        hold_match_count = sum(1 for k in recent_keys if hold_key and k == hold_key)
        hold_for = 0.0
        if len(recent_samples) >= hold_checks:
            hold_for = max(0.0, recent_samples[-1][0] - recent_samples[0][0])
        self.wave_signal_hold_key = hold_key
        self.wave_signal_hold_count = hold_match_count
        if hold_key and any(k == hold_key for k in recent_keys):
            first_match = next((ts for ts, key in recent_samples if key == hold_key), now)
            self.wave_signal_hold_since = first_match
        else:
            self.wave_signal_hold_since = 0.0
        signal_stable = (
            raw_mode != "wait"
            and len(recent_samples) >= hold_checks
            and hold_match_count >= hold_required
            and hold_for >= hold_sec
        )
        mode = raw_mode if signal_stable else "wait"

        side_rows = [r for r in rows if (r.get("vote") or r.get("bias")) == side and r.get("move_pct") is not None]
        target_profit = float(s.get("wave_tsunami_target_profit_usdt") or 0.10) if is_tsunami else float(s.get("wave_normal_target_profit_usdt") or s.get("wave_target_profit_usdt") or 0.05)
        cycle_leverage = int(s.get("wave_tsunami_leverage") or 10) if is_tsunami else int(s.get("wave_normal_leverage") or 5)
        details = {
            "signal_mode": signal_mode,
            "leader_symbols": leader_symbols,
            "top10_normal_count": normal_count_need,
            "top10_tsunami_count": tsunami_count_need,
            "top10_accel_count": accel_count_need,
            "top10_tsunami_requires_accel": tsunami_requires_accel,
            "old_long_count": old_long_count,
            "old_short_count": old_short_count,
            "long_count_accel": count_accel_long,
            "short_count_accel": count_accel_short,
            "active": active_n,
            "price_ready": int(vote_summary.get("price_ready") or 0),
            "no_fresh_price": int(vote_summary.get("no_fresh_price") or 0),
            "long": int(vote_summary.get("long") or 0),
            "short": int(vote_summary.get("short") or 0),
            "neutral": int(vote_summary.get("neutral") or 0),
            "long_pct": long_dom,
            "short_pct": short_dom,
            "neutral_pct": neutral_dom,
            "side": side,
            "dominance": dominance,
            "opposite": opposite,
            "acceleration": accel,
            "long_acceleration": accel_long,
            "short_acceleration": accel_short,
            "old_long_pct": old_long,
            "old_short_pct": old_short,
            "normal_ratio": normal_ratio,
            "early_ratio": early_ratio,
            "accel_trigger": accel_trigger,
            "mode": mode,
            "detected_mode": raw_mode,
            "pending_mode": raw_mode if raw_mode != "wait" and not signal_stable else "",
            "pending_side": side if raw_mode != "wait" and not signal_stable else "",
            "signal_hold_count": self.wave_signal_hold_count,
            "signal_hold_need": hold_required,
            "signal_hold_required": hold_required,
            "signal_hold_checks": hold_checks,
            "signal_hold_for": hold_for,
            "signal_hold_sec": hold_sec,
            "need": need,
            "trade_candidates": len(side_rows),
            "target": target_profit,
            "leverage": cycle_leverage,
        }
        self.stats.wave_state = {
            "signal_mode": signal_mode,
            "leader_symbols": leader_symbols,
            "top10_normal_count": normal_count_need,
            "top10_tsunami_count": tsunami_count_need,
            "top10_accel_count": accel_count_need,
            "top10_tsunami_requires_accel": tsunami_requires_accel,
            "old_long_count": old_long_count,
            "old_short_count": old_short_count,
            "long_count_accel": count_accel_long,
            "short_count_accel": count_accel_short,
            "side": side,
            "mode": mode,
            "detected_mode": raw_mode,
            "pending_mode": raw_mode if raw_mode != "wait" and not signal_stable else "",
            "pending_side": side if raw_mode != "wait" and not signal_stable else "",
            "signal_hold_count": self.wave_signal_hold_count,
            "signal_hold_need": hold_required,
            "signal_hold_required": hold_required,
            "signal_hold_checks": hold_checks,
            "signal_hold_for": hold_for,
            "signal_hold_sec": hold_sec,
            "dominance": dominance,
            "acceleration": accel,
            "long_acceleration": accel_long,
            "short_acceleration": accel_short,
            "old_long_pct": old_long,
            "old_short_pct": old_short,
            "active": active_n,
            "price_ready": int(vote_summary.get("price_ready") or 0),
            "no_fresh_price": int(vote_summary.get("no_fresh_price") or 0),
            "long": int(vote_summary.get("long") or 0),
            "short": int(vote_summary.get("short") or 0),
            "neutral": int(vote_summary.get("neutral") or 0),
            "long_pct": long_dom,
            "short_pct": short_dom,
            "neutral_pct": neutral_dom,
            "trade_candidates": len(side_rows),
            "open_target": need,
            "open_count": len(self.stats.open_position_symbols),
            "target": target_profit,
            "leverage": cycle_leverage,
        }
        if mode == "wait":
            self.wave_candidate_side = None
            self.wave_candidate_count = 0
            if raw_mode != "wait" and not signal_stable:
                details["reason"] = f"signal_hold: {self.wave_signal_hold_count}/{hold_required} of last {hold_checks} checks, {hold_for:.1f}/{hold_sec:.1f}s"
                self.stats.wave_state["reason"] = details["reason"]
                return None, [], details
            if signal_mode == "top10_leaders":
                details["reason"] = f"TOP10: нет {normal_count_need}/{active_n} direction; EARLY нужен +{accel_count_need} мон. за 60с; TSUNAMI нужен {tsunami_count_need}/{active_n}"
            else:
                details["reason"] = "нет 65% + ускорения и нет 75% dominance"
            self.stats.wave_state["reason"] = details["reason"]
            return None, [], details
        if len(side_rows) < need:
            self.wave_candidate_side = None
            self.wave_candidate_count = 0
            details["reason"] = "not_enough_trade_candidates_after_market_vote"
            return None, [], details

        confirm_need = max(1, int(s.get("wave_entry_confirmations") or 1))
        if self.wave_candidate_side == side and now >= self.wave_cooldown_until_ts:
            self.wave_candidate_count += 1
        else:
            self.wave_candidate_side = side
            self.wave_candidate_count = 1
        details["confirm"] = self.wave_candidate_count
        details["confirm_need"] = confirm_need
        if self.wave_candidate_count < confirm_need:
            details["reason"] = "confirming"
            return None, [], details

        picked = self._pick_wave_middle_rows(rows, side, need, s)
        if len(picked) < need:
            details["reason"] = "not_enough_picks_after_middle_slice"
            return None, [], details

        details["pick_start_pct"] = max(0.0, min(0.95, float(s.get("wave_pick_start_pct") or 0.25)))
        details["pick_end_pct"] = max(details["pick_start_pct"] + 0.01, min(1.0, float(s.get("wave_pick_end_pct") or 0.60)))
        details["pick_pool_size"] = len(side_rows)
        details["selected"] = [r.get("symbol") for r in picked]
        details["cycle_leverage"] = cycle_leverage
        details["cycle_target"] = target_profit
        return side, picked, details

    async def _wave_loop_tick(self, s: dict[str, Any]) -> None:
        if self.active_tasks:
            return
        now = time.time()
        if now < self.wave_cooldown_until_ts:
            self.stats.last_action = f"wave cooldown: {self.wave_cooldown_until_ts - now:.0f}s"
            return
        client = await self._ensure_client()
        # v0064: private positions check is throttled and timed out. It used to be
        # called every loop tick and could make the scan appear frozen.
        try:
            pos_interval = max(2.0, float(s.get("existing_positions_check_interval_sec") or 8.0))
        except Exception:
            pos_interval = 8.0
        if now - self._last_existing_positions_ts >= pos_interval:
            self._last_existing_positions_ts = now
            try:
                fetched = await asyncio.wait_for(client.fetch_positions(), timeout=max(4.0, float(s.get("mexc_private_timeout") or 15.0)))
                existing = [p for p in fetched if str(p.get("symbol") or "").upper().endswith("_USDT")]
                self._cached_existing_positions = existing
            except Exception as e:
                existing = list(self._cached_existing_positions or [])
                self._log_error("wave_existing_positions_check_error", e)
        else:
            existing = list(self._cached_existing_positions or [])
        if existing:
            self.stats.open_position_symbols = [str(p.get("symbol")) for p in existing if p.get("symbol")]
            self.stats.current_symbols = self.stats.open_position_symbols[: max(1, int(s.get("wave_positions") or 5))]
            self.stats.last_action = "wave wait: existing positions detected; use Close All or let previous manager finish before new wave"
            self._log_event("wave_skip_existing_positions", positions=existing)
            return
        rows = await self._refresh_market_scan(s, force=False)
        side, picks, details = self._detect_wave_signal(rows, s)
        self._log_debug("wave_signal_check", **details, picks=[r.get("symbol") for r in picks])
        if not side:
            reason = details.get("reason") or "waiting"
            self.stats.last_action = (
                f"WAIT {str(details.get('side','-')).upper()}: {reason} | "
                f"LONG {float(details.get('long_pct') or 0):.0%} / SHORT {float(details.get('short_pct') or 0):.0%} / NEUTRAL {float(details.get('neutral_pct') or 0):.0%} "
                f"({details.get('active', 0)} active) | за 60с {self._fmt_pp(details.get('acceleration') or 0)}"
            )
            return
        key = f"WAVE_{side.upper()}"
        self.stats.current_symbols = [r.get("symbol") for r in picks]
        self.stats.wave_state.update({"side": side, "mode": details.get("mode", "wave"), "target": float(details.get("cycle_target") or 0.0), "leverage": int(details.get("cycle_leverage") or s.get("leverage") or 5), "open_target": int(s.get("wave_positions") or 5), "open_count": 0, "selected": [r.get("symbol") for r in picks], "open_skips": []})
        pct_txt = f"LONG {float(details.get('long_pct') or 0):.0%} / SHORT {float(details.get('short_pct') or 0):.0%} / NEUTRAL {float(details.get('neutral_pct') or 0):.0%}"
        self.stats.last_action = f"FIRE {side.upper()} {details.get('mode','wave').upper()}: {pct_txt}; opening basket {len(picks)}/{int(s.get('wave_positions') or 5)} | за 60с {self._fmt_pp(details.get('acceleration') or 0)}"
        self._log_event("wave_fire", side=side, picks=picks, details=details)
        mode_title = self._mode_title(details.get("mode"))
        conclusion = "рынка нет" if str(details.get("mode")) == "wait" else (f"TSUNAMI {side.upper()}" if str(details.get("mode")) == "tsunami" else f"рынок {side.upper()}")
        await self._notify(
            f"🚀 {mode_title} {side.upper()}\n"
            f"PRICE 10s: {pct_txt} ({details.get('active', 0)} active)\n"
            f"За 60с {self._wave_accel_lines(details)[0]}\n"
            f"За 60с {self._wave_accel_lines(details)[1]}\n"
            f"Вывод: {conclusion}. Открываю 5 {side.upper()} из середины 25-60%.\n"
            f"Leverage {details.get('cycle_leverage')}x | REAL NET TP +${float(details.get('cycle_target') or 0):.2f}\n"
            f"Монеты: " + ", ".join(self.stats.current_symbols[:10])
        )
        self.active_tasks[key] = asyncio.create_task(self._wave_basket_cycle(side, picks, details), name=key)
        # Reset confirmation so the next cycle must see a fresh wave.
        self.wave_candidate_side = None
        self.wave_candidate_count = 0

    async def _open_wave_position(self, symbol: str, direction: str, s: dict[str, Any], equity_before: float | None) -> dict[str, Any] | None:
        """Open one position for the wave basket without independent management."""
        self.stats.wave_state.pop("last_open_skip", None)
        if self._is_ignored_symbol(symbol, s):
            self._remember_wave_open_skip(symbol, "ignored")
            return None
        client = await self._ensure_client()
        # Safety for retry rounds: if the previous aggressive LIMIT actually filled
        # but the position read was delayed/rate-limited, never place a second entry
        # for the same symbol. First check whether this slot is already open.
        try:
            existing_pos = await client.find_position(symbol, direction)
        except Exception as e:
            self.stats.last_error = f"{symbol}: {self._friendly_error(e)}"
            self._remember_wave_open_skip(symbol, "order_error", error=self._friendly_error(e), stage="pre_existing_position_check")
            self._log_error("wave_existing_position_check_error", e, symbol=symbol, direction=direction)
            return None
        if existing_pos:
            self._log_event("wave_entry_existing_position_reused", symbol=symbol, direction=direction, position=existing_pos)
            if symbol not in self.stats.open_position_symbols:
                self.stats.open_position_symbols.append(symbol)
            return existing_pos
        book = await self._depth(symbol, limit=10)
        if not book.get("bids") or not book.get("asks"):
            self._remember_wave_open_skip(symbol, "no_book")
            self._log_debug("wave_open_no_book", symbol=symbol)
            return None
        bid, ask = float(book["bids"][0][0]), float(book["asks"][0][0])
        tick = await client.price_tick(symbol)
        self._record_mid_price(symbol, bid, ask, tick)
        spread_ticks = (ask - bid) / max(tick, 1e-12)
        if spread_ticks > float(s.get("max_spread_ticks") or 2) + 1e-9:
            self._remember_wave_open_skip(symbol, "spread", spread_ticks=spread_ticks, max_spread_ticks=float(s.get("max_spread_ticks") or 2))
            self._log_debug("wave_open_spread_reject", symbol=symbol, spread_ticks=spread_ticks)
            return None
        # Re-evaluate the coin in the same wave side right before open. This is cheap
        # and prevents stale rows from entering after a flip, but it does not wait.
        direction_now = await self._choose_direction(symbol, s, book)
        if direction_now != direction:
            self._remember_wave_open_skip(symbol, "side_flip", want=direction, now=direction_now)
            self._log_debug("wave_open_side_flip", symbol=symbol, want=direction, now=direction_now)
            return None
        if not await self._pretrade_fee_guard(symbol, s, client):
            self._remember_wave_open_skip(symbol, "fee_guard")
            return None
        leverage = int(s.get("leverage") or 5)
        open_type = int(s.get("open_type") or 1)
        margin_usdt, margin_note = await self._position_margin_usdt(s)
        if margin_usdt <= 0:
            self._remember_wave_open_skip(symbol, "no_margin", margin_note=margin_note)
            self._log_event("wave_open_no_margin", symbol=symbol, margin_note=margin_note)
            return None
        # v0064: aggressive entry means: choose a price that already exists in
        # the opposite side of the book and has enough cumulative liquidity for
        # this slot. We do NOT place a passive maker order and wait in queue.
        # LONG consumes asks; SHORT consumes bids. If the best level is enough,
        # entry_price stays best ask/bid. If not, allow a tiny bounded sweep to
        # the nearest existing level that can fill the whole vol, then cancel
        # leftovers after TTL.
        entry_price = ask if direction == "long" else bid
        try:
            vol = await client.vol_from_margin(symbol, margin_usdt, leverage, entry_price)
            contract_size = await client.contract_size(symbol)
            actual_margin = (await client.amount_from_contracts(symbol, vol)) * entry_price / max(leverage, 1)
        except Exception as e:
            if self._is_symbol_reject_error(e):
                self._remember_wave_open_skip(symbol, "volume_margin_reject", error=str(e)[:160])
                self._ignore_symbol(symbol, f"wave volume/margin reject: {str(e)[:160]}")
                return None
            raise
        if str(s.get("position_size_mode") or "balance_percent").lower() == "balance_percent" and actual_margin > margin_usdt * 1.05:
            reason = f"min order too large for wave slot: desired_margin={margin_usdt:.4f}, min_actual_margin={actual_margin:.4f}"
            if "capped by available balance" in margin_note:
                self._remember_wave_open_skip(symbol, "no_margin", reason=reason, margin_note=margin_note)
                self._log_event("wave_free_margin_too_low", symbol=symbol, reason=reason, margin_note=margin_note)
                return None
            self._remember_wave_open_skip(symbol, "min_order_too_large", reason=reason)
            self._ignore_symbol(symbol, reason)
            return None

        sweep_levels = max(1, int(s.get("wave_entry_book_sweep_levels") or 5))
        liq_mult = max(1.0, float(s.get("wave_entry_liquidity_multiplier") or 1.0))
        max_sweep_ticks = max(0.0, float(s.get("wave_entry_max_sweep_ticks") or 3.0))
        levels = list(book["asks"] if direction == "long" else book["bids"])[:sweep_levels]
        need_contracts = float(vol) * liq_mult
        cum_contracts = 0.0
        chosen_price = 0.0
        chosen_level = 0
        for idx, lvl in enumerate(levels, start=1):
            try:
                px_i = float(lvl[0])
                qty_i = float(lvl[1])
            except Exception:
                continue
            if px_i <= 0 or qty_i <= 0:
                continue
            cum_contracts += qty_i
            if cum_contracts + 1e-9 >= need_contracts:
                chosen_price = px_i
                chosen_level = idx
                break
        if chosen_price <= 0:
            top_qty = float(levels[0][1]) if levels else 0.0
            self._remember_wave_open_skip(symbol, "top_liquidity_low", need_contracts=vol, seen_contracts=round(cum_contracts, 4), levels=sweep_levels, top_qty=top_qty)
            self._log_debug("wave_open_top_liquidity_reject", symbol=symbol, direction=direction, need_contracts=vol, seen_contracts=cum_contracts, levels=sweep_levels)
            return None
        best_price = ask if direction == "long" else bid
        sweep_ticks = abs(chosen_price - best_price) / max(tick, 1e-12)
        if sweep_ticks > max_sweep_ticks + 1e-9:
            self._remember_wave_open_skip(symbol, "entry_sweep_too_wide", need_contracts=vol, seen_contracts=round(cum_contracts, 4), sweep_ticks=round(sweep_ticks, 4), max_sweep_ticks=max_sweep_ticks, level=chosen_level)
            self._log_debug("wave_open_sweep_reject", symbol=symbol, direction=direction, need_contracts=vol, seen_contracts=cum_contracts, sweep_ticks=sweep_ticks, max_sweep_ticks=max_sweep_ticks, chosen_price=chosen_price, best_price=best_price)
            return None
        entry_price = chosen_price
        entry_liquidity_note = {
            "level": chosen_level,
            "need_contracts": vol,
            "available_contracts": round(cum_contracts, 4),
            "sweep_ticks": round(sweep_ticks, 4),
            "notional_usdt": round(float(vol) * contract_size * entry_price, 6),
        }

        post_only = bool(s.get("wave_entry_post_only", False))
        self._log_event("wave_entry_prepare", symbol=symbol, direction=direction, vol=vol, entry_price=entry_price, post_only=post_only, margin_note=margin_note, actual_margin=actual_margin, bid=bid, ask=ask, equity_before=equity_before, entry_liquidity=entry_liquidity_note)
        try:
            if post_only:
                # Safer maker entry, but can miss fast waves.
                px = bid if direction == "long" else ask
                order = await client.open_post_only(symbol, direction, vol, px, leverage, open_type)
            else:
                # Fast entry for wave mode: normal LIMIT to take existing
                # liquidity from the book, capped at entry_price. This is not
                # MARKET and not POST_ONLY: if the book runs away, it simply
                # does not fill and the code cancels/retries/top-ups.
                side_code = 1 if direction == "long" else 3
                px = await client.round_price(symbol, entry_price, "ceil" if direction == "long" else "floor")
                order = await client.place_order(symbol, side_code, 1, vol, px, leverage, open_type, external_oid=f"wave_open_{int(time.time()*1000)%10**10}")
        except Exception as e:
            if self._is_symbol_reject_error(e):
                self._remember_wave_open_skip(symbol, "symbol_reject", error=str(e)[:160])
                self._ignore_symbol(symbol, f"wave open reject: {str(e)[:160]}")
                return None
            self.stats.last_error = f"{symbol}: {self._friendly_error(e)}"
            self._remember_wave_open_skip(symbol, "order_error", error=self._friendly_error(e))
            self._log_error("wave_entry_order_error", e, symbol=symbol, direction=direction, vol=vol)
            return None
        oid = order.get("id")
        await asyncio.sleep(max(0.05, float(s.get("wave_entry_order_lifetime_ms") or s.get("order_lifetime_ms") or 450) / 1000.0))
        try:
            if oid:
                cancel_res = await client.cancel_order(oid, symbol)
                self._log_debug("wave_entry_cancel_after_lifetime", symbol=symbol, order_id=oid, result=cancel_res)
        except Exception as e:
            self._log_error("wave_entry_cancel_error", e, symbol=symbol, order_id=oid)
            try:
                await client.cancel_all_orders(symbol)
            except Exception as e2:
                self._log_error("wave_entry_cancel_all_error", e2, symbol=symbol)
        try:
            pos = await client.find_position(symbol, direction)
        except Exception as e:
            self.stats.last_error = f"{symbol}: {self._friendly_error(e)}"
            self._remember_wave_open_skip(symbol, "order_error", error=self._friendly_error(e), order_id=oid, stage="post_order_position_check")
            self._log_error("wave_entry_find_position_error", e, symbol=symbol, order_id=oid)
            return None
        if not pos:
            # MEXC position state can lag a filled aggressive LIMIT by a few hundred ms.
            # Recheck once before marking this slot empty, otherwise the retry loop can
            # open a duplicate on the same symbol.
            await asyncio.sleep(0.35)
            try:
                pos = await client.find_position(symbol, direction)
            except Exception as e:
                self.stats.last_error = f"{symbol}: {self._friendly_error(e)}"
                self._remember_wave_open_skip(symbol, "order_error", error=self._friendly_error(e), order_id=oid, stage="post_order_position_recheck")
                self._log_error("wave_entry_find_position_recheck_error", e, symbol=symbol, order_id=oid)
                return None
        if not pos:
            self._remember_wave_open_skip(symbol, "not_filled", order_id=oid)
            self._log_event("wave_entry_not_filled", symbol=symbol, order_id=oid)
            return None
        fee_bad, fee_info = self._position_has_nonzero_fee(pos)
        if fee_bad:
            aborted = await self._abort_invalid_fee_position(client, symbol, direction, pos, s, equity_before=equity_before, reason_info=fee_info)
            if aborted:
                self._remember_wave_open_skip(symbol, "invalid_fee_abort", fee_info=fee_info)
                return None
        self.stats.trade_timestamps.append(time.time())
        if symbol not in self.stats.open_position_symbols:
            self.stats.open_position_symbols.append(symbol)
        self._log_event("wave_entry_filled", symbol=symbol, direction=direction, position=pos)
        return pos

    async def _close_wave_positions(self, client: MexcFuturesClient, positions: list[dict[str, Any]], s: dict[str, Any], reason: str) -> dict[str, Any]:
        leverage = int(s.get("leverage") or 5)
        open_type = int(s.get("open_type") or 1)
        symbols = sorted({MexcFuturesClient.contract_id(p.get("symbol")) for p in positions if p.get("symbol")})
        results: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        self._log_event("wave_close_start", reason=reason, symbols=symbols, positions=positions)
        for sym in symbols:
            try:
                results.append({"stage": "cancel_before", "symbol": sym, "result": await client.cancel_all_orders(sym)})
            except Exception as e:
                errors.append({"stage": "cancel_before", "symbol": sym, "error": str(e)[:220]})
        for p in positions:
            try:
                if str(s.get("wave_close_mode") or "market").lower() == "market":
                    results.append({"stage": "close_market", "symbol": p.get("symbol"), "result": await client.close_market(p, leverage, open_type)})
                else:
                    book = await self._depth(str(p.get("symbol")), limit=5)
                    if not book.get("bids") or not book.get("asks"):
                        results.append({"stage": "close_market_fallback", "symbol": p.get("symbol"), "result": await client.close_market(p, leverage, open_type)})
                    else:
                        px = float(book["bids"][0][0]) if p.get("side") == "long" else float(book["asks"][0][0])
                        results.append({"stage": "close_limit_fast", "symbol": p.get("symbol"), "result": await client.close_limit(str(p.get("symbol")), str(p.get("side")), int(round(float(p.get("contracts") or 0))), px, leverage, open_type, post_only=False)})
            except Exception as e:
                # If MEXC says nonexistent/closed, it is not fatal for a basket close.
                if "2009" in str(e) or "nonexistent" in str(e).lower() or "closed" in str(e).lower():
                    results.append({"stage": "already_closed", "symbol": p.get("symbol"), "note": str(e)[:180]})
                else:
                    errors.append({"stage": "close", "symbol": p.get("symbol"), "error": str(e)[:260]})
        await asyncio.sleep(0.4)
        for sym in symbols:
            try:
                results.append({"stage": "cancel_after", "symbol": sym, "result": await client.cancel_all_orders(sym)})
            except Exception as e:
                errors.append({"stage": "cancel_after", "symbol": sym, "error": str(e)[:220]})
        res = {"ok": not errors, "results": results, "errors": errors}
        self._log_event("wave_close_done", result=res)
        return res

    async def _wave_basket_cycle(self, side: str, rows: list[dict[str, Any]], signal: dict[str, Any] | None = None) -> None:
        client = await self._ensure_client()
        s0 = self._settings()
        signal = signal or {}
        # Per-cycle settings: normal/early = 5x + $0.05, tsunami = 10x + $0.10.
        s = dict(s0)
        if signal.get("cycle_leverage"):
            s["leverage"] = int(signal.get("cycle_leverage"))
        if signal.get("cycle_target"):
            s["wave_target_profit_usdt"] = float(signal.get("cycle_target"))
        symbols = [MexcFuturesClient.contract_id(r.get("symbol")) for r in rows if r.get("symbol")]
        target = max(0.0001, float(s.get("wave_target_profit_usdt") or 0.05))
        min_take = max(0.0, float(s.get("wave_min_take_profit_usdt") or 0.03))
        giveback = max(0.0, float(s.get("wave_trailing_giveback_usdt") or 0.02))
        be_after = max(0.0, float(s.get("wave_break_even_after_sec") or 600.0))
        be_profit = max(0.0, float(s.get("wave_breakeven_profit_usdt") or 0.001))
        max_hold = max(be_after, float(s.get("wave_max_hold_sec") or 900.0))
        max_loss_exit = max(0.0, float(s.get("wave_max_loss_exit_usdt") or 0.0))
        min_filled = max(1, int(s.get("wave_min_filled_positions") or 3))
        equity_before = await self._read_usdt_total(client) if bool(s.get("real_pnl_enabled", True)) else None
        opened: list[dict[str, Any]] = []
        started = time.time()
        self._log_event("wave_cycle_start", side=side, symbols=symbols, target=target, leverage=s.get("leverage"), signal=signal, equity_before=equity_before)
        wave_slots = int(s.get("wave_positions") or 5)
        order_symbols = symbols[:wave_slots]
        # v0064: the first 5 picks can become stale while the controlled burst is
        # opening. If a slot is skipped because the coin flipped side, spread widened,
        # or the order did not fill, immediately top up from a fresh same-side scan
        # instead of accepting a 2/5 basket. MEXC throttle/rate-limit errors
        # wait and retry the same symbol; hard rejects are replaced by top-up scans.
        gap = max(0.05, float(s.get("wave_open_batch_gap_ms") or 420) / 1000.0)
        retry_delay = max(0.2, float(s.get("wave_open_retry_delay_sec") or 1.25))
        retry_rounds = max(0, int(s.get("wave_open_retry_rounds") or 2))
        topup_enabled = bool(s.get("wave_fill_topup_enabled", True))
        topup_rounds = max(0, int(s.get("wave_fill_topup_rounds") or 3))
        max_attempts = max(wave_slots, int(wave_slots * max(1.0, float(s.get("wave_open_max_attempts_multiplier") or 3.0))))
        attempted_symbols: set[str] = set()
        skipped: list[dict[str, Any]] = []
        pending = list(order_symbols)
        topup_i = 0
        open_attempts = 0

        while self.running and len(opened) < wave_slots and open_attempts < max_attempts:
            if not pending:
                if not topup_enabled or topup_i >= topup_rounds:
                    break
                topup_i += 1
                try:
                    fresh_rows = await self._refresh_market_scan(s, force=False)
                except Exception as e:
                    self._log_error("wave_topup_scan_error", e, side=side, opened=[p.get("symbol") for p in opened])
                    fresh_rows = list(self.stats.last_scan_rows or [])
                blocked = {MexcFuturesClient.contract_id(x) for x in attempted_symbols if x} | {MexcFuturesClient.contract_id(p.get("symbol")) for p in opened if p.get("symbol")}
                replacement_rows = self._pick_wave_middle_rows(fresh_rows, side, max(1, wave_slots - len(opened)), s, blocked=blocked)
                replacements = [MexcFuturesClient.contract_id(r.get("symbol")) for r in replacement_rows if r.get("symbol")]
                if not replacements:
                    self._log_debug("wave_topup_no_replacements", side=side, opened=[p.get("symbol") for p in opened], attempted=list(attempted_symbols), skipped=skipped[-8:])
                    break
                pending = replacements
                self.stats.current_symbols = [p.get("symbol") for p in opened if p.get("symbol")] + pending[: max(0, wave_slots - len(opened))]
                self.stats.last_action = f"TOPUP {side.upper()} basket: need {wave_slots-len(opened)} more; trying {', '.join(pending[:5])}"
                self._log_event("wave_topup_candidates", side=side, round=topup_i, pending=pending, opened=[p.get("symbol") for p in opened], skipped=skipped[-8:])

            batch = list(pending)
            pending = []
            for round_i in range(retry_rounds + 1):
                if not self.running or not batch or len(opened) >= wave_slots or open_attempts >= max_attempts:
                    break
                next_retry: list[str] = []
                for sym in list(batch):
                    if not self.running or len(opened) >= wave_slots or open_attempts >= max_attempts:
                        break
                    open_attempts += 1
                    attempted_symbols.add(sym)
                    self.stats.wave_state.update({"open_target": wave_slots, "open_count": len(opened), "attempts": open_attempts})
                    self.stats.last_action = f"OPEN {side.upper()} basket: slot {len(opened)+1}/{wave_slots} {sym} | try {open_attempts}/{max_attempts}"
                    try:
                        pos = await self._open_wave_position(sym, side, s, equity_before)
                    except Exception as e:
                        self._remember_wave_open_skip(sym, "order_error", error=self._friendly_error(e))
                        self._log_error("wave_burst_open_error", e, symbol=sym, round=round_i)
                        pos = None
                    if pos:
                        opened.append(pos)
                    else:
                        last_skip = dict(self.stats.wave_state.get("last_open_skip") or {})
                        if str(last_skip.get("symbol") or "") != sym:
                            last_skip = {"symbol": sym, "reason": "not_filled"}
                        skipped.append(last_skip)
                        last_err = str(self.stats.last_error or "")
                        skip_err = str(last_skip.get("error") or "")
                        err_text = (last_err + " " + skip_err).lower()
                        transient_markers = (
                            "510", "429", "rate limit", "too frequent", "frequency",
                            "too many", "throttle", "timeout", "timed out", "temporarily",
                            "503", "504", "service unavailable",
                        )
                        transient = (
                            str(last_skip.get("reason") or "") == "order_error"
                            and any(x in err_text for x in transient_markers)
                        )
                        if transient and round_i < retry_rounds:
                            self._log_event("wave_open_throttle_retry_scheduled", symbol=sym, round=round_i + 1, retry_delay=retry_delay, error=(last_err or skip_err)[:220])
                            next_retry.append(sym)
                    self.stats.open_position_symbols = [p.get("symbol") for p in opened if p.get("symbol")]
                    self.stats.current_symbols = self.stats.open_position_symbols + [x for x in (batch + pending) if x not in self.stats.open_position_symbols][: max(0, wave_slots-len(self.stats.open_position_symbols))]
                    self.stats.wave_state.update({"open_count": len(opened), "open_skips": skipped[-12:], "attempts": open_attempts})
                    await asyncio.sleep(gap)
                batch = next_retry
                if batch and round_i < retry_rounds:
                    self.stats.last_action = f"OPEN {side.upper()} basket: retrying {len(batch)} rate-limited slots after {retry_delay:.1f}s"
                    await asyncio.sleep(retry_delay)
        if not opened:
            self.stats.last_action = "wave fired but no entries filled"
            self._log_event("wave_cycle_no_fills", side=side, symbols=symbols)
            return
        if len(opened) < min_filled:
            self.stats.last_action = f"PARTIAL {side.upper()} basket: opened {len(opened)}/{wave_slots}; waiting/managing only filled slots"
            self._log_event("wave_cycle_underfilled", filled=len(opened), min_filled=min_filled, target_slots=wave_slots, symbols=[p.get("symbol") for p in opened], skipped=skipped[-12:], attempts=open_attempts)
        elif len(opened) < wave_slots:
            self.stats.last_action = f"PARTIAL {side.upper()} basket: opened {len(opened)}/{wave_slots}; replaced stale slots but still not full"
            self._log_event("wave_cycle_partial", filled=len(opened), target_slots=wave_slots, symbols=[p.get("symbol") for p in opened], skipped=skipped[-12:], attempts=open_attempts)

        # v0064: if MEXC charged real fees despite the zero-fee universe, do not
        # instantly kill the position. Instead raise the basket NET target so the
        # cycle only closes after fees + a real profit buffer are covered.
        entry_fee_sum = sum(self._position_fee_usdt(p) for p in opened)
        if entry_fee_sum > 0:
            fee_mult = max(1.0, float(s.get("wave_fee_target_multiplier") or 2.4))
            fee_buffer = max(0.0, float(s.get("wave_fee_profit_buffer_usdt") or target))
            fee_target = entry_fee_sum * fee_mult + fee_buffer
            old_target = target
            target = max(target, fee_target)
            min_take = max(min_take, target * 0.60)
            self._log_event("wave_fee_adjusted_target", entry_fee_sum=entry_fee_sum, old_target=old_target, target=target, min_take=min_take, opened=[p.get("symbol") for p in opened])
        status_icon = "✅" if len(opened) >= wave_slots else "⚠️"
        skip_txt = self._format_wave_skips(skipped, limit=5)
        skip_part = f" | skip: {skip_txt}" if skip_txt else ""
        opened_txt = ", ".join([str(p.get("symbol")) for p in opened if p.get("symbol")]) or "-"
        self.stats.wave_state.update({"open_target": wave_slots, "open_count": len(opened), "open_skips": skipped[-12:], "attempts": open_attempts, "target": target})
        pct_txt = f"LONG {float(signal.get('long_pct') or self.stats.wave_state.get('long_pct') or 0):.0%} / SHORT {float(signal.get('short_pct') or self.stats.wave_state.get('short_pct') or 0):.0%} / NEUTRAL {float(signal.get('neutral_pct') or self.stats.wave_state.get('neutral_pct') or 0):.0%}"
        await self._notify(
            f"{status_icon} КОРЗИНА {side.upper()} {self._mode_title(signal.get('mode'))}\n"
            f"PRICE 10s: {pct_txt}\n"
            f"Цель: {wave_slots} | открыто: {len(opened)} | не добрал: {max(0, wave_slots-len(opened))}\n"
            f"Открыты: {opened_txt}\n"
            f"REAL NET TP +${target:.2f} | leverage {s.get('leverage')}x{skip_part}"
        )
        peak_net = -999.0
        reason = "running"
        close_sent = False
        while self.running:
            # Work only the symbols that belong to this basket.
            positions: list[dict[str, Any]] = []
            for sym in [p.get("symbol") for p in opened if p.get("symbol")]:
                try:
                    positions.extend(await client.fetch_positions(str(sym)))
                except Exception as e:
                    self._log_error("wave_fetch_position_error", e, symbol=sym)
            # Keep same-side positions from this basket; if the exchange returns none, it is closed.
            positions = [p for p in positions if p.get("side") == side]
            self.stats.open_position_symbols = [p.get("symbol") for p in positions if p.get("symbol")]
            if not positions:
                reason = "basket_positions_closed"
                break
            # Abort if an already-open position proves the contract is not fee-free.
            fee_bad_positions = []
            for ppos in positions:
                fee_bad, fee_info = self._position_has_nonzero_fee(ppos)
                if fee_bad and bool(s.get("abort_nonzero_fee_position", True)):
                    fee_bad_positions.append((ppos, fee_info))
            if fee_bad_positions:
                reason = "actual_fee_detected"
                self._log_event("wave_actual_fee_detected", items=[{"position": ppos, "fee_info": info} for ppos, info in fee_bad_positions])
                await self._close_wave_positions(client, positions, s, reason)
                close_sent = True
                break
            await self._refresh_equity_snapshot(client)
            net = float(self.stats.net_equity_pnl or 0.0)
            if equity_before is not None and self.stats.live_equity:
                net = float(self.stats.live_equity) - float(equity_before)
            peak_net = max(peak_net, net)
            elapsed = time.time() - started
            should_close = False
            if net >= target:
                reason = "target_net_profit"
                should_close = True
            elif peak_net >= min_take and giveback > 0 and net <= peak_net - giveback:
                reason = "trail_take_profit"
                should_close = True
            elif be_after > 0 and elapsed >= be_after and net >= be_profit:
                reason = "timeout_breakeven_profit"
                should_close = True
            elif max_loss_exit > 0 and elapsed >= max_hold and net <= -max_loss_exit:
                reason = "max_hold_loss_exit"
                should_close = True
            elif max_hold > 0 and elapsed >= max_hold and net >= 0:
                reason = "max_hold_nonnegative_exit"
                should_close = True
            slots = await self._build_wave_slots(client, opened, positions, side, wave_slots)
            self.stats.last_action = f"MANAGE {side.upper()} basket: open={len(positions)}/{wave_slots} net={net:.5f}/{target:.5f} peak={peak_net:.5f} t={elapsed:.0f}s"
            self.stats.wave_state.update({"side": side, "mode": signal.get("mode", self.stats.wave_state.get("mode", "wave")), "target": target, "open_target": wave_slots, "open_count": len(positions), "net": net, "peak": peak_net, "slots": slots})
            self._log_debug("wave_manage_tick", side=side, net=net, peak_net=peak_net, target=target, elapsed=elapsed, open_symbols=self.stats.open_position_symbols, slots=slots, reason=reason if should_close else "")
            if should_close:
                await self._close_wave_positions(client, positions, s, reason)
                close_sent = True
                break
            await asyncio.sleep(0.45)
        await asyncio.sleep(0.5)
        equity_after = await self._read_usdt_total(client) if bool(s.get("real_pnl_enabled", True)) else None
        pnl = 0.0
        if equity_before is not None and equity_after is not None:
            pnl = float(equity_after) - float(equity_before)
        # Count one wave cycle as one trade/cycle, not one per coin.
        self.stats.estimated_pnl += pnl
        self.stats.trades += 1
        win_min = max(0.0, float(s.get("real_win_min_usdt") or 0.0))
        is_win = pnl > win_min
        self._increment_total_trade_counters(pnl, is_win=is_win)
        if is_win:
            self.stats.wins += 1
            self.stats.consecutive_losses = 0
        else:
            self.stats.losses += 1
            self.stats.consecutive_losses += 1
        self.last_trade_closed_ts = time.time()
        self.wave_cooldown_until_ts = time.time() + max(0.0, float(s.get("wave_cooldown_after_cycle_sec") or 20.0))
        await self._refresh_equity_snapshot(client)
        self._log_event("wave_cycle_closed", side=side, reason=reason, close_sent=close_sent, opened=[p.get("symbol") for p in opened], pnl=pnl, equity_before=equity_before, equity_after=equity_after, is_win=is_win)
        self.stats.last_action = f"wave {side} closed {reason}, pnl={pnl:.6f} USDT"
        self.stats.open_position_symbols.clear()
        await self._notify(
            f"🏁 КОРЗИНА {side.upper()} ЗАКРЫТА\n"
            f"Причина: {reason}\n"
            f"REAL PnL: {pnl:+.6f} USDT\n"
            f"Пауза перед повтором: {float(s.get('wave_cooldown_after_cycle_sec') or 20):.0f}s"
        )

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
        self._record_mid_price(symbol, bid, ask, tick)
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
                self._record_mid_price(symbol, bid2, ask2, tick)
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
        fee_bad, fee_info = self._position_has_nonzero_fee(pos)
        if fee_bad:
            aborted = await self._abort_invalid_fee_position(client, symbol, direction, pos, s, equity_before=equity_before, reason_info=fee_info)
            if aborted:
                return
        self.stats.trade_timestamps.append(time.time())
        if symbol not in self.stats.open_position_symbols:
            self.stats.open_position_symbols.append(symbol)
        self._log_event("entry_filled", symbol=symbol, direction=direction, position=pos)
        await self._notify(f"✅ FILLED {symbol} {direction.upper()} contracts={pos.get('contracts')} entry={pos.get('entryPrice') or entry_price}")
        await self._manage_position(symbol, direction, pos, s, equity_before=equity_before)

    async def _manage_basket_position(self, symbol: str, direction: str, pos: dict[str, Any], s: dict[str, Any], equity_before: float | None = None) -> None:
        """v0064 Wave Price Tsunami Basket manager.

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
        breakeven_after = max(0.0, float(s.get("basket_break_even_after_sec") or 0.0))
        breakeven_profit = max(0.0, float(s.get("basket_breakeven_profit_usdt") or 0.0005))
        breakeven_band = max(0.0, float(s.get("basket_breakeven_band_usdt") or 0.0))
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
            breakeven_after=breakeven_after,
            breakeven_profit=breakeven_profit,
            breakeven_band=breakeven_band,
            equity_before=equity_before,
            stop="OFF",
        )
        while self.running:
            current = await client.find_position(symbol, direction)
            if not current:
                reason = "basket_target_closed"
                self._log_event("basket_position_closed", symbol=symbol, direction=direction)
                break
            fee_bad, fee_info = self._position_has_nonzero_fee(current)
            if fee_bad and bool(s.get("abort_nonzero_fee_position", True)):
                aborted = await self._abort_invalid_fee_position(client, symbol, direction, current, s, equity_before=equity_before, reason_info=fee_info)
                if aborted:
                    reason = "invalid_fee_aborted"
                    break
            book = await self._depth(symbol, limit=5)
            if not book["bids"] or not book["asks"]:
                self._log_debug("basket_no_book", symbol=symbol, direction=direction)
                await asyncio.sleep(0.2)
                continue
            bid, ask = book["bids"][0][0], book["asks"][0][0]
            exit_price_est = bid if direction == "long" else ask
            proxy_pnl = (exit_price_est - entry) * amount if direction == "long" else (entry - exit_price_est) * amount
            maker_pnl = (ask - entry) * amount if direction == "long" else (entry - bid) * amount
            elapsed = time.time() - started
            rotation_mode = bool(breakeven_after > 0 and elapsed >= breakeven_after)
            active_target_usdt = breakeven_profit if rotation_mode else target_usdt
            active_min_proxy = max(active_target_usdt, breakeven_profit if rotation_mode else min_proxy)
            active_target_ticks = max(1, int(math.ceil(active_target_usdt / max(tick_value, 1e-12))))
            target_price = entry + active_target_ticks * tick if direction == "long" else entry - active_target_ticks * tick

            # v0064 rotation: after the stale timeout, stop waiting for +$0.01.
            # The close order is downgraded to breakeven/small-profit so the slot can
            # rotate into a better coin. This is not a stop; it does not cross a loss.
            if direction == "long":
                close_px = max(ask, target_price) if proxy_pnl >= active_min_proxy else target_price
            else:
                close_px = min(bid, target_price) if proxy_pnl >= active_min_proxy else target_price
            if rotation_mode and maker_pnl >= max(0.0, breakeven_profit - breakeven_band):
                # Work a maker/limit exit around the best quote when that maker quote
                # is already breakeven/small-profit. This rotates dead slots without
                # intentionally crossing a loss.
                close_px = ask if direction == "long" else bid

            now = time.time()
            requote_s = max(0.05, float(s.get("basket_close_requote_ms") or s.get("requote_interval_ms") or 200) / 1000.0)
            px_changed = close_order_px is None or abs(float(close_px) - float(close_order_px)) >= max(tick * 0.5, 1e-12)
            should_requote = (not close_order_id) or px_changed or (now - close_order_ts >= requote_s and (proxy_pnl >= active_min_proxy or rotation_mode))
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
                self.stats.last_action = f"{symbol}: basket {'BE' if rotation_mode else 'TP'} target=${active_target_usdt:.4f}, proxy={proxy_pnl:.5f}, close_px={close_px}"
                self._log_event("basket_close_submitted", symbol=symbol, direction=direction, contracts=contracts, close_px=close_px, target_price=target_price, target_ticks=active_target_ticks, target_usdt=active_target_usdt, original_target_usdt=target_usdt, rotation_mode=rotation_mode, elapsed=elapsed, proxy_pnl=proxy_pnl, maker_pnl=maker_pnl, order=order)
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
        await self._refresh_equity_snapshot(client)
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
        await self._refresh_equity_snapshot(client)
        await self._notify(f"🏁 CLOSED {symbol} {direction.upper()} reason={reason} pnl={pnl:.6f} USDT ({pnl_source})")
