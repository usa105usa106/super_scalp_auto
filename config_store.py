from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

DEFAULTS: dict[str, Any] = {
    "bot_version": "v0023",

    # secrets are set from Telegram with /api set KEY SECRET. Telegram token stays in ENV.
    "mexc_api_key": "",
    "mexc_api_secret": "",

    # MEXC connection/runtime defaults. Coolify only needs TELEGRAM_BOT_TOKEN and ADMIN_IDS.
    # These values are built in and can be changed from Telegram with /set.
    "mexc_rest_base": "https://api.mexc.com",
    "mexc_recv_window": 20000,
    "mexc_private_rate_limit": 8,
    "mexc_public_timeout": 6.0,
    "mexc_private_timeout": 15.0,
    "mexc_strict_leverage": False,
    "mexc_futures_ws": "wss://contract.mexc.com/edge",
    # v0018: MEXC order/create already carries leverage; changing leverage before every maker entry
    # causes code 2019 when orders exist and then code 510 rate-limit storms.
    "mexc_set_leverage_on_entry": False,

    # live trading core
    "live_enabled": False,
    "leverage": 5,
    "open_type": 1,  # 1 isolated, 2 cross on MEXC futures
    # one trade uses a percent of TOTAL USDT equity by default.
    "position_size_mode": "balance_percent",  # balance_percent | fixed_usdt
    "position_margin_percent": 10.0,
    "margin_per_position_usdt": 2.0,
    "max_positions": 1,
    "symbols_limit": 1,

    # micro-maker behavior
    "target_ticks": 1,
    "stop_ticks": 1,
    "order_lifetime_ms": 550,
    "requote_interval_ms": 200,
    "cycle_sleep_ms": 100,
    "max_position_lifetime_sec": 18,
    "post_only_entry": True,
    "post_only_close": True,
    "emergency_market_close": True,
    "direction_mode": "both",  # both | long | short

    # dynamic market scanner / symbol selection
    "auto_select_symbols": True,
    # Empty allowed_symbols = full-auto universe from API-confirmed zero-fee pairs.
    # If you set /symbols LINK_USDT,SOL_USDT then the scanner trades only that whitelist.
    "allowed_symbols": "",
    "only_zero_fee": True,
    "allow_manual_fee_fallback": False,
    "max_zero_fee_scan_symbols": 100,
    "scan_interval_sec": 1.0,
    "zero_fee_rescan_sec": 60.0,
    # 0 = do not cap universe; active WS/scoring window is max_zero_fee_scan_symbols/ws_depth_max_symbols.
    "zero_fee_universe_max_symbols": 0,
    "switch_score_improvement_pct": 5.0,
    "min_symbol_hold_sec": 5.0,
    "min_spread_ticks": 1,
    "max_spread_ticks": 2,
    # absolute minimum depth on EACH side of the top book levels.
    # v0023 lowers v0022's 100 USDT because it blocked all trades on the current market.
    "min_depth_usdt": 50.0,
    # dynamic minimum: position notional * this multiplier must fit on EACH side
    "min_depth_multiplier": 3.0,
    "min_24h_volume_usdt": 0.0,
    "min_imbalance_ratio": 1.20,
    "score_top_levels": 5,
    # v0023 active-plus profile: more trades than v0022, but still filters thin/flickering books.
    "min_trade_score": 25.0,
    "entry_recheck_ms": 120,
    "entry_recheck_required": True,
    "entry_recheck_count": 1,
    "cooldown_after_loss_sec": 20,
    "cooldown_after_trade_sec": 1,
    "emergency_market_close_on_time_stop": False,
    "max_position_hard_lifetime_sec": 45,
    "telegram_time_offset_hours": 3.0,
    "trade_profile": "active_plus_v0023",
    # Persistently ignored symbols: regional restrictions, min/max margin/volume rejects, unsupported contracts.
    "ignored_symbols": {},
    "max_ignored_symbols": 1000,

    # Fast market data. REST is used only as fallback/warmup; normal scanner/trade cycles use WS depth cache.
    "market_data_mode": "websocket",  # websocket | rest
    "ws_depth_enabled": True,
    "ws_depth_max_symbols": 100,
    "ws_book_stale_ms": 700,
    "ws_warmup_ms": 350,
    "rest_depth_fallback": True,
    # Scanner REST fallback in WS mode. 0 = safest/fastest: scan only local WS cache,
    # avoiding REST storms when many books are stale/missing. Trade cycle can still
    # use REST fallback for the currently traded symbol.
    "ws_scan_rest_fallback_limit": 0,

    # risk guard
    "daily_loss_limit_usdt": 0.8,
    "max_consecutive_losses": 5,
    "max_trades_per_hour": 120,
    "stop_on_api_errors": 8,

    # Persistent counters. They are updated after every closed trade and survive bot restarts.
    "total_trades_count": 0,
    "total_wins_count": 0,
    "total_losses_count": 0,
    "total_estimated_pnl_usdt": 0.0,

    # Telegram live panel: one message is edited instead of chat spam.
    "telegram_live_panel": True,
    "telegram_live_update_sec": 5.0,
    "telegram_live_fast_update_sec": 2.0,
    "telegram_live_stopped_update_sec": 0.0,
    "telegram_delete_command_messages": True,
    "telegram_delete_api_messages": False,
    "telegram_panel_chat_id": 0,
    "telegram_panel_message_id": 0,
    "telegram_panel_mode": "main",  # main | settings | symbols | api

    # Telegram ordinary command keyboard / menu.
    "telegram_reply_keyboard": True,
    "telegram_reply_keyboard_delete_hint": True,
    # Full debug log. /log_full exports logs/log_full.txt as a Telegram .txt document.
    "full_log_enabled": True,
    "full_log_scan_details": False,
    "full_log_scan_symbol_limit": 30,
    "full_log_retention_minutes": 20.0,
    "full_log_export_max_mb": 8.0,
}


ACTIVE_PLUS_PROFILE_V0023: dict[str, Any] = {
    # Active micro-maker mode: many more attempts than v0022.
    # Goal: collect one-tick maker exits while avoiding the worst thin/noisy books.
    # Profit is not guaranteed; this profile is tuned to trade, not to sit idle.
    "leverage": 5,
    "position_margin_percent": 10.0,
    "max_positions": 1,
    "symbols_limit": 1,
    "max_zero_fee_scan_symbols": 100,
    "ws_depth_max_symbols": 100,
    "target_ticks": 1,
    "stop_ticks": 1,
    "order_lifetime_ms": 550,
    "requote_interval_ms": 200,
    "max_position_lifetime_sec": 18,
    "min_depth_usdt": 50.0,
    "min_depth_multiplier": 3.0,
    "min_spread_ticks": 1,
    "max_spread_ticks": 2,
    "min_imbalance_ratio": 1.20,
    "min_trade_score": 25.0,
    "entry_recheck_ms": 120,
    "entry_recheck_required": True,
    "entry_recheck_count": 1,
    "cooldown_after_loss_sec": 20,
    "cooldown_after_trade_sec": 1,
    "emergency_market_close_on_time_stop": False,
    "max_position_hard_lifetime_sec": 45,
    "telegram_time_offset_hours": 3.0,
    "max_trades_per_hour": 120,
    "max_consecutive_losses": 5,
    "daily_loss_limit_usdt": 0.8,
    "switch_score_improvement_pct": 5.0,
    "min_symbol_hold_sec": 5.0,
    "mexc_private_rate_limit": 8,
    "mexc_strict_leverage": False,
    "mexc_set_leverage_on_entry": False,
    "trade_profile": "active_plus_v0023",
}



SENSITIVE = {"mexc_api_key", "mexc_api_secret"}


def mask_secret(value: str) -> str:
    value = str(value or "")
    if not value:
        return "missing"
    if len(value) <= 8:
        return "saved"
    return f"{value[:4]}...{value[-4:]}"


class ConfigStore:
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path or os.getenv("MICRO_MAKER_SETTINGS", "runtime_settings.json"))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.save(dict(DEFAULTS))
        self._clear_ignored_on_start()

    def _clear_ignored_on_start(self) -> None:
        """Ignore cache is session-only. On restart/redeploy we clear it so old
        low-margin/API-error rejects do not shrink the scanner universe forever.
        """
        try:
            data = json.loads(self.path.read_text(encoding="utf-8")) if self.path.exists() else {}
            if isinstance(data, dict) and data.get("ignored_symbols"):
                data["ignored_symbols"] = {}
                tmp = self.path.with_suffix(self.path.suffix + ".tmp")
                tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                tmp.replace(self.path)
        except Exception:
            pass

    def load(self) -> dict[str, Any]:
        data = {}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}
        out = dict(DEFAULTS)
        out.update(data)

        # v0017 migration: v0015/v0016 shipped with min_depth_usdt=5000.
        # That blocks all micro-maker candidates on small accounts. If the stored
        # value is exactly the old default, migrate it to the new micro default.
        try:
            old_ver = str(data.get("bot_version") or "")
            if old_ver != DEFAULTS["bot_version"] and float(data.get("min_depth_usdt", 5000.0)) == 5000.0:
                out["min_depth_usdt"] = DEFAULTS["min_depth_usdt"]
        except Exception:
            pass

        # v0018 migration: do not change leverage before every order. On MEXC this
        # fails while maker orders are open (code 2019) and quickly creates code 510
        # rate-limit storms. Keep leverage in the create-order payload instead.
        try:
            old_ver = str(data.get("bot_version") or "")
            if old_ver != DEFAULTS["bot_version"]:
                if "mexc_set_leverage_on_entry" not in data:
                    out["mexc_set_leverage_on_entry"] = DEFAULTS["mexc_set_leverage_on_entry"]
                if bool(data.get("mexc_strict_leverage", True)) is True:
                    out["mexc_strict_leverage"] = DEFAULTS["mexc_strict_leverage"]
                if int(float(data.get("mexc_private_rate_limit", 18))) > DEFAULTS["mexc_private_rate_limit"]:
                    out["mexc_private_rate_limit"] = DEFAULTS["mexc_private_rate_limit"]
        except Exception:
            pass
        # v0023 migration: user requested more trades after v0022 was too strict.
        # Apply only strategy/risk filters; never touch API keys, counters, panel ids, or ignored symbols.
        try:
            old_ver = str(data.get("bot_version") or "")
            if old_ver != DEFAULTS["bot_version"] and str(data.get("trade_profile") or "") != "custom":
                out.update(ACTIVE_PLUS_PROFILE_V0023)
                # Do not carry bad temporary ignores caused by low free margin / old bugs.
                out["ignored_symbols"] = {}
        except Exception:
            pass

        out["bot_version"] = DEFAULTS["bot_version"]
        return out

    def save(self, data: dict[str, Any]) -> None:
        merged = dict(DEFAULTS)
        merged.update(data or {})
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def set(self, key: str, value: Any) -> dict[str, Any]:
        data = self.load()
        if key not in DEFAULTS and key not in SENSITIVE:
            raise KeyError(f"unknown setting: {key}")
        data[key] = value
        self.save(data)
        return data

    def update(self, values: dict[str, Any]) -> dict[str, Any]:
        data = self.load()
        for key, value in (values or {}).items():
            if key not in DEFAULTS and key not in SENSITIVE:
                raise KeyError(f"unknown setting: {key}")
            data[key] = value
        self.save(data)
        return data

    @staticmethod
    def public_view(data: dict[str, Any]) -> dict[str, Any]:
        out = dict(data or {})
        for k in SENSITIVE:
            out[k] = mask_secret(str(out.get(k) or ""))
        return out


def parse_symbols(raw: str) -> list[str]:
    out: list[str] = []
    for item in str(raw or "").replace(";", ",").split(","):
        s = item.strip().upper().replace("-", "_").replace("/", "_").replace(":USDT", "")
        if not s:
            continue
        if "_" not in s and s.endswith("USDT"):
            s = s[:-4] + "_USDT"
        if "_" not in s:
            s = s + "_USDT"
        if s not in out:
            out.append(s)
    return out
