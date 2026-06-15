from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

DEFAULTS: dict[str, Any] = {
    "bot_version": "v0076",

    # secrets are set from Telegram with /api set KEY SECRET. Telegram token stays in ENV.
    "mexc_api_key": "",
    "mexc_api_secret": "",

    # MEXC connection/runtime defaults. Coolify only needs TELEGRAM_BOT_TOKEN and ADMIN_IDS.
    # These values are built in and can be changed from Telegram with /set.
    "mexc_rest_base": "https://api.mexc.com",
    "mexc_recv_window": 20000,
    "mexc_private_rate_limit": 3,
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
    "position_margin_percent": 20.0,
    "margin_per_position_usdt": 2.0,
    "max_positions": 5,
    "symbols_limit": 5,

    # micro-maker behavior
    # v0025: base ticks are kept small, but real fee/zero-fee guard can lift
    # target_ticks dynamically when MEXC charges actual fees.
    "target_ticks": 0,
    "stop_ticks": 0,
    "order_lifetime_ms": 550,
    "requote_interval_ms": 200,
    "cycle_sleep_ms": 100,
    "max_position_lifetime_sec": 0,
    "post_only_entry": True,
    "post_only_close": True,
    "emergency_market_close": True,
    "direction_mode": "both",  # both | long | short

    # dynamic market scanner / symbol selection
    "auto_select_symbols": True,
    # Empty allowed_symbols = full-auto universe from API-confirmed zero-fee pairs.
    # If you set /symbols LINK_USDT,SOL_USDT then the scanner trades only that whitelist.
    "allowed_symbols": "",
    # Basket mode trades only contracts settled in this quote currency.
    # This prevents SOL_USDC/BTC_USDC attempts when the account has only USDT collateral.
    "contract_quote_filter": "USDT",
    "only_zero_fee": True,
    "allow_manual_fee_fallback": False,
    "max_zero_fee_scan_symbols": 0,
    "scan_interval_sec": 1.0,
    "zero_fee_rescan_sec": 60.0,
    # 0 = ALL: do not cap the zero-fee universe, active price scan, or WS subscriptions.
    "zero_fee_universe_max_symbols": 0,
    "switch_score_improvement_pct": 5.0,
    "min_symbol_hold_sec": 5.0,
    "min_spread_ticks": 1,
    "max_spread_ticks": 2,
    # absolute minimum depth on EACH side of the top book levels.
    # v0025 keeps v0023's 100 USDT because it blocked all trades on the current market.
    "min_depth_usdt": 35.0,
    # dynamic minimum: position notional * this multiplier must fit on EACH side
    "min_depth_multiplier": 2.0,
    "min_24h_volume_usdt": 0.0,
    "min_imbalance_ratio": 1.03,
    "score_top_levels": 5,
    # v0025 real-profit profile: active scanner with real-balance PnL and fee-aware targets.
    "min_trade_score": 0.0,
    "entry_recheck_ms": 120,
    "entry_recheck_required": True,
    "entry_recheck_count": 1,
    "cooldown_after_loss_sec": 0,
    "cooldown_after_trade_sec": 1,
    "emergency_market_close_on_time_stop": False,
    "max_position_hard_lifetime_sec": 0,
    "telegram_time_offset_hours": 3.0,
    # v0025 real-profit accounting. If MEXC charges fees, one tick can show
    # green by price movement while real balance falls. These settings make
    # the panel/counters use real equity delta and automatically require enough
    # ticks to cover entry+exit fees.
    "real_pnl_enabled": True,
    "fee_aware_target": True,
    "min_net_profit_usdt": 0.004,
    "max_fee_target_ticks": 10,
    "ignore_symbol_after_real_loss": False,

    # v0025: hard pre-trade fee gate. Dedicated zero-fee endpoint alone was not
    # enough on live SOL: the position reported real fees and balance fell.
    # Before every real entry we also query contract fee_rate for that exact symbol.
    # If maker/taker fees are non-zero, the symbol is skipped/ignored for the session.
    "require_contract_zero_fee_on_entry": True,
    "max_entry_maker_fee_rate": 0.0,
    "max_entry_taker_fee_rate": 0.0,
    "fee_guard_ignore_symbol": True,
    "trade_profile": "wave_price_tsunami_v0076",
    "edge_filter_enabled": False,
    "entry_top_imbalance_ratio": 1.15,
    "entry_microprice_min_ticks": 0.10,
    "entry_no_adverse_move_ticks": 0.0,
    "ban_symbol_after_real_loss": False,
    "min_gross_profit_usdt": 0.004,
    "real_win_min_usdt": 0.0005,

    # v0029 basket harvest: hold 3 live positions, no per-position stops,
    # close only when real/proxy profit target is reached, then immediately refill.
    "basket_harvest_enabled": True,
    "basket_positions": 5,
    "basket_target_profit_usdt": 0.05,
    "basket_min_proxy_profit_usdt": 0.0505,
    "basket_random_top_n": 12,
    "basket_semi_random": False,
    "basket_close_requote_ms": 200,
    # v0031 rebound/rotation mode: do not wait hours for +$0.01.
    # After timeout, downgrade target to breakeven and rotate the slot when near zero.
    "basket_rebound_enabled": True,
    "basket_rebound_lookback_sec": 25.0,
    "basket_rebound_min_move_ticks": 3.0,
    "basket_rebound_confirm_top_ratio": 1.15,
    "basket_rebound_confirm_micro_ticks": 0.05,
    "basket_break_even_after_sec": 600.0,
    "basket_breakeven_profit_usdt": 0.0005,
    "basket_breakeven_band_usdt": 0.0,
    "basket_force_breakeven_rotation": True,
    # v0030 truth mode: panel/counters must not show green while open positions drag equity down.
    "abort_nonzero_fee_position": False,
    "emergency_market_close_invalid_fee": False,
    "panel_show_net_equity_pnl": True,

    # v0032 Wave Hunter: sit in ambush, detect a short market-wide impulse,
    # then open the whole basket in one direction and close the whole basket on NET equity profit.
    "wave_basket_enabled": True,
    "wave_positions": 5,
    "wave_margin_percent_test": 20.0,
    "wave_margin_percent_large": 10.0,
    "wave_lookback_sec": 10.0,
    "wave_min_move_ticks": 0.0,
    "wave_min_side_ratio": 0.75,
    "wave_min_candidates": 5,
    "wave_entry_confirmations": 1,
    "wave_entry_top_ratio": 1.05,
    "wave_entry_micro_ticks": 0.0,
    "wave_target_profit_usdt": 0.05,
    "wave_min_take_profit_usdt": 0.03,
    "wave_trailing_giveback_usdt": 0.03,
    "wave_break_even_after_sec": 600.0,
    "wave_breakeven_profit_usdt": 0.0,
    "wave_max_hold_sec": 600.0,
    "wave_max_loss_exit_usdt": 0.0,
    "wave_close_mode": "market",
    "wave_entry_post_only": False,
    "wave_entry_order_lifetime_ms": 450,
    # v0076: aggressive entry must not wait in queue. Pick an existing book
    # level with enough cumulative liquidity, place a normal LIMIT there,
    # wait briefly, then cancel leftovers.
    "wave_entry_book_sweep_levels": 5,
    "wave_entry_liquidity_multiplier": 1.0,
    "wave_entry_max_sweep_ticks": 3.0,
    "wave_min_filled_positions": 3,
    "wave_cooldown_after_cycle_sec": 20.0,
    "wave_require_leader_confirmation": False,
    "wave_leader_symbols": "BTC_USDT,SOL_USDT,ETH_USDT",
    "wave_min_leader_confirm": 1,
    "wave_max_opposite_leaders": 0,
    "wave_leader_min_move_ticks": 3.0,
    "wave_quality_score_required": 2,
    "wave_fee_target_multiplier": 2.4,
    "wave_fee_profit_buffer_usdt": 0.05,
    "wave_parallel_open": False,
    "wave_open_batch_gap_ms": 1000,
    "wave_open_retry_delay_sec": 2.0,
    "wave_open_retry_rounds": 4,
    "wave_fill_topup_enabled": True,
    "wave_fill_topup_rounds": 5,
    "wave_open_max_attempts_multiplier": 5.0,

    # v0076 Price Tsunami mode: global 10s price vote, clean early/normal/tsunami rules.
    # Count how many ALL active zero-fee coins rose/fell over 10 seconds, then use 60-second
    # dominance growth to catch an early market wave.
    "wave_price_vote_enabled": True,
    # v0076: market signal mode switch. all_zero_total keeps the current
    # full zero-fee universe vote. top10_leaders uses the 10 most liquid
    # non-stable zero-fee USDT leaders for direction; trade entries still
    # use the full zero-fee universe. TOP10 rules: 7/10 NORMAL,
    # 7/10 + +2 leaders over 60s EARLY, 8/10 TSUNAMI.
    "wave_market_signal_mode": "all_zero_total",  # all_zero_total | top10_leaders
    "wave_top10_leader_count": 10,
    "wave_top10_normal_count": 7,
    "wave_top10_tsunami_count": 8,
    "wave_top10_accel_count": 2,
    "wave_top10_tsunami_requires_accel": False,
    "wave_top10_excluded_symbols": "USDT_USDT,USDC_USDT,USDE_USDT,USD1_USDT,DAI_USDT,FDUSD_USDT,TUSD_USDT,PYUSD_USDT,USDD_USDT,EURC_USDT",
    "wave_price_lookback_sec": 10.0,
    "wave_price_min_move_pct": 0.015,
    "wave_accel_lookback_sec": 60.0,
    "wave_accel_trigger_pct": 15.0,
    # v0076: signal must HOLD, not just flash for one scan tick.
    # Example: Early requires current side >=65% AND +15p.p. growth.
    # Stable entry requires 4 of last 5 hold samples over about 10 seconds.
    "wave_signal_hold_checks": 5,
    "wave_signal_hold_required": 4,
    "wave_signal_hold_sec": 10.0,
    "wave_early_min_side_ratio": 0.65,
    "wave_accel_min_side_ratio": 0.65,
    "wave_normal_target_profit_usdt": 0.05,
    "wave_tsunami_target_profit_usdt": 0.10,
    "wave_normal_leverage": 5,
    "wave_tsunami_leverage": 10,
    "wave_pick_start_pct": 0.25,
    "wave_pick_end_pct": 0.60,

    # Persistently ignored symbols: regional restrictions, min/max margin/volume rejects, unsupported contracts.
    "ignored_symbols": {},
    "max_ignored_symbols": 1000,

    # Fast market data. REST is used only as fallback/warmup; normal scanner/trade cycles use WS depth cache.
    "market_data_mode": "websocket",  # websocket | rest
    "ws_depth_enabled": True,
    "ws_depth_max_symbols": 0,
    "ws_book_stale_ms": 1200,
    "ws_warmup_ms": 350,
    "rest_depth_fallback": True,
    # Scanner REST fallback in WS mode. 0 = safest/fastest: scan only local WS cache,
    # avoiding REST storms when many books are stale/missing. Trade cycle can still
    # use REST fallback for the currently traded symbol.
    "ws_scan_rest_fallback_limit": 0,

    # risk guard
    "daily_loss_limit_usdt": 999.0,
    "max_consecutive_losses": 999,
    "max_trades_per_hour": 120,
    "stop_on_api_errors": 999,
    # Runtime/private API throttles. Prevents balance/positions private requests
    # from running on every 100ms loop tick and freezing/rate-limiting the bot.
    "runtime_loop_tick_timeout_sec": 22.0,
    "private_balance_poll_sec": 12.0,
    "private_positions_poll_sec": 8.0,
    "private_manage_positions_poll_sec": 1.0,
    "private_manage_balance_poll_sec": 1.5,
    "position_margin_cache_sec": 5.0,

    # Persistent counters. They are updated after every closed trade and survive bot restarts.
    "total_trades_count": 0,
    "total_wins_count": 0,
    "total_losses_count": 0,
    "total_estimated_pnl_usdt": 0.0,

    # Telegram live panel: one message is edited instead of chat spam.
    "telegram_live_panel": True,
    "telegram_live_update_sec": 5.0,
    "telegram_live_fast_update_sec": 5.0,
    "telegram_live_stopped_update_sec": 0.0,
    "telegram_delete_command_messages": False,
    "telegram_delete_api_messages": False,
    "telegram_panel_chat_id": 0,
    "telegram_panel_message_id": 0,
    "telegram_panel_mode": "main",  # main | settings | symbols | api
    # v0076 clean panel lifecycle: edit one panel every 5s, rotate once per 10m.
    "telegram_panel_message_ids": [],
    "telegram_panel_created_ts": 0.0,
    "telegram_panel_cycle_sec": 600.0,
    "telegram_panel_refresh_mode": "edit_rotate",

    # Telegram ordinary command keyboard / menu.
    "telegram_reply_keyboard": False,
    "telegram_reply_keyboard_delete_hint": False,
    # Full debug log. /log_full exports logs/log_full.txt as a Telegram .txt document.
    "full_log_enabled": True,
    "full_log_scan_details": False,
    "full_log_scan_symbol_limit": 30,
    "full_log_retention_minutes": 20.0,
    "full_log_export_max_mb": 8.0,
}


WAVE_HUNTER_PROFILE_V0032: dict[str, Any] = {
    # Basket-harvest live mode: 3 small positions, no per-position stops,
    # close only on +$0.01 real/proxy profit and immediately refill the basket.
    "leverage": 5,
    "position_margin_percent": 20.0,
    "max_positions": 5,
    "symbols_limit": 5,
    "max_zero_fee_scan_symbols": 0,
    "ws_depth_max_symbols": 0,
    "target_ticks": 0,
    "stop_ticks": 0,
    "order_lifetime_ms": 550,
    "requote_interval_ms": 200,
    "max_position_lifetime_sec": 0,
    "min_depth_usdt": 35.0,
    "min_depth_multiplier": 2.0,
    "min_spread_ticks": 1,
    "max_spread_ticks": 2,
    "min_imbalance_ratio": 1.03,
    "min_trade_score": 0.0,
    "entry_recheck_ms": 120,
    "entry_recheck_required": True,
    "entry_recheck_count": 1,
    "cooldown_after_loss_sec": 0,
    "cooldown_after_trade_sec": 1,
    "emergency_market_close_on_time_stop": False,
    "max_position_hard_lifetime_sec": 0,
    "telegram_time_offset_hours": 3.0,
    "max_trades_per_hour": 120,
    "max_consecutive_losses": 999,
    "daily_loss_limit_usdt": 999.0,
    "switch_score_improvement_pct": 5.0,
    "min_symbol_hold_sec": 5.0,
    "mexc_private_rate_limit": 3,
    "mexc_strict_leverage": False,
    "mexc_set_leverage_on_entry": False,
    "real_pnl_enabled": True,
    "fee_aware_target": True,
    "min_net_profit_usdt": 0.004,
    "max_fee_target_ticks": 10,
    "ignore_symbol_after_real_loss": False,
    "require_contract_zero_fee_on_entry": True,
    "max_entry_maker_fee_rate": 0.0,
    "max_entry_taker_fee_rate": 0.0,
    "fee_guard_ignore_symbol": True,
    "trade_profile": "wave_price_tsunami_v0076",
    "edge_filter_enabled": False,
    "entry_top_imbalance_ratio": 1.15,
    "entry_microprice_min_ticks": 0.10,
    "entry_no_adverse_move_ticks": 0.0,
    "ban_symbol_after_real_loss": False,
    "min_gross_profit_usdt": 0.004,
    "real_win_min_usdt": 0.0005,
    "basket_harvest_enabled": True,
    "basket_positions": 5,
    "basket_target_profit_usdt": 0.05,
    "basket_min_proxy_profit_usdt": 0.0505,
    "basket_random_top_n": 12,
    "basket_semi_random": False,
    "basket_close_requote_ms": 200,
    # v0031 rebound/rotation mode: do not wait hours for +$0.01.
    # After timeout, downgrade target to breakeven and rotate the slot when near zero.
    "basket_rebound_enabled": True,
    "basket_rebound_lookback_sec": 25.0,
    "basket_rebound_min_move_ticks": 3.0,
    "basket_rebound_confirm_top_ratio": 1.15,
    "basket_rebound_confirm_micro_ticks": 0.05,
    "basket_break_even_after_sec": 600.0,
    "basket_breakeven_profit_usdt": 0.0005,
    "basket_breakeven_band_usdt": 0.0,
    "basket_force_breakeven_rotation": True,
    # v0030 truth mode: panel/counters must not show green while open positions drag equity down.
    "abort_nonzero_fee_position": False,
    "emergency_market_close_invalid_fee": False,
    "panel_show_net_equity_pnl": True,

    # v0032 Wave Hunter: sit in ambush, detect a short market-wide impulse,
    # then open the whole basket in one direction and close the whole basket on NET equity profit.
    "wave_basket_enabled": True,
    "wave_positions": 5,
    "wave_margin_percent_test": 20.0,
    "wave_margin_percent_large": 10.0,
    "wave_lookback_sec": 10.0,
    "wave_min_move_ticks": 0.0,
    "wave_min_side_ratio": 0.75,
    "wave_min_candidates": 5,
    "wave_entry_confirmations": 1,
    "wave_entry_top_ratio": 1.05,
    "wave_entry_micro_ticks": 0.0,
    "wave_target_profit_usdt": 0.05,
    "wave_min_take_profit_usdt": 0.03,
    "wave_trailing_giveback_usdt": 0.03,
    "wave_break_even_after_sec": 600.0,
    "wave_breakeven_profit_usdt": 0.0,
    "wave_max_hold_sec": 600.0,
    "wave_max_loss_exit_usdt": 0.0,
    "wave_close_mode": "market",
    "wave_entry_post_only": False,
    "wave_entry_order_lifetime_ms": 450,
    # v0076: aggressive entry must not wait in queue. Pick an existing book
    # level with enough cumulative liquidity, place a normal LIMIT there,
    # wait briefly, then cancel leftovers.
    "wave_entry_book_sweep_levels": 5,
    "wave_entry_liquidity_multiplier": 1.0,
    "wave_entry_max_sweep_ticks": 3.0,
    "wave_min_filled_positions": 3,
    "wave_cooldown_after_cycle_sec": 20.0,
    "wave_require_leader_confirmation": False,
    "wave_leader_symbols": "BTC_USDT,SOL_USDT,ETH_USDT",
    "wave_min_leader_confirm": 1,
    "wave_max_opposite_leaders": 0,
    "wave_leader_min_move_ticks": 3.0,
    "wave_quality_score_required": 2,
    "wave_fee_target_multiplier": 2.4,
    "wave_fee_profit_buffer_usdt": 0.05,
    "wave_parallel_open": False,
    "wave_open_batch_gap_ms": 1000,
    "wave_open_retry_delay_sec": 2.0,
    "wave_open_retry_rounds": 4,
    "wave_fill_topup_enabled": True,
    "wave_fill_topup_rounds": 5,
    "wave_open_max_attempts_multiplier": 5.0,

    # v0076 Price Tsunami mode: global 10s price vote, clean early/normal/tsunami rules.
    # Count how many ALL active zero-fee coins rose/fell over 10 seconds, then use 60-second
    # dominance growth to catch an early market wave.
    "wave_price_vote_enabled": True,
    # v0076: market signal mode switch. all_zero_total keeps the current
    # full zero-fee universe vote. top10_leaders uses the 10 most liquid
    # non-stable zero-fee USDT leaders for direction; trade entries still
    # use the full zero-fee universe. TOP10 rules: 7/10 NORMAL,
    # 7/10 + +2 leaders over 60s EARLY, 8/10 TSUNAMI.
    "wave_market_signal_mode": "all_zero_total",  # all_zero_total | top10_leaders
    "wave_top10_leader_count": 10,
    "wave_top10_normal_count": 7,
    "wave_top10_tsunami_count": 8,
    "wave_top10_accel_count": 2,
    "wave_top10_tsunami_requires_accel": False,
    "wave_top10_excluded_symbols": "USDT_USDT,USDC_USDT,USDE_USDT,USD1_USDT,DAI_USDT,FDUSD_USDT,TUSD_USDT,PYUSD_USDT,USDD_USDT,EURC_USDT",
    "wave_price_lookback_sec": 10.0,
    "wave_price_min_move_pct": 0.015,
    "wave_accel_lookback_sec": 60.0,
    "wave_accel_trigger_pct": 15.0,
    # v0076: signal must HOLD, not just flash for one scan tick.
    # Example: Early requires current side >=65% AND +15p.p. growth.
    # Stable entry requires 4 of last 5 hold samples over about 10 seconds.
    "wave_signal_hold_checks": 5,
    "wave_signal_hold_required": 4,
    "wave_signal_hold_sec": 10.0,
    "wave_early_min_side_ratio": 0.65,
    "wave_accel_min_side_ratio": 0.65,
    "wave_normal_target_profit_usdt": 0.05,
    "wave_tsunami_target_profit_usdt": 0.10,
    "wave_normal_leverage": 5,
    "wave_tsunami_leverage": 10,
    "wave_pick_start_pct": 0.25,
    "wave_pick_end_pct": 0.60,
}

# Backwards-compatible import name used by main.py.
ZERO_FEE_GUARD_PROFILE_V0025 = WAVE_HUNTER_PROFILE_V0032
ACTIVE_PLUS_PROFILE_V0023 = WAVE_HUNTER_PROFILE_V0032
EDGE_PLUS_PROFILE_V0027 = WAVE_HUNTER_PROFILE_V0032
BASKET_HARVEST_PROFILE_V0029 = WAVE_HUNTER_PROFILE_V0032
BASKET_REBOUND_PROFILE_V0031 = WAVE_HUNTER_PROFILE_V0032



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
        # v0025 migration: keep the active scanner, but switch accounting to real
        # balance delta and enable fee-aware targets. Never touch API keys, counters,
        # panel ids. Ignored cache is session-only and is cleared on start above.
        try:
            old_ver = str(data.get("bot_version") or "")
            if old_ver != DEFAULTS["bot_version"] and str(data.get("trade_profile") or "") != "custom":
                out.update(ZERO_FEE_GUARD_PROFILE_V0025)
                out["ignored_symbols"] = {}
        except Exception:
            pass

        # v0076 rollback safety: never inherit broken mirror-era Telegram behavior from old settings.
        out["telegram_delete_command_messages"] = False
        out["telegram_reply_keyboard"] = False
        out["telegram_reply_keyboard_delete_hint"] = False
        out["telegram_live_update_sec"] = 5.0
        out["telegram_live_fast_update_sec"] = 5.0
        out["telegram_panel_cycle_sec"] = 600.0
        out["telegram_panel_refresh_mode"] = "edit_rotate"
        out["stop_on_api_errors"] = 999
        # v0076 UI default: the live toggle starts from ALL total after upgrade.
        # The user can switch to TOP10 with one tap on the live panel.
        try:
            if str(data.get("bot_version") or "") != DEFAULTS["bot_version"] and str(data.get("trade_profile") or "") != "custom":
                out["wave_market_signal_mode"] = DEFAULTS["wave_market_signal_mode"]
        except Exception:
            out["wave_market_signal_mode"] = DEFAULTS["wave_market_signal_mode"]
        # v0076: v0070 stored 700ms, which was too strict for 144 WS books.
        # Raise old/lower values to the safer audited default, but keep higher manual values.
        try:
            if float(out.get("ws_book_stale_ms") or 0) < float(DEFAULTS["ws_book_stale_ms"]):
                out["ws_book_stale_ms"] = DEFAULTS["ws_book_stale_ms"]
        except Exception:
            out["ws_book_stale_ms"] = DEFAULTS["ws_book_stale_ms"]
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
