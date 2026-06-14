from __future__ import annotations

import asyncio
import os
import time
import json
try:
    import fcntl
except Exception:  # Windows/limited platforms
    fcntl = None
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from config_store import ConfigStore, DEFAULTS, ACTIVE_PLUS_PROFILE_V0023, mask_secret, parse_symbols
from micro_maker_engine import MicroMakerEngine
from mexc_client import MexcFuturesClient
from full_logger import export_full_log, clear_full_log, log_event, log_error

load_dotenv()

STORE = ConfigStore()
ENGINE: MicroMakerEngine | None = None
PANEL_LOCK = asyncio.Lock()
PANEL_UPDATE_TASK: asyncio.Task | None = None
PROCESS_START_TS = time.time()
UI_BG_TASKS: dict[str, asyncio.Task] = {}
TELEGRAM_IO_TIMEOUT_SEC = float(os.getenv("TELEGRAM_IO_TIMEOUT_SEC", "12"))
BOT_INSTANCE_ID = f"{os.getpid()}-{int(PROCESS_START_TS)}"


async def tg_wait(awaitable, timeout: float | None = None):
    """Bound every Telegram network call so one bad edit/send cannot freeze UI locks."""
    return await asyncio.wait_for(awaitable, timeout=max(3.0, float(timeout or TELEGRAM_IO_TIMEOUT_SEC)))


def spawn_ui_task(coro, name: str = "ui_bg") -> asyncio.Task:
    """Run slow Telegram/API actions outside the callback handler so buttons do not stick.

    v0064: one background UI task per action name. Repeated button taps must not
    stack duplicate scans/fee checks/close-all operations in the background.
    If the same action is already running, keep it and close the unused coroutine.
    """
    existing = UI_BG_TASKS.get(name)
    if existing and not existing.done():
        try:
            if hasattr(coro, "close"):
                coro.close()
        except Exception:
            pass
        return existing
    task = asyncio.create_task(coro, name=name)
    UI_BG_TASKS[name] = task

    def _cleanup(t: asyncio.Task, task_name: str = name) -> None:
        if UI_BG_TASKS.get(task_name) is t:
            UI_BG_TASKS.pop(task_name, None)

    task.add_done_callback(_cleanup)
    return task


def get_admin_ids() -> set[int]:
    """Optional Telegram access control.

    If ADMIN_IDS is empty, the bot is open to whoever can chat with it.
    If ADMIN_IDS is set, only those Telegram user IDs can use commands/buttons.
    Example: ADMIN_IDS=123456789,987654321
    """
    raw = os.getenv("ADMIN_IDS", "").strip()
    if not raw:
        return set()
    out: set[int] = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            continue
    return out


def is_admin_update(update: Update) -> bool:
    ids = get_admin_ids()
    if not ids:
        return True
    user = update.effective_user
    return bool(user and user.id in ids)


async def reject_non_admin(update: Update) -> None:
    try:
        log_event(
            "telegram_unauthorized_access",
            user_id=getattr(update.effective_user, "id", None),
            username=getattr(update.effective_user, "username", None),
            chat_id=getattr(update.effective_chat, "id", None),
        )
    except Exception:
        pass
    try:
        if update.callback_query:
            await update.callback_query.answer("⛔ Нет доступа", show_alert=True)
            return
        if update.effective_message:
            await update.effective_message.reply_text("⛔ Нет доступа")
    except Exception:
        pass


def admin_guard(handler):
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not is_admin_update(update):
            await reject_non_admin(update)
            return
        try:
            await handler(update, context)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # v0064: command handlers must never fail silently.
            try:
                log_error("telegram_handler_error", e, handler=getattr(handler, "__name__", "unknown"), chat_id=getattr(update.effective_chat, "id", None))
            except Exception:
                pass
            try:
                chat_id = update.effective_chat.id if update.effective_chat else None
                if chat_id:
                    await tg_wait(context.bot.send_message(
                        chat_id=chat_id,
                        text=f"❌ Ошибка команды {getattr(handler, '__name__', 'handler')}: {str(e)[:900]}",
                    ), timeout=8.0)
            except Exception:
                pass
    return wrapped


def b(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text, callback_data=data)


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [b("▶️ Start Tsunami", "mm:start"), b("⏸ Stop/Pause", "mm:stop")],
        [b("❌ Close All", "mm:close_all"), b("📒 Trades", "mm:trades")],
        [b("📊 Panel", "menu:main"), b("🔍 Price Scan", "mm:scan")],
        [b("⚙️ Settings", "menu:settings"), b("📈 Symbols", "menu:symbols")],
        [b("🔑 API", "menu:api"), b("🧾 Fees", "mm:fees")],
        [b("🪞 Mirror Lab", "mm:mirror")],
    ])



def command_keyboard() -> ReplyKeyboardMarkup:
    """Ordinary Telegram reply keyboard, separate from inline panel buttons."""
    return ReplyKeyboardMarkup(
        [["/start", "/ping"], ["/balance", "/status"], ["/trades", "/log_full"], ["/mirror_test report", "/mirror_test start"], ["/help"]],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Команды бота",
    )


async def delete_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: float = 1.5) -> None:
    try:
        await asyncio.sleep(delay)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except TelegramError:
        pass
    except Exception:
        pass


async def install_command_keyboard(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    """v0064: disabled by default. Native Telegram commands are enough.

    The previous helper message made commands look broken: /log_full first sent
    "menu enabled" and then, if another step failed, the user saw no real answer.
    """
    s = STORE.load()
    if not bool(s.get("telegram_reply_keyboard")):
        return
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text="⌨️ Командное меню включено. Если мешает: /set telegram_reply_keyboard false",
            reply_markup=command_keyboard(),
        )
    except TelegramError:
        pass


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, sec = divmod(rem, 60)
    if d:
        return f"{d}d {h:02d}:{m:02d}:{sec:02d}"
    return f"{h:02d}:{m:02d}:{sec:02d}"


def memory_usage_text() -> str:
    # Prefer current RSS from Linux /proc. Fallback to max RSS via resource.
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = float(line.split()[1])
                    return f"{kb / 1024:.1f} MB RSS"
    except Exception:
        pass
    try:
        import resource
        rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        # Linux returns KiB, macOS returns bytes.
        mb = rss / 1024 if rss < 10_000_000 else rss / 1024 / 1024
        return f"{mb:.1f} MB maxRSS"
    except Exception:
        return "n/a"


def ping_text(update: Update | None = None, started_perf: float | None = None) -> str:
    s = STORE.load()
    now_perf = time.perf_counter()
    processing_ms = 0.0 if started_perf is None else (now_perf - started_perf) * 1000.0
    telegram_lag = "n/a"
    msg_date = None
    if update and update.effective_message:
        msg_date = update.effective_message.date
    if msg_date:
        try:
            if msg_date.tzinfo is None:
                msg_date = msg_date.replace(tzinfo=timezone.utc)
            lag_ms = max(0.0, (datetime.now(timezone.utc) - msg_date).total_seconds() * 1000.0)
            telegram_lag = f"{lag_ms:.0f} ms"
        except Exception:
            telegram_lag = "n/a"
    return (
        f"🏓 Ping {s.get('bot_version', 'v0028')}\n\n"
        f"Отклик обработчика: {processing_ms:.1f} ms\n"
        f"Telegram lag: {telegram_lag}\n"
        f"Память: {memory_usage_text()}\n"
        f"Время работы процесса: {format_duration(time.time() - PROCESS_START_TS)}\n"
        f"Версия: {s.get('bot_version', 'v0028')}"
    )


def settings_text() -> str:
    s = STORE.load()
    return (
        f"⚙️ Price Tsunami Settings ({s.get('bot_version', 'v0064')})\n\n"
        f"Signal mode: {s.get('wave_market_signal_mode', 'all_zero_total')}\n"
        f"all_zero_total: рынок считает весь zero-fee trade universe.\n"
        f"top10_leaders: рынок считают TOP10 ликвидных non-stable zero-fee, входы всё равно из полного zero-fee universe.\n"
        f"Votes: price up = LONG, price down = SHORT, flat/no fresh price = NEUTRAL | lookback {s.get('wave_price_lookback_sec')}s\n"
        f"Denominator: проценты считаются от выбранного signal universe; без свежей цены = NEUTRAL.\n"
        f"TOP10 rules: 7/10 = NORMAL, 7/10 + рост +2 монеты за 60с = EARLY, 8/10 = TSUNAMI.\n"
        f"Early Wave: текущие >= {float(s.get('wave_early_min_side_ratio') or 0.65):.0%} и эта же сторона выросла на +{float(s.get('wave_accel_trigger_pct') or 15):.0f}п.п. за {float(s.get('wave_accel_lookback_sec') or 60):.0f}s → 5 сделок, 5x, NET +${float(s.get('wave_normal_target_profit_usdt') or 0.05):.2f}\n"
        f"Normal Wave: текущие >= {float(s.get('wave_min_side_ratio') or 0.75):.0%} → 5 сделок, 5x, NET +${float(s.get('wave_normal_target_profit_usdt') or 0.05):.2f}\n"
        f"Tsunami: текущие >= {float(s.get('wave_min_side_ratio') or 0.75):.0%} и эта же сторона выросла на +{float(s.get('wave_accel_trigger_pct') or 15):.0f}п.п. → 5 сделок, 10x, NET +${float(s.get('wave_tsunami_target_profit_usdt') or 0.10):.2f}\n"
        "Важно: 65%/75% — это текущее итоговое значение; +15п.п. уже внутри него, это не 65+15.\n"
        f"Hold: нужно {int(s.get('wave_signal_hold_required') or 4)} из {int(s.get('wave_signal_hold_checks') or 5)} checks за ~{float(s.get('wave_signal_hold_sec') or 10.0):.0f}s, не один тик.\n\n"
        f"Basket: {s.get('wave_positions')} slots | size {s.get('position_margin_percent')}% equity на слот | isolated\n"
        f"Pick range: middle {float(s.get('wave_pick_start_pct') or 0.25):.0%}-{float(s.get('wave_pick_end_pct') or 0.60):.0%} of same-side candidates\n"
        f"Open retry: 5 сделок открываются с паузой {float(s.get('wave_open_batch_gap_ms') or 0)/1000:.1f}s; при MEXC rate-limit бот ждёт {float(s.get('wave_open_retry_delay_sec') or 0):.1f}s и повторяет до {s.get('wave_open_retry_rounds')} раз, потом добирает replacement.\n"
        f"After 10m: close only zero/microplus; if minus, wait recovery.\n"
        f"Stop: pause only. Close All: cancel orders + close positions.\n\n"
        "Mirror Lab: /mirror_test start|report|stop|clear — виртуальный original vs mirror тест без реальных ордеров.\n"
        "Команды: /set size 20, /set candidates 0, /set ws_symbols 0, /set panel_sec 5, /symbols clear"
    )

def settings_menu() -> InlineKeyboardMarkup:
    s = STORE.load()
    return InlineKeyboardMarkup([
        [b(("✅ " if s.get('wave_normal_leverage') == 5 else "") + "5x normal", "set:wave_normal_leverage:5"), b(("✅ " if s.get('wave_tsunami_leverage') == 10 else "") + "10x tsunami", "set:wave_tsunami_leverage:10")],
        [b("✅ Basket 5", "set:wave_positions:5"), b("Scan ALL", "set:max_zero_fee_scan_symbols:0"), b("WS ALL", "set:ws_depth_max_symbols:0")],
        [b(("✅ " if s.get('wave_market_signal_mode') == 'all_zero_total' else "") + "Signal ALL", "set:wave_market_signal_mode:all_zero_total"), b(("✅ " if s.get('wave_market_signal_mode') == 'top10_leaders' else "") + "Signal TOP10", "set:wave_market_signal_mode:top10_leaders")],
        [b("TOP10 7/10", "set:wave_top10_normal_count:7"), b("TSUNAMI 8/10", "set:wave_top10_tsunami_count:8"), b("Accel +2", "set:wave_top10_accel_count:2")],
        [b("Early 65%", "set:wave_early_min_side_ratio:0.65"), b("Normal 75%", "set:wave_min_side_ratio:0.75"), b("Ускор. 15п.п.", "set:wave_accel_trigger_pct:15")],
        [b("Hold 4/5", "set:wave_signal_hold_required:4"), b("Hold 5 checks", "set:wave_signal_hold_checks:5"), b("Hold 10s", "set:wave_signal_hold_sec:10")],
        [
            b(("✅ " if float(s.get('position_margin_percent') or 0) == 5 else "") + "Size 5%", "set:position_margin_percent:5"),
            b(("✅ " if float(s.get('position_margin_percent') or 0) == 10 else "") + "Size 10%", "set:position_margin_percent:10"),
            b(("✅ " if float(s.get('position_margin_percent') or 0) == 15 else "") + "Size 15%", "set:position_margin_percent:15"),
            b(("✅ " if float(s.get('position_margin_percent') or 0) == 20 else "") + "Size 20%", "set:position_margin_percent:20"),
        ],
        [b("NET +$0.05", "set:wave_normal_target_profit_usdt:0.05"), b("Tsunami +$0.10", "set:wave_tsunami_target_profit_usdt:0.10"), b("Pick 25-60", "set:wave_pick_start_pct:0.25")],
        [b("Panel 2s", "set:telegram_live_update_sec:2"), b("Panel 5s", "set:telegram_live_update_sec:5"), b("Panel 10s", "set:telegram_live_update_sec:10"), b("Stopped OFF", "set:telegram_live_stopped_update_sec:0")],
        [b("Dir BOTH", "set:direction_mode:both"), b("LONG", "set:direction_mode:long"), b("SHORT", "set:direction_mode:short")],
        [b("Emergency ON/OFF", "toggle:emergency_market_close"), b("Post-close ON/OFF", "toggle:post_only_close")],
        [b("🌊 Price Tsunami Basket v0064", "preset:plus"), b("Custom mode", "preset:custom")],
        [b("⬅️ Back to Live", "menu:main")],
    ])


def symbols_text(engine: MicroMakerEngine | None = None) -> str:
    """Clean Symbols screen.

    v0064: show what matters first: raw zero-fee count, blocked count,
    ignored count, trade universe, and current scan readiness. Long explanatory
    text is removed from the main Telegram card.
    """
    s = STORE.load()
    syms = parse_symbols(str(s.get("allowed_symbols") or ""))
    whitelist_txt = "ON — " + ", ".join(syms) if syms else "OFF — FULL AUTO"
    ignored = s.get("ignored_symbols") or {}
    stored_ignored_count = len(ignored) if isinstance(ignored, dict) else 0

    raw_total = blocked_total = ignored_total = trade_universe = price_ready = no_fresh = None
    active = 0
    leader_symbols: list[str] = []
    if engine is not None:
        raw_total = int(getattr(engine.stats, "zero_fee_total_count", 0) or 0)
        blocked_total = int(getattr(engine.stats, "zero_fee_blocked_count", 0) or 0)
        ignored_total = int(getattr(engine.stats, "zero_fee_ignored_count", 0) or stored_ignored_count)
        trade_universe = int(getattr(engine.stats, "zero_fee_universe_count", 0) or len(engine.zero_fee_cache) or 0)
        w = getattr(engine.stats, "wave_state", {}) or {}
        active = int(w.get("active") or trade_universe or 0)
        price_ready = int(w.get("price_ready") or 0)
        no_fresh = int(w.get("no_fresh_price") or 0)
        leader_symbols = list(getattr(engine, "last_wave_leader_symbols", []) or [])
    if raw_total is None or raw_total <= 0:
        raw_total = 0
    if blocked_total is None:
        blocked_total = 0
    if ignored_total is None:
        ignored_total = stored_ignored_count
    if trade_universe is None:
        trade_universe = 0
    if price_ready is None:
        price_ready = 0
    if no_fresh is None:
        no_fresh = 0

    scan_cap = "ALL" if int(s.get("max_zero_fee_scan_symbols") or 0) <= 0 else str(s.get("max_zero_fee_scan_symbols"))
    ws_cap = "ALL" if int(s.get("ws_depth_max_symbols") or 0) <= 0 else str(s.get("ws_depth_max_symbols"))
    fee_mode = "zero-fee only" if bool(s.get("only_zero_fee")) else "all active, fee-guard on entry"
    quote = str(s.get("contract_quote_filter") or "USDT").upper()

    universe_line = f"MEXC zero-fee total: {raw_total}" if raw_total else "MEXC zero-fee total: ещё нет данных"
    leaders_line = ""
    if str(s.get('wave_market_signal_mode') or 'all_zero_total') == 'top10_leaders':
        leaders_line = "TOP10 leaders: " + (", ".join(leader_symbols[:10]) if leader_symbols else "будут выбраны после scan") + "\n"
    return (
        f"📈 Symbols / Universe {s.get('bot_version', 'v0064')}\n\n"
        "РЕЖИМ\n"
        f"Auto-select: {'ON' if s.get('auto_select_symbols') else 'OFF'}\n"
        f"Signal: {s.get('wave_market_signal_mode', 'all_zero_total')}\n"
        f"Fee mode: {fee_mode}\n"
        f"Whitelist: {whitelist_txt}\n\n"
        "UNIVERSE\n"
        f"{universe_line}\n"
        f"Blocked by filters: {blocked_total}\n"
        f"Ignored this session: {ignored_total}\n"
        f"Trade universe: {trade_universe}\n\n"
        "СКАН\n"
        f"Scan cap: {scan_cap} | WS cap: {ws_cap}\n"
        f"Scanning now: {active or trade_universe} / {trade_universe}\n"
        f"Ready prices: {price_ready}\n"
        f"No fresh price: {no_fresh}\n"
        f"{leaders_line}\n"
        "ФИЛЬТРЫ\n"
        f"Quote: {quote} only\n"
        "Blocked: STOCK symbols\n"
        "Fee: 0% maker/taker required\n"
        f"Spread: {s.get('min_spread_ticks')}–{s.get('max_spread_ticks')} ticks\n"
        f"Min depth: ${s.get('min_depth_usdt')} or position ×{s.get('min_depth_multiplier')}\n\n"
        "ВЫБОР СДЕЛОК\n"
        "Direction: ALL zero total или TOP10 leaders по тумблеру\n"
        f"Pick zone: middle {int(float(s.get('wave_pick_start_pct') or 0.25) * 100)}–{int(float(s.get('wave_pick_end_pct') or 0.60) * 100)}%\n"
        f"Basket slots: {int(s.get('wave_positions') or 5)}\n\n"
        "КОМАНДЫ\n"
        "/symbols LINK_USDT,SOL_USDT — whitelist\n"
        "/symbols clear — FULL AUTO\n"
        "/ignore clear или /clear_ignored — очистить ignored"
    )


def symbols_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [b("Auto-select ON/OFF", "toggle:auto_select_symbols"), b("ZeroFee ON/OFF", "toggle:only_zero_fee")],
        [b(("✅ " if STORE.load().get('wave_market_signal_mode') == 'all_zero_total' else "") + "Signal ALL", "set:wave_market_signal_mode:all_zero_total"), b(("✅ " if STORE.load().get('wave_market_signal_mode') == 'top10_leaders' else "") + "Signal TOP10", "set:wave_market_signal_mode:top10_leaders")],
        [b("Manual fallback ON/OFF", "toggle:allow_manual_fee_fallback"), b("🔍 Price Scan", "mm:scan")],
        [b("WS ON/OFF", "toggle:ws_depth_enabled"), b("MD WS", "set:market_data_mode:websocket"), b("MD REST", "set:market_data_mode:rest")],
        [b("Scan 1s", "set:scan_interval_sec:1"), b("Scan 3s", "set:scan_interval_sec:3"), b("Scan 5s", "set:scan_interval_sec:5")],
        [b("Scan ALL", "set:max_zero_fee_scan_symbols:0"), b("Cap 250", "set:max_zero_fee_scan_symbols:250"), b("Cap 100", "set:max_zero_fee_scan_symbols:100")],
        [b("WS ALL", "set:ws_depth_max_symbols:0"), b("WS 250", "set:ws_depth_max_symbols:250"), b("WS 100", "set:ws_depth_max_symbols:100")],
        [b("Rescan 60s", "set:zero_fee_rescan_sec:60"), b("Clear ignore", "ignore:clear")],
        [b("Depth $50", "set:min_depth_usdt:50"), b("$75", "set:min_depth_usdt:75"), b("$100", "set:min_depth_usdt:100")],
        [b("Depth x3", "set:min_depth_multiplier:3"), b("x4", "set:min_depth_multiplier:4"), b("x5", "set:min_depth_multiplier:5")],
        [b("Imb 1.20", "set:min_imbalance_ratio:1.20"), b("1.30", "set:min_imbalance_ratio:1.30"), b("1.45", "set:min_imbalance_ratio:1.45")],
        [b("Score 25", "set:min_trade_score:25"), b("35", "set:min_trade_score:35"), b("45", "set:min_trade_score:45")],
        [b("Switch +5%", "set:switch_score_improvement_pct:5"), b("+10%", "set:switch_score_improvement_pct:10"), b("+20%", "set:switch_score_improvement_pct:20")],
        [b("Spread 1-2", "preset:spread:1:2"), b("Spread 1-4", "preset:spread:1:4"), b("Spread 2-6", "preset:spread:2:6")],
        [b("Clear whitelist", "symbols:clear"), b("⬅️ Back to Live", "menu:main")],
    ])


def api_text() -> str:
    s = STORE.load()
    return (
        "🔑 MEXC API\n\n"
        f"Key: {mask_secret(str(s.get('mexc_api_key') or ''))}\n"
        f"Secret: {mask_secret(str(s.get('mexc_api_secret') or ''))}\n\n"
        "Сохранить: /api set API_KEY API_SECRET\n"
        "Проверить: /api status\n"
        "Удалить: /api clear\n\n"
        "В v0028 ввод API НЕ удаляется из чата: бот сохраняет ключи, оставляет сообщение и отвечает коротко: ✅ API saved."
    )


async def ensure_engine(context: ContextTypes.DEFAULT_TYPE, chat_id: int | None = None) -> MicroMakerEngine:
    global ENGINE

    async def notify(_: str) -> None:
        # No new chat messages on fills/switches/closes. The live panel will show the latest event.
        if chat_id:
            await update_live_panel(context.application, force=True)

    if ENGINE is None:
        log_event("telegram_engine_create", chat_id=chat_id)
        ENGINE = MicroMakerEngine(STORE, notify)
    else:
        ENGINE.notify = notify
    return ENGINE


def reset_engine_signal_state(engine: MicroMakerEngine | None) -> None:
    """Clear market-signal hold/history after changing ALL/TOP10 mode or presets."""
    if not engine:
        return
    try:
        engine.reset_signal_state()
    except AttributeError:
        for attr, value in {
            "wave_dominance_history": [],
            "wave_signal_hold_samples": [],
            "wave_signal_hold_last_sample_ts": 0.0,
            "wave_signal_hold_key": None,
            "wave_signal_hold_count": 0,
            "wave_signal_hold_since": 0.0,
            "wave_candidate_side": None,
            "wave_candidate_count": 0,
        }.items():
            try:
                setattr(engine, attr, value.copy() if isinstance(value, list) else value)
            except Exception:
                pass
        try:
            engine.stats.wave_state = {}
        except Exception:
            pass
    except Exception:
        pass


def normalize_market_mode(raw: str) -> str | None:
    low = str(raw or "").strip().lower()
    if low in {"all", "all_zero", "all_zero_total", "zero", "default", "по_всем"}:
        return "all_zero_total"
    if low in {"top10", "top", "leaders", "leader", "top10_leaders", "топ10"}:
        return "top10_leaders"
    return None


def panel_mode_for_signal_return() -> str:
    mode = str(STORE.load().get("telegram_panel_mode") or "settings")
    return "symbols" if mode == "symbols" else "settings"


def panel_text(engine: MicroMakerEngine | None = None) -> str:
    e = engine or ENGINE
    if e:
        return e.quick_status_text()
    s = STORE.load()
    return (
        f"🌊 Price Tsunami {s.get('bot_version', 'v0064')}\n"
        "State: STOPPED\n\n"
        "PRICE SCAN 10s: пока нет данных.\n"
        "LONG 0% | SHORT 0% | NEUTRAL 0%\n"
        "Вывод: сидим в засаде, сделки не открываем.\n\n"
        "Правила:\n"
        "Early: сейчас >=65% и эта же сторона выросла на +15п.п. за 60s → 5 сделок, 5x, NET +$0.05\n"
        "Normal: сейчас >=75% стороны → 5 сделок, 5x, NET +$0.05\n"
        "Tsunami: сейчас >=75% и эта же сторона выросла на +15п.п. за 60s → 5 сделок, 10x, NET +$0.10\n"
        "65/75 — итог сейчас; +15п.п. уже внутри этих процентов.\n"
        "v0064: сигнал должен держаться 4 из 5 checks за ~10s; один шумовой провал не сбрасывает сигнал.\n\n"
        "Stop = пауза, позиции/ордера не трогает. Close All = снести всё.\n"
        "Нажми ▶️ Start Tsunami."
    )

async def safe_delete_message(context: ContextTypes.DEFAULT_TYPE, update: Update, *, retries: bool = False) -> bool:
    s = STORE.load()
    if not bool(s.get("telegram_delete_command_messages")):
        return False
    msg = update.effective_message
    if not msg or not update.effective_chat:
        return False
    chat_id = update.effective_chat.id
    message_id = msg.message_id
    delays = [0.0, 0.35, 1.2] if retries else [0.0]
    last_error: Exception | None = None
    for delay in delays:
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            return True
        except TelegramError as e:
            last_error = e
        except Exception as e:
            last_error = e
    if retries and last_error:
        log_error("telegram_delete_sensitive_message_failed", last_error, chat_id=chat_id, message_id=message_id)
    return False


async def set_panel_identity(chat_id: int, message_id: int, mode: str = "main") -> None:
    STORE.update({
        "telegram_panel_chat_id": int(chat_id),
        "telegram_panel_message_id": int(message_id),
        "telegram_panel_mode": mode,
    })


async def delete_stored_panel(app: Application) -> None:
    s = STORE.load()
    chat_id = int(s.get("telegram_panel_chat_id") or 0)
    message_id = int(s.get("telegram_panel_message_id") or 0)
    if not chat_id or not message_id:
        return
    try:
        await app.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except TelegramError:
        pass
    STORE.update({"telegram_panel_message_id": 0, "telegram_panel_chat_id": 0})


async def upsert_panel(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    mode: str = "main",
    recreate: bool = False,
) -> None:
    async with PANEL_LOCK:
        s = STORE.load()
        old_chat_id = int(s.get("telegram_panel_chat_id") or 0)
        old_message_id = int(s.get("telegram_panel_message_id") or 0)
        if recreate and old_chat_id and old_message_id:
            try:
                await tg_wait(context.bot.delete_message(chat_id=old_chat_id, message_id=old_message_id))
            except TelegramError:
                pass
            old_message_id = 0
        if old_chat_id == chat_id and old_message_id:
            try:
                await tg_wait(context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=old_message_id,
                    text=text[:3900],
                    reply_markup=reply_markup,
                ))
                await set_panel_identity(chat_id, old_message_id, mode)
                return
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    await set_panel_identity(chat_id, old_message_id, mode)
                    return
                # fall through and create a fresh panel if the old one disappeared / is not editable
            except TelegramError:
                pass
        msg = await tg_wait(context.bot.send_message(chat_id=chat_id, text=text[:3900], reply_markup=reply_markup))
        await set_panel_identity(chat_id, msg.message_id, mode)


async def edit_query_as_panel(q, text: str, reply_markup: InlineKeyboardMarkup, mode: str = "main") -> None:
    if q.message:
        try:
            await tg_wait(q.edit_message_text(text[:3900], reply_markup=reply_markup))
            await set_panel_identity(q.message.chat_id, q.message.message_id, mode)
            return
        except BadRequest as e:
            if "Message is not modified" in str(e):
                await set_panel_identity(q.message.chat_id, q.message.message_id, mode)
                return
            # Callback UI must never stay stuck because an edit failed/raced with live refresh.
            log_error("telegram_edit_query_as_panel_bad_request", e, mode=mode)
            return
        except TelegramError as e:
            log_error("telegram_edit_query_as_panel_error", e, mode=mode)
            return
        except Exception as e:
            log_error("telegram_edit_query_as_panel_unexpected", e, mode=mode)
            return


async def update_live_panel(app: Application, force: bool = False) -> None:
    s = STORE.load()
    if not bool(s.get("telegram_live_panel")):
        return
    if str(s.get("telegram_panel_mode") or "main") != "main" and not force:
        return
    chat_id = int(s.get("telegram_panel_chat_id") or 0)
    message_id = int(s.get("telegram_panel_message_id") or 0)
    if not chat_id:
        return
    engine = ENGINE
    running = bool(engine and engine.is_running())
    refresh_mode = str(s.get("telegram_panel_refresh_mode") or "edit").lower()
    text = panel_text()[:3900]
    async with PANEL_LOCK:
        # Re-read inside the lock in case a menu callback changed mode.
        s2 = STORE.load()
        if str(s2.get("telegram_panel_mode") or "main") != "main" and not force:
            return
        chat_id = int(s2.get("telegram_panel_chat_id") or chat_id)
        message_id = int(s2.get("telegram_panel_message_id") or message_id)
        # v0064: while RUNNING, default to delete+send a fresh panel so the panel
        # stays at the bottom of Telegram. Telegram edits do not move a message.
        if running and refresh_mode in {"resend", "fresh", "bottom"}:
            if message_id:
                try:
                    await tg_wait(app.bot.delete_message(chat_id=chat_id, message_id=message_id), timeout=4.0)
                except Exception:
                    pass
            try:
                msg = await tg_wait(app.bot.send_message(chat_id=chat_id, text=text, reply_markup=main_menu()), timeout=8.0)
                await set_panel_identity(chat_id, msg.message_id, "main")
                return
            except Exception as e:
                log_error("telegram_live_panel_resend_error", e)
                # fall back to edit below if resend failed
        if not message_id:
            try:
                msg = await tg_wait(app.bot.send_message(chat_id=chat_id, text=text, reply_markup=main_menu()), timeout=8.0)
                await set_panel_identity(chat_id, msg.message_id, "main")
                return
            except Exception as e:
                log_error("telegram_live_panel_send_error", e)
                return
        try:
            await tg_wait(app.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=main_menu(),
            ), timeout=8.0)
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                log_error("telegram_live_panel_edit_bad_request", e)
        except Exception as e:
            log_error("telegram_live_panel_edit_error", e)


async def live_panel_loop(app: Application) -> None:
    """Smart live-panel refresh.

    Defaults in v0028:
    - STOPPED: no automatic refresh, so the panel is readable and quiet.
    - RUNNING without an open position: every 5 seconds.
    - RUNNING with an open position: every 2 seconds.
    - Important strategy events still force an immediate refresh via notify().
    """
    while True:
        try:
            s = STORE.load()
            if not bool(s.get("telegram_live_panel")):
                await asyncio.sleep(2.0)
                continue

            engine = ENGINE
            running = bool(engine and engine.is_running())
            has_open_position = bool(engine and getattr(engine.stats, "open_position_symbols", []))

            if not running:
                interval = float(s.get("telegram_live_stopped_update_sec") or 0.0)
                if interval <= 0:
                    await asyncio.sleep(2.0)
                    continue
            elif has_open_position:
                interval = float(s.get("telegram_live_fast_update_sec") or 2.0)
            else:
                interval = float(s.get("telegram_live_update_sec") or 5.0)

            await asyncio.sleep(max(1.0, interval))
            await update_live_panel(app, force=False)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(2.0)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    # v0064: no helper keyboard spam inside commands; answer direct.
    engine = await ensure_engine(context, chat_id)
    await upsert_panel(
        context,
        chat_id,
        panel_text(engine),
        main_menu(),
        mode="main",
        recreate=True,
    )


async def panel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    # v0064: no helper keyboard spam inside commands; answer direct.
    arg = (context.args[0].lower() if context.args else "show")
    if arg in {"reset", "new"}:
        await upsert_panel(context, chat_id, panel_text(), main_menu(), mode="main", recreate=True)
    elif arg in {"off", "delete"}:
        await delete_stored_panel(context.application)
    else:
        await upsert_panel(context, chat_id, panel_text(), main_menu(), mode="main", recreate=False)


async def api_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # v0028: user's API input message must remain in Telegram chat history.
    # Save keys into settings only; do NOT call safe_delete_message here.
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    args = context.args or []
    if not args or args[0].lower() in {"status", "show"}:
        await upsert_panel(context, chat_id, api_text(), main_menu(), mode="api")
        return
    if args[0].lower() == "set":
        if len(args) < 3:
            await context.bot.send_message(chat_id=chat_id, text="Usage: /api set API_KEY API_SECRET")
            return
        STORE.update({"mexc_api_key": args[1].strip(), "mexc_api_secret": args[2].strip()})
        log_event("api_saved_keep_chat_message", mode="command_set")
        await context.bot.send_message(chat_id=chat_id, text="✅ API saved")
        return
    if args[0].lower() == "clear":
        STORE.update({"mexc_api_key": "", "mexc_api_secret": ""})
        await upsert_panel(context, chat_id, "✅ MEXC API удалён.\n\n" + api_text(), main_menu(), mode="api")
        return
    await upsert_panel(context, chat_id, "Usage: /api set API_KEY API_SECRET | /api status | /api clear", main_menu(), mode="api")


def _parse_api_plain_text(text: str) -> tuple[str, str] | None:
    raw = " ".join(str(text or "").strip().split())
    if not raw:
        return None
    low = raw.lower()
    for prefix in ("/api set ", "api set ", "mexc api ", "api "):
        if low.startswith(prefix):
            raw = raw[len(prefix):].strip()
            break
    parts = raw.split()
    if len(parts) < 2:
        return None
    key, secret = parts[0].strip(), parts[1].strip()
    if len(key) < 8 or len(secret) < 8:
        return None
    # MEXC keys often start with mx, but do not require that strictly because
    # accounts/regions can vary. Require both tokens to be long enough instead.
    return key, secret


async def api_plaintext_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Allows the user to open 🔑 API and paste "KEY SECRET" without /api set.
    # v0028: keep that pasted message in Telegram chat history by request.
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id or not update.effective_message or not update.effective_message.text:
        return
    s = STORE.load()
    text = update.effective_message.text.strip()
    low = text.lower()
    in_api_mode = str(s.get("telegram_panel_mode") or "") == "api"
    if not in_api_mode and not low.startswith(("api set ", "mexc api ", "api ")):
        return
    parsed = _parse_api_plain_text(text)
    if not parsed:
        return
    key, secret = parsed
    STORE.update({"mexc_api_key": key, "mexc_api_secret": secret})
    log_event("api_saved_keep_chat_message", mode="plain_text")
    await context.bot.send_message(chat_id=chat_id, text="✅ API saved")


def apply_plus_profile() -> None:
    STORE.update(dict(ACTIVE_PLUS_PROFILE_V0023))


async def preset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    args = [a.lower() for a in (context.args or [])]
    if args and args[0] in {"custom", "manual"}:
        STORE.set("trade_profile", "custom")
        await upsert_panel(context, chat_id, "✅ Custom mode включён: дальше /set не будет перетираться миграцией профиля.\n\n" + settings_text(), settings_menu(), mode="settings")
        return
    apply_plus_profile()
    engine = await ensure_engine(context, chat_id)
    reset_engine_signal_state(engine)
    engine.clear_ignored_symbols()
    await upsert_panel(context, chat_id, "🌊 Price Tsunami v0064 применён: 10s price-scan, итоговые 65/75% + рост 15п.п., 5 LONG/SHORT, 5x/10x, REAL NET выход.\n\n" + settings_text(), settings_menu(), mode="settings")


async def set_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    args = context.args or []
    if len(args) < 2:
        await upsert_panel(context, chat_id, "Usage: /set leverage 5 | /set size 10 | /set scan_interval_sec 5", settings_menu(), mode="settings")
        return
    alias = {
        "margin": "margin_per_position_usdt",
        "size": "position_margin_percent",
        "risk": "position_margin_percent",
        "pos": "max_positions",
        "positions": "max_positions",
        "symbols": "symbols_limit",
        "tp": "target_ticks",
        "sl": "stop_ticks",
        "life": "order_lifetime_ms",
        "scan": "scan_interval_sec",
        "candidates": "max_zero_fee_scan_symbols",
        "depth": "min_depth_usdt",
        "depth_usdt": "min_depth_usdt",
        "depthx": "min_depth_multiplier",
        "imb": "min_imbalance_ratio",
        "imbalance": "min_imbalance_ratio",
        "score": "min_trade_score",
        "min_score": "min_trade_score",
        "recheck": "entry_recheck_ms",
        "recheck_ms": "entry_recheck_ms",
        "recheck_count": "entry_recheck_count",
        "cooldown_loss": "cooldown_after_loss_sec",
        "cooldown_trade": "cooldown_after_trade_sec",
        "time_offset": "telegram_time_offset_hours",
        "tz": "telegram_time_offset_hours",
        "log_retention": "full_log_retention_minutes",
        "log_mb": "full_log_export_max_mb",
        "time_market": "emergency_market_close_on_time_stop",
        "hard_life": "max_position_hard_lifetime_sec",
        "switch": "switch_score_improvement_pct",
        "md": "market_data_mode",
        "market_mode": "wave_market_signal_mode",
        "signal_mode": "wave_market_signal_mode",
        "signal": "wave_market_signal_mode",
        "ws": "ws_depth_enabled",
        "ws_symbols": "ws_depth_max_symbols",
        "ws_stale": "ws_book_stale_ms",
        "rescan": "zero_fee_rescan_sec",
        "universe": "zero_fee_universe_max_symbols",
        "panel_sec": "telegram_live_update_sec",
        "panel_fast_sec": "telegram_live_fast_update_sec",
        "panel_stopped_sec": "telegram_live_stopped_update_sec",
        "rest_base": "mexc_rest_base",
        "base": "mexc_rest_base",
        "recv": "mexc_recv_window",
        "recv_window": "mexc_recv_window",
        "rate": "mexc_private_rate_limit",
        "private_rate": "mexc_private_rate_limit",
        "public_timeout": "mexc_public_timeout",
        "private_timeout": "mexc_private_timeout",
        "strict_leverage": "mexc_strict_leverage",
        "leverage_setup": "mexc_set_leverage_on_entry",
        "set_leverage": "mexc_set_leverage_on_entry",
        "ws_endpoint": "mexc_futures_ws",
    }
    key = alias.get(args[0].lower(), args[0].lower())
    if key not in DEFAULTS:
        await upsert_panel(context, chat_id, f"Unknown setting: {key}", settings_menu(), mode="settings")
        return
    raw = args[1]
    old = DEFAULTS[key]
    try:
        if key == "wave_market_signal_mode":
            normalized = normalize_market_mode(raw)
            if not normalized:
                await upsert_panel(context, chat_id, "❌ market mode: используй all или top10", settings_menu(), mode="settings")
                return
            val = normalized
        elif isinstance(old, bool):
            val: Any = raw.lower() in {"1", "true", "yes", "on", "да", "вкл"}
        elif isinstance(old, int):
            val = int(float(raw))
        elif isinstance(old, float):
            val = float(raw)
        else:
            val = raw
        STORE.set(key, val)
        if key == "wave_market_signal_mode":
            reset_engine_signal_state(ENGINE)
        if key in {"scan_interval_sec", "max_zero_fee_scan_symbols", "zero_fee_rescan_sec", "zero_fee_universe_max_symbols", "min_depth_usdt", "min_depth_multiplier", "switch_score_improvement_pct", "min_imbalance_ratio", "min_trade_score", "entry_recheck_ms", "entry_recheck_required", "entry_recheck_count", "cooldown_after_loss_sec", "cooldown_after_trade_sec", "market_data_mode", "ws_depth_enabled", "ws_depth_max_symbols", "ws_book_stale_ms"}:
            await upsert_panel(context, chat_id, f"✅ {key} = {val}\n\n" + symbols_text(), symbols_menu(), mode="symbols")
        else:
            await upsert_panel(context, chat_id, f"✅ {key} = {val}\n\n" + settings_text(), settings_menu(), mode="settings")
    except Exception as e:
        await upsert_panel(context, chat_id, f"❌ {e}", settings_menu(), mode="settings")


async def symbols_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    engine = await ensure_engine(context, chat_id)
    raw = " ".join(context.args or []).strip()
    if raw.lower() in {"clear", "auto", "all", "*"}:
        STORE.set("allowed_symbols", "")
        await upsert_panel(context, chat_id, "✅ Whitelist очищен. Включён FULL AUTO.\n\n" + symbols_text(engine), symbols_menu(), mode="symbols")
        return
    syms = parse_symbols(raw)
    if not syms:
        await upsert_panel(context, chat_id, symbols_text(engine), symbols_menu(), mode="symbols")
        return
    STORE.set("allowed_symbols", ",".join(syms))
    await upsert_panel(context, chat_id, "✅ Whitelist updated:\n" + ", ".join(syms) + "\n\n" + symbols_text(engine), symbols_menu(), mode="symbols")


async def send_direct_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, *, timeout: float = 10.0) -> None:
    """Send a normal chat message without touching the live panel lock."""
    await tg_wait(context.bot.send_message(chat_id=chat_id, text=text[:3900]), timeout=timeout)


async def reply_direct(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, *, timeout: float = 10.0) -> None:
    """Reply to the exact command message.

    v0064: commands must return visible messages in chat and never rely on the
    live panel. If reply_text fails because Telegram/client state is odd, fall
    back to a normal send_message.
    """
    chat_id = update.effective_chat.id if update.effective_chat else None
    msg = update.effective_message
    if msg is not None:
        try:
            await tg_wait(msg.reply_text(text[:3900]), timeout=timeout)
            return
        except Exception:
            pass
    if chat_id:
        await send_direct_message(context, chat_id, text, timeout=timeout)


def _tail_file_text(path: Path, max_bytes: int = 250_000) -> str:
    try:
        if not path.exists():
            return ""
        with path.open("rb") as f:
            try:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(max(0, size - max_bytes), os.SEEK_SET)
            except Exception:
                pass
            data = f.read(max_bytes)
        text = data.decode("utf-8", errors="replace")
        if "\n" in text and len(data) >= max_bytes:
            text = text.split("\n", 1)[1]
        return text
    except Exception as e:
        return f"<tail read error: {e}>"


def build_fast_log_export() -> Path:
    """Fast emergency log export that never calls exchange APIs and avoids heavy pruning.

    This is intentionally simpler than export_full_log(): it tails local log files
    and writes a small file. The goal is reliability when the bot is half-broken.
    """
    from datetime import datetime
    s = STORE.load()
    out_dir = Path("logs/exports")
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"mexc_micro_maker_FAST_log_{s.get('bot_version','v0064')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    engine = ENGINE
    parts: list[str] = []
    parts.append("MEXC MICRO MAKER FAST DEBUG LOG")
    parts.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}")
    parts.append(f"Version: {s.get('bot_version')} instance={BOT_INSTANCE_ID} pid={os.getpid()}")
    parts.append("\n=== SETTINGS SHORT ===")
    for k in ["bot_version","trade_profile","live_enabled","telegram_panel_chat_id","telegram_panel_message_id","telegram_panel_mode","telegram_delete_command_messages","telegram_reply_keyboard","market_data_mode","wave_market_signal_mode"]:
        parts.append(f"{k}: {s.get(k)}")
    parts.append("\n=== ENGINE SHORT ===")
    try:
        if engine is None:
            parts.append("ENGINE=None")
        else:
            parts.append(f"running={bool(engine.is_running())} running_flag={getattr(engine,'running',None)}")
            st = getattr(engine, 'stats', None)
            parts.append(f"last_action={getattr(st,'last_action','')}")
            parts.append(f"last_error={getattr(st,'last_error','')}")
            parts.append(f"last_scan_ts={getattr(st,'last_scan_ts',0)}")
            parts.append(f"zero_fee_cache_count={len(getattr(engine,'zero_fee_cache',[]) or [])}")
            parts.append(f"ui_tasks={list(UI_BG_TASKS.keys())}")
    except Exception as e:
        parts.append(f"<engine short error: {e}>")
    parts.append("\n=== LOG TAIL ===")
    log_dir = Path(os.getenv("MICRO_MAKER_LOG_DIR", "logs"))
    for fp in list(sorted(log_dir.glob("log_full.txt.*"))) + [log_dir / "log_full.txt"]:
        if fp.exists():
            parts.append(f"\n--- {fp} ---")
            parts.append(_tail_file_text(fp, max_bytes=220_000))
    out.write_text("\n".join(parts), encoding="utf-8")
    return out


async def finish_direct_task(context: ContextTypes.DEFAULT_TYPE, chat_id: int | None, text_or_coro, label: str, *, timeout_sec: float = 60.0) -> None:
    """Run a slow task and always return result/error as a normal chat message."""
    if not chat_id:
        return
    try:
        if asyncio.iscoroutine(text_or_coro):
            msg = await asyncio.wait_for(text_or_coro, timeout=max(1.0, float(timeout_sec or 60.0)))
        else:
            msg = str(text_or_coro)
        await send_direct_message(context, chat_id, str(msg)[:3900], timeout=10.0)
    except asyncio.TimeoutError:
        await send_direct_message(context, chat_id, f"⏱ {label}: timeout {timeout_sec:.0f}s. Команда не зависла, фон остановлен/сброшен.", timeout=10.0)
    except Exception as e:
        try:
            log_error("direct_task_error", e, label=label, chat_id=chat_id)
        except Exception:
            pass
        await send_direct_message(context, chat_id, f"❌ {label}: {str(e)[:900]}", timeout=10.0)


async def mirror_collect_loop(context: ContextTypes.DEFAULT_TYPE, chat_id: int, rounds: int = 90, interval_sec: float = 2.0) -> None:
    """Background virtual collector for Mirror Lab.

    It runs Price Scan in read-only mode, records Mirror snapshots inside the
    engine, and never opens/cancels/closes real orders.
    """
    engine = await ensure_engine(context, chat_id)
    log_event("mirror_lab_collect_started", chat_id=chat_id, rounds=rounds, interval_sec=interval_sec)
    errors = 0
    completed = 0
    try:
        for _ in range(max(1, int(rounds))):
            if not bool(STORE.load().get("mirror_lab_enabled", False)):
                break
            try:
                await asyncio.wait_for(engine.scan_now_text(), timeout=25.0)
                completed += 1
            except asyncio.TimeoutError as e:
                errors += 1
                log_error("mirror_lab_collect_scan_timeout", e, chat_id=chat_id)
            except Exception as e:
                errors += 1
                log_error("mirror_lab_collect_scan_error", e, chat_id=chat_id)
            await asyncio.sleep(max(0.5, float(interval_sec)))
    finally:
        snaps = len(getattr(engine, "mirror_lab_snapshots", []) or [])
        log_event("mirror_lab_collect_finished", chat_id=chat_id, completed=completed, errors=errors, snapshots=snaps)
        try:
            if bool(STORE.load().get("mirror_lab_enabled", False)):
                await send_direct_message(
                    context,
                    chat_id,
                    f"🪞 Mirror Lab сбор завершён/пауза. Снимков: {snaps}.\nКоманда отчёта: /mirror_test report",
                    timeout=10.0,
                )
        except Exception:
            pass


async def mirror_test_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Direct Mirror Lab command handler.

    v0064 fix: no acknowledgement-only response. Every command returns its
    final result as a normal reply message. Slow collection runs in background,
    but start/report/stop/clear always give a visible answer immediately.
    """
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return

    try:
        txt = update.effective_message.text if update.effective_message else "/mirror_test"
        cmd = txt.split()[0].split("@")[0].lstrip("/").lower()
    except Exception:
        txt = "/mirror_test"
        cmd = "mirror_test"
    args = [str(a).lower() for a in (context.args or [])]

    if cmd in {"mirror_start", "mirror_on"}:
        action = "start"
    elif cmd in {"mirror_report", "mirror_status"}:
        action = "report"
    elif cmd in {"mirror_stop", "mirror_off"}:
        action = "stop"
    elif cmd in {"mirror_clear", "mirror_reset"}:
        action = "clear"
    else:
        action = args[0] if args else "report"

    try:
        engine = await asyncio.wait_for(ensure_engine(context, chat_id), timeout=3.0)
    except Exception as e:
        await reply_direct(update, context, f"❌ Mirror Lab engine error: {str(e)[:900]}", timeout=8.0)
        return

    if action in {"start", "on", "run", "enable"}:
        try:
            STORE.set("mirror_lab_enabled", True)
            msg = engine.clear_mirror_lab()
            existing = UI_BG_TASKS.get("mirror_collect")
            already = bool(existing and not existing.done())
            if not already:
                spawn_ui_task(mirror_collect_loop(context, chat_id), name="mirror_collect")
            await reply_direct(
                update,
                context,
                "🪞 Mirror Lab START v0064\n\n"
                "Статус: виртуальный сбор запущен. Реальных сделок нет.\n"
                f"{msg}\n"
                f"Collector: {'уже работал' if already else 'запущен'}\n\n"
                "Проверка:\n"
                "/doctor — должен показать ui_tasks: mirror_collect\n"
                "/mirror_report или /mirror_test report — отчёт через 1–3 минуты.",
                timeout=10.0,
            )
        except Exception as e:
            log_error("mirror_test_start_error", e, chat_id=chat_id)
            await reply_direct(update, context, f"❌ Mirror START error: {str(e)[:900]}", timeout=8.0)
        return

    if action in {"stop", "off", "disable"}:
        try:
            STORE.set("mirror_lab_enabled", False)
            task = UI_BG_TASKS.get("mirror_collect")
            if task and not task.done():
                task.cancel()
            report = engine.mirror_lab_report_text()
            await reply_direct(update, context, "🪞 Mirror Lab STOP v0064\n\n" + report, timeout=10.0)
        except Exception as e:
            log_error("mirror_test_stop_error", e, chat_id=chat_id)
            await reply_direct(update, context, f"❌ Mirror STOP error: {str(e)[:900]}", timeout=8.0)
        return

    if action in {"clear", "reset"}:
        try:
            STORE.set("mirror_lab_enabled", False)
            task = UI_BG_TASKS.get("mirror_collect")
            if task and not task.done():
                task.cancel()
            msg = engine.clear_mirror_lab()
            await reply_direct(update, context, "🪞 Mirror Lab CLEAR v0064\n\n" + msg, timeout=8.0)
        except Exception as e:
            log_error("mirror_test_clear_error", e, chat_id=chat_id)
            await reply_direct(update, context, f"❌ Mirror CLEAR error: {str(e)[:900]}", timeout=8.0)
        return

    if action in {"status", "report", "show", ""}:
        try:
            report = engine.mirror_lab_report_text()
            task = UI_BG_TASKS.get("mirror_collect")
            collector = "running" if task and not task.done() else "off"
            await reply_direct(update, context, f"🪞 Mirror Lab REPORT v0064\nCollector: {collector}\n\n" + report, timeout=10.0)
        except Exception as e:
            log_error("mirror_test_report_error", e, chat_id=chat_id)
            await reply_direct(update, context, f"❌ Mirror REPORT error: {str(e)[:900]}", timeout=8.0)
        return

    await reply_direct(
        update,
        context,
        "🪞 Mirror Lab commands v0064:\n"
        "/mirror_test start — включить виртуальный сбор\n"
        "/mirror_test report — показать replay отчёт\n"
        "/mirror_test stop — остановить сбор\n"
        "/mirror_test clear — очистить буфер\n\n"
        "Коротко: /mirror_start /mirror_report /mirror_stop /mirror_clear",
        timeout=8.0,
    )

async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    await send_direct_message(context, chat_id, "⏳ /scan принят v0064. Это read-only Price Scan, реальных сделок нет...", timeout=8.0)
    engine = await ensure_engine(context, chat_id)
    try:
        txt = await asyncio.wait_for(engine.scan_now_text(), timeout=60.0)
    except Exception as e:
        txt = f"❌ Scan error: {str(e)[:900]}"
    await send_direct_message(context, chat_id, txt[:3900], timeout=10.0)


async def market_mode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    engine = await ensure_engine(context, chat_id)
    raw = " ".join(context.args or []).strip().lower()
    normalized = normalize_market_mode(raw)
    if normalized:
        STORE.set("wave_market_signal_mode", normalized)
        reset_engine_signal_state(engine)
        if normalized == "all_zero_total":
            msg = "✅ Market signal mode: all_zero_total — рынок считается по всему zero-fee universe."
        else:
            msg = "✅ Market signal mode: top10_leaders — рынок считают TOP10 ликвидных non-stable, входы из полного zero-fee universe."
        await upsert_panel(context, chat_id, msg + "\n\n" + settings_text(), settings_menu(), mode="settings")
        return
    s = STORE.load()
    await upsert_panel(
        context, chat_id,
        "Market signal mode: " + str(s.get("wave_market_signal_mode", "all_zero_total")) + "\n\n"
        "Команды:\n"
        "/market_mode all — как сейчас, рынок по всему zero-fee universe\n"
        "/market_mode top10 — TOP10 направление: 7/10 normal, 7/10 +2 early, 8/10 tsunami; входы из полного zero-fee",
        settings_menu(), mode="settings")


async def ignore_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    engine = await ensure_engine(context, chat_id)
    args = [a.lower() for a in (context.args or [])]
    if args and args[0] in {"clear", "reset", "0"}:
        msg = engine.clear_ignored_symbols()
        await upsert_panel(context, chat_id, msg + "\n\n" + symbols_text(engine), symbols_menu(), mode="symbols")
        return
    await upsert_panel(context, chat_id, engine.ignored_symbols_text(), symbols_menu(), mode="symbols")


async def clear_ignored_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    engine = await ensure_engine(context, chat_id)
    msg = engine.clear_ignored_symbols()
    await upsert_panel(context, chat_id, msg + "\n\n" + symbols_text(engine), symbols_menu(), mode="symbols")


async def close_all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    engine = await ensure_engine(context, chat_id)
    msg = await engine.close_all()
    await upsert_panel(context, chat_id, msg + "\n\n" + panel_text(engine), main_menu(), mode="main")



async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    started = time.perf_counter()
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    await send_direct_message(context, chat_id, ping_text(update, started), timeout=8.0)


async def balance_text(engine: MicroMakerEngine) -> str:
    """Read live USDT balance and currently open futures positions from MEXC."""
    try:
        client = await engine._ensure_client()
        bal = await client.fetch_balance()
        usdt = bal.get("USDT") or {}
        total = float(usdt.get("total") or 0)
        free = float(usdt.get("free") or 0)
        used = float(usdt.get("used") or 0)
        positions = []
        try:
            positions = await client.fetch_positions()
        except Exception:
            positions = []
        pos_text = "нет открытых позиций"
        if positions:
            rows = []
            for p in positions[:10]:
                rows.append(f"{p.get('symbol')} {p.get('side')} contracts={p.get('contracts')} entry={p.get('entryPrice')}")
            pos_text = "\n".join(rows)
        return (
            "💰 Balance — live API read\n\n"
            f"USDT total: {total:.4f}\n"
            f"USDT free: {free:.4f}\n"
            f"USDT used: {used:.4f}\n\n"
            f"Positions:\n{pos_text}"
        )
    except Exception as e:
        return f"❌ Balance error: {str(e)[:500]}"


async def balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    engine = await ensure_engine(context, chat_id)
    await tg_wait(context.bot.send_message(chat_id=chat_id, text=(await balance_text(engine))[:3900]))


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    await send_direct_message(context, chat_id, "⏳ /status принят v0064, читаю статус отдельным сообщением...", timeout=8.0)
    engine = await ensure_engine(context, chat_id)
    try:
        try:
            await asyncio.wait_for(engine._ensure_client(), timeout=12.0)
        except Exception:
            pass
        txt = await asyncio.wait_for(engine.status_text(), timeout=25.0)
    except Exception as e:
        txt = f"❌ Status error: {str(e)[:900]}"
    await send_direct_message(context, chat_id, txt[:3900], timeout=10.0)


async def trades_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    engine = await ensure_engine(context, chat_id)
    args = [a.lower() for a in (context.args or [])]
    if args and args[0] in {"reset", "clear", "0"}:
        STORE.update({
            "total_trades_count": 0,
            "total_wins_count": 0,
            "total_losses_count": 0,
            "total_estimated_pnl_usdt": 0.0,
        })
        await send_direct_message(context, chat_id, "✅ Total trade counter reset.\n\n" + engine.trades_counter_text(), timeout=8.0)
        return
    await send_direct_message(context, chat_id, engine.trades_counter_text(), timeout=8.0)


async def log_full_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # v0064: text-first log. Default command must always give useful output,
    # even if Telegram document upload is slow/broken.
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    args = [a.lower() for a in (context.args or [])]
    if args and args[0] in {"clear", "reset", "0"}:
        clear_full_log()
        log_event("log_full_cleared_by_user", chat_id=chat_id)
        await send_direct_message(context, chat_id, "✅ log_full очищен. Новый лог начнёт писаться сразу после следующего действия бота.", timeout=8.0)
        return

    log_event("log_full_fast_requested", chat_id=chat_id, version=STORE.load().get("bot_version"))
    tail = _tail_file_text(Path(os.getenv("MICRO_MAKER_LOG_DIR", "logs")) / "log_full.txt", max_bytes=3500)
    if not tail.strip():
        tail = "Лог пока пустой. Если скан RUNNING, через 5–10 секунд тут должны появиться wave_signal_check / scan_summary."
    await send_direct_message(context, chat_id, ("📄 log_full v0064 FAST TEXT\n\n" + tail[-3600:])[:3900], timeout=10.0)

    # File upload only on explicit request, so /log_full itself cannot appear to hang.
    if args and args[0] in {"file", "full", "txt", "document"}:
        try:
            path = await asyncio.wait_for(asyncio.to_thread(build_fast_log_export), timeout=5.0)
            with open(path, "rb") as f:
                await tg_wait(context.bot.send_document(
                    chat_id=chat_id,
                    document=f,
                    filename=Path(path).name,
                    caption=f"📄 FAST log {STORE.load().get('bot_version', 'v0064')}",
                ), timeout=18.0)
        except Exception as e:
            log_error("log_full_file_error", e, chat_id=chat_id)
            await send_direct_message(context, chat_id, f"⚠️ Файл лога не ушёл, но текстовый хвост выше уже отправлен. Ошибка файла: {str(e)[:500]}", timeout=8.0)


async def log_tail_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    tail = _tail_file_text(Path(os.getenv("MICRO_MAKER_LOG_DIR", "logs")) / "log_full.txt", max_bytes=3500)
    if not tail.strip():
        tail = "Лог пока пустой."
    await send_direct_message(context, chat_id, ("📄 log_tail v0064\n\n" + tail[-3600:])[:3900], timeout=10.0)


async def doctor_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fast no-lock diagnostics: proves which process/version is answering."""
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    engine = await ensure_engine(context, chat_id)
    s = STORE.load()
    task = getattr(engine, "task", None)
    loop_state = "none"
    if task:
        loop_state = "done" if task.done() else "running"
    ws_stats = {}
    try:
        if getattr(engine, "depth_ws", None):
            ws_stats = engine.depth_ws.stats()
    except Exception:
        ws_stats = {}
    text = (
        f"🩺 Doctor {s.get('bot_version')}\n"
        f"instance: {BOT_INSTANCE_ID}\n"
        f"pid: {os.getpid()}\n"
        f"engine.running: {bool(getattr(engine, 'running', False))}\n"
        f"engine.is_running: {engine.is_running()}\n"
        f"run_loop task: {loop_state}\n"
        f"loop_tick_count: {getattr(engine, '_run_loop_tick_count', 0)}\n"
        f"loop_heartbeat_age: {time.time() - float(getattr(engine, '_last_loop_tick_ts', 0) or 0):.1f}s\n"
        f"api_errors: {getattr(engine.stats, 'api_errors', 0)}\n"
        f"panel_mode: {s.get('telegram_panel_mode')}\n"
        f"panel_msg: {s.get('telegram_panel_message_id')}\n"
        f"ui_tasks: {', '.join([k for k,v in UI_BG_TASKS.items() if not v.done()]) or '-'}\n"
        f"last_scan_age: {time.time() - float(getattr(engine.stats, 'last_scan_ts', 0) or 0):.1f}s\n"
        f"last_error: {getattr(engine.stats, 'last_error', '') or '-'}\n"
        f"ws: desired={ws_stats.get('desired')} subscribed={ws_stats.get('subscribed')} fresh={ws_stats.get('fresh_books')} err={ws_stats.get('last_error') or '-'}"
    )
    await tg_wait(context.bot.send_message(chat_id=chat_id, text=text[:3900]), timeout=8.0)


async def panel_reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Forget the old panel id and create a fresh message. Useful after redeploy/version mix."""
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    STORE.update({"telegram_panel_message_id": 0, "telegram_panel_chat_id": int(chat_id), "telegram_panel_mode": "main"})
    await upsert_panel(context, chat_id, panel_text(), main_menu(), mode="main", recreate=True)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    # v0064: no helper keyboard spam inside commands; answer direct.
    s = STORE.load()
    txt = (
        f"🆘 Price Tsunami Help — {s.get('bot_version', 'v0064')}\n\n"
        "Логика торговли:\n"
        "1) Бот держит ALL active zero-fee *_USDT universe, без лимита 250.\n"
        "2) Каждые ~10 секунд сравнивает mid-price каждой монеты.\n"
        "3) Считает рынок: LONG %, SHORT %, NEUTRAL %. Проценты от всего universe; если по монете нет свежей цены/истории — она считается NEUTRAL, а не пропадает из знаменателя.\n"
        "4) Если перевес слабый — ничего не открывает.\n\n"
        "Режимы входа:\n"
        "Early Wave: сейчас >=65% одной стороны и эта же сторона выросла на +15п.п. за 60s → 5 сделок, 5x, REAL NET TP +$0.05.\n"
        "Normal Wave: сейчас >=75% одной стороны → 5 сделок, 5x, REAL NET TP +$0.05.\n"
        "Tsunami: сейчас >=75% и эта же сторона выросла на +15п.п. за 60s → 5 сделок, 10x, REAL NET TP +$0.10.\n"
        "TOP10: 7/10 = NORMAL, 7/10 + рост +2 монеты за 60с = EARLY, 8/10 = TSUNAMI. Входы всё равно из полного zero-fee universe.\n"
        "Важно: 65% и 75% — текущий итоговый процент; +15п.п. уже внутри этого значения, это не 65+15.\n"
        "v0064 HOLD: вход только когда сигнал подтверждён 4 из 5 checks за ~10s; один шумовой провал не сбрасывает сигнал.\n\n"
        "Выбор монет: не самый перегретый топ, а середина 25-60% same-side candidates.\n"
        "Все сделки открываются одной стороной: либо 5 LONG, либо 5 SHORT. Если MEXC режет быстрые заявки, бот ждёт и повторяет те же слоты, затем добирает заменами.\n"
        "Закрытие: вся корзина по REAL NET equity PnL. Через 10 минут закрывает только ноль/микроплюс; минус не режет, ждёт восстановления.\n\n"
        "Кнопки:\n"
        "▶️ Start Tsunami — запустить режим.\n"
        "⏸ Stop/Pause — только пауза, позиции и ордера не трогает.\n"
        "❌ Close All — отменяет ордера и закрывает позиции market.\n"
        "🔍 Price Scan — показать текущие LONG/SHORT/NEUTRAL и вывод.\n"
        "🪞 Mirror Lab — виртуально прогнать плохие модели и их зеркало по накопленным scan-снимкам.\n\n"
        "Mirror commands: /mirror_test start, /mirror_test report, /mirror_test stop, /mirror_test clear.\n"
        "Команды: /api set KEY SECRET, /balance, /status, /log_full, /symbols clear, /set size 20, /set candidates 0."
    )
    await send_direct_message(context, chat_id, txt[:3900], timeout=8.0)


async def _finish_panel_task(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int | None,
    engine: MicroMakerEngine,
    text_or_coro,
    reply_markup: InlineKeyboardMarkup | None = None,
    mode: str = "main",
    append_panel: bool = True,
    timeout_sec: float = 90.0,
) -> None:
    """Execute a slow action and refresh panel afterwards. Used so button callbacks answer instantly.

    v0064: detail screens such as Price Scan should not append the live panel
    underneath. Slow background actions are deduped and wrapped in a timeout so
    repeated button taps cannot leave endless pending UI tasks.
    """
    if not chat_id:
        return
    try:
        if asyncio.iscoroutine(text_or_coro):
            msg = await asyncio.wait_for(text_or_coro, timeout=max(1.0, float(timeout_sec or 90.0)))
        else:
            msg = str(text_or_coro)
        final_text = msg if not append_panel else (msg + "\n\n" + panel_text(engine))
        await upsert_panel(context, chat_id, final_text[:3900], reply_markup or main_menu(), mode=mode)
    except asyncio.TimeoutError as e:
        log_error("telegram_background_panel_task_timeout", e, mode=mode, timeout_sec=timeout_sec)
        try:
            fallback = f"⏱ Команда не завершилась за {timeout_sec:.0f}с. Проверь статус/лог; повторный тап не запускает дубль в фоне."
            if append_panel:
                fallback += "\n\n" + panel_text(engine)
            await upsert_panel(context, chat_id, fallback[:3900], reply_markup or main_menu(), mode=mode)
        except Exception:
            pass
    except Exception as e:
        log_error("telegram_background_panel_task_error", e, mode=mode)
        try:
            fallback = f"❌ Ошибка фоновой команды: {str(e)[:500]}"
            if append_panel:
                fallback += "\n\n" + panel_text(engine)
            await upsert_panel(context, chat_id, fallback[:3900], reply_markup or main_menu(), mode=mode)
        except Exception:
            pass


async def start_engine_direct(context: ContextTypes.DEFAULT_TYPE, chat_id: int | None, engine: MicroMakerEngine) -> None:
    if not chat_id:
        return
    try:
        msg = await asyncio.wait_for(engine.start(), timeout=35.0)
        await send_direct_message(context, chat_id, (msg or "▶️ Start done")[:3900], timeout=8.0)
        await upsert_panel(context, chat_id, panel_text(engine), main_menu(), mode="main", recreate=True)
    except asyncio.TimeoutError:
        try:
            engine.running = False
            STORE.set("live_enabled", False)
        except Exception:
            pass
        await send_direct_message(context, chat_id, "⏱ Start timeout 35s. Запуск сброшен, чтобы бот не завис. Проверь /doctor и /log_tail.", timeout=8.0)
    except Exception as e:
        log_error("start_engine_direct_error", e, chat_id=chat_id)
        await send_direct_message(context, chat_id, f"❌ Start error: {str(e)[:900]}", timeout=8.0)


async def stop_engine_direct(context: ContextTypes.DEFAULT_TYPE, chat_id: int | None, engine: MicroMakerEngine) -> None:
    if not chat_id:
        return
    try:
        msg = await asyncio.wait_for(engine.stop(close_positions=False), timeout=20.0)
    except Exception as e:
        log_error("stop_engine_direct_error", e, chat_id=chat_id)
        msg = f"❌ Stop error: {str(e)[:900]}"
    await send_direct_message(context, chat_id, (msg or "⏸ Stop done")[:3900], timeout=8.0)
    try:
        await upsert_panel(context, chat_id, panel_text(engine), main_menu(), mode="main", recreate=True)
    except Exception:
        pass


def fees_cached_text(engine: MicroMakerEngine) -> str:
    s = STORE.load()
    zeros = list(getattr(engine, "zero_fee_cache", []) or [])
    ignored = engine._ignored_symbols(s) if hasattr(engine, "_ignored_symbols") else set()
    trade = [x for x in zeros if x and x not in ignored and not engine._blocked_symbol(x)] if zeros else []
    st = getattr(engine, "stats", None)
    first = ", ".join(trade[:60]) if trade else "-"
    more = f"\n...ещё {max(0, len(trade)-60)}" if len(trade) > 60 else ""
    return (
        f"🧾 Fees / Zero-fee {s.get('bot_version', 'v0064')}\n\n"
        "Мгновенный cached-отчёт без тяжёлого API-запроса.\n"
        "Кнопка Fees больше не зависает на полной перепроверке биржи.\n\n"
        f"Zero-fee cache total: {len(zeros)}\n"
        f"Trade universe cached: {len(trade)}\n"
        f"Blocked: {getattr(st, 'zero_fee_blocked_count', 0) if st else 0}\n"
        f"Ignored: {len(ignored)}\n"
        f"Fee guard: {'ON' if s.get('require_contract_zero_fee_on_entry') else 'OFF'}\n"
        f"Only zero-fee: {'ON' if s.get('only_zero_fee') else 'OFF'}\n\n"
        f"Пары:\n{first}{more}\n\n"
        "Глубокая API-перепроверка будет отдельной командой, не на основной кнопке."
    )[:3900]


async def fees_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    engine = await ensure_engine(context, chat_id)
    await send_direct_message(context, chat_id, fees_cached_text(engine), timeout=8.0)


async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    data = q.data or ""
    chat_id = q.message.chat_id if q.message else None
    # Answer first so Telegram button spinner disappears immediately.
    try:
        if data == "mm:stop":
            await q.answer("⏸ Stop принят")
        elif data == "mm:close_all":
            await q.answer("❌ Close All запущен")
        elif data == "mm:start":
            await q.answer("▶️ Start Tsunami принят")
        else:
            await q.answer()
    except TelegramError:
        pass
    engine = await ensure_engine(context, chat_id)
    if data == "menu:main":
        await edit_query_as_panel(q, panel_text(engine), main_menu(), mode="main")
        return
    if data == "menu:settings":
        await edit_query_as_panel(q, settings_text(), settings_menu(), mode="settings")
        return
    if data == "menu:symbols":
        await edit_query_as_panel(q, symbols_text(engine), symbols_menu(), mode="symbols")
        return
    if data == "menu:api":
        await edit_query_as_panel(q, api_text(), main_menu(), mode="api")
        return
    if data == "mm:start":
        if engine.is_running():
            await send_direct_message(context, chat_id, "▶️ Start: бот уже RUNNING. Обновляю панель снизу.", timeout=6.0)
            await upsert_panel(context, chat_id, panel_text(engine), main_menu(), mode="main", recreate=True)
            return
        await send_direct_message(context, chat_id, "▶️ Start принят v0064. Запускаю цикл, результат будет отдельным сообщением.", timeout=6.0)
        spawn_ui_task(start_engine_direct(context, chat_id, engine), name="ui_start_live")
        return
    if data == "mm:stop":
        # Stop is a hard pause only: no order/position cleanup here.
        engine.running = False
        STORE.set("live_enabled", False)
        if engine.task and not engine.task.done():
            engine.task.cancel()
        for t in list(engine.active_tasks.values()):
            if not t.done():
                t.cancel()
        engine.active_tasks.clear()
        await send_direct_message(context, chat_id, "⏸ Stop/Pause принят v0064. Ордера/позиции НЕ трогаю.", timeout=6.0)
        spawn_ui_task(stop_engine_direct(context, chat_id, engine), name="ui_stop_live")
        return
    if data == "mm:close_all":
        engine.running = False
        STORE.set("live_enabled", False)
        await edit_query_as_panel(q, "❌ Close All принят. Закрытие/отмена запущены в фоне...\n\n" + panel_text(engine), main_menu(), mode="main")
        spawn_ui_task(_finish_panel_task(context, chat_id, engine, engine.close_all(), main_menu(), mode="main", timeout_sec=180.0), name="ui_close_all")
        return
    if data == "mm:status":
        await edit_query_as_panel(q, panel_text(engine), main_menu(), mode="main")
        return
    if data == "mm:scan":
        await send_direct_message(context, chat_id, "🔍 Price Scan v0064: запускаю read-only скан отдельным сообщением...", timeout=5.0)
        spawn_ui_task(finish_direct_task(context, chat_id, engine.scan_now_text(), "Price Scan", timeout_sec=60.0), name="ui_scan_now_direct")
        return
    if data == "mm:mirror":
        await send_direct_message(context, chat_id, engine.mirror_lab_report_text(), timeout=10.0)
        await edit_query_as_panel(
            q,
            "🪞 Mirror Lab report отправлен отдельным сообщением.\n\n"
            "Команды:\n/mirror_test start\n/mirror_test report\n/mirror_test stop\n/mirror_test clear",
            main_menu(),
            mode="main",
        )
        return
    if data == "mm:balance":
        if chat_id:
            await tg_wait(context.bot.send_message(chat_id=chat_id, text=(await balance_text(engine))[:3900]))
        return
    if data == "mm:trades":
        await edit_query_as_panel(q, engine.trades_counter_text(), main_menu(), mode="main")
        return
    if data == "mm:fees":
        await send_direct_message(context, chat_id, fees_cached_text(engine), timeout=8.0)
        return
    if data == "symbols:clear":
        STORE.set("allowed_symbols", "")
        await edit_query_as_panel(q, symbols_text(engine), symbols_menu(), mode="symbols")
        return
    if data == "ignore:clear":
        msg = engine.clear_ignored_symbols()
        await edit_query_as_panel(q, msg + "\n\n" + symbols_text(engine), symbols_menu(), mode="symbols")
        return
    if data == "preset:plus":
        apply_plus_profile()
        reset_engine_signal_state(engine)
        engine.clear_ignored_symbols()
        await edit_query_as_panel(q, "🧺 Price Tsunami v0064 применён.\n\n" + settings_text(), settings_menu(), mode="settings")
        return
    if data == "preset:custom":
        STORE.set("trade_profile", "custom")
        await edit_query_as_panel(q, "✅ Custom mode включён.\n\n" + settings_text(), settings_menu(), mode="settings")
        return
    if data.startswith("preset:spread:"):
        _, _, mn, mx = data.split(":", 3)
        STORE.update({"min_spread_ticks": int(float(mn)), "max_spread_ticks": int(float(mx))})
        await edit_query_as_panel(q, symbols_text(engine), symbols_menu(), mode="symbols")
        return
    if data.startswith("toggle:"):
        key = data.split(":", 1)[1]
        s = STORE.load()
        STORE.set(key, not bool(s.get(key)))
        if key in {"auto_select_symbols", "allow_manual_fee_fallback", "only_zero_fee", "ws_depth_enabled"}:
            await edit_query_as_panel(q, symbols_text(engine), symbols_menu(), mode="symbols")
        else:
            await edit_query_as_panel(q, settings_text(), settings_menu(), mode="settings")
        return
    if data.startswith("set:wave_market_signal_mode:"):
        _, key, raw = data.split(":", 2)
        value = normalize_market_mode(raw) or raw
        if value not in {"all_zero_total", "top10_leaders"}:
            await edit_query_as_panel(q, "❌ market mode: используй all или top10", settings_menu(), mode="settings")
            return
        STORE.set(key, value)
        reset_engine_signal_state(engine)
        return_mode = panel_mode_for_signal_return()
        if return_mode == "symbols":
            await edit_query_as_panel(q, symbols_text(engine), symbols_menu(), mode="symbols")
        else:
            await edit_query_as_panel(q, settings_text(), settings_menu(), mode="settings")
        return
    if data.startswith("set:"):
        _, key, raw = data.split(":", 2)
        old = DEFAULTS.get(key)
        try:
            if isinstance(old, bool):
                value: Any = raw.lower() in {"1", "true", "yes", "on"}
            elif isinstance(old, int):
                value = int(float(raw))
            elif isinstance(old, float):
                value = float(raw)
            else:
                value = raw
            STORE.set(key, value)
            if key in {"scan_interval_sec", "max_zero_fee_scan_symbols", "zero_fee_rescan_sec", "zero_fee_universe_max_symbols", "min_depth_usdt", "min_depth_multiplier", "switch_score_improvement_pct", "min_spread_ticks", "max_spread_ticks", "min_imbalance_ratio", "min_trade_score", "entry_recheck_ms", "entry_recheck_required", "entry_recheck_count", "cooldown_after_loss_sec", "cooldown_after_trade_sec", "market_data_mode", "ws_depth_enabled", "ws_depth_max_symbols", "ws_book_stale_ms"}:
                await edit_query_as_panel(q, symbols_text(engine), symbols_menu(), mode="symbols")
            else:
                await edit_query_as_panel(q, settings_text(), settings_menu(), mode="settings")
        except Exception as e:
            await edit_query_as_panel(q, f"❌ {e}", settings_menu(), mode="settings")
        return


async def post_init(app: Application) -> None:
    global ENGINE, PANEL_UPDATE_TASK
    # v0064: force sane Telegram UX. Commands must remain visible and answer directly.
    try:
        STORE.update({
            "telegram_delete_command_messages": False,
            "telegram_reply_keyboard": False,
            "telegram_reply_keyboard_delete_hint": False,
        })
    except Exception:
        pass
    log_event("telegram_post_init", version=STORE.load().get("bot_version"), instance_id=BOT_INSTANCE_ID, pid=os.getpid())
    ENGINE = MicroMakerEngine(STORE)
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "Открыть live-панель"),
            BotCommand("ping", "Отклик, память, uptime, версия"),
            BotCommand("balance", "Баланс USDT и позиции"),
            BotCommand("status", "Полный статус бота"),
            BotCommand("trades", "Счётчик сделок"),
            BotCommand("log_full", "Быстрый текстовый лог"),
            BotCommand("log_tail", "Хвост лога текстом"),
            BotCommand("fees", "Cached zero-fee статус"),
            BotCommand("help", "Справка"),
            BotCommand("preset", "Plus/custom профиль"),
            BotCommand("market_mode", "all или top10 режим сигнала рынка"),
            BotCommand("mirror_test", "Mirror Lab: start/report/stop/clear"),
            BotCommand("mirror_report", "Mirror Lab отчёт"),
            BotCommand("clear_ignored", "Очистить ignored-лист"),
            BotCommand("doctor", "Диагностика зависаний/версии"),
            BotCommand("panel_reset", "Создать новую live-панель"),
        ])
    except TelegramError:
        pass
    PANEL_UPDATE_TASK = asyncio.create_task(live_panel_loop(app), name="telegram_live_panel_loop")


async def post_shutdown(app: Application) -> None:
    global PANEL_UPDATE_TASK
    if PANEL_UPDATE_TASK and not PANEL_UPDATE_TASK.done():
        PANEL_UPDATE_TASK.cancel()
        try:
            await PANEL_UPDATE_TASK
        except asyncio.CancelledError:
            pass


_SINGLE_INSTANCE_LOCK = None


def acquire_single_instance_lock() -> None:
    """Prevent two bot versions with the same Telegram token from running on one host.

    Seeing v0049 and v0055 alternating in Telegram almost always means two old
    processes/containers are polling the same bot token. This lock stops that
    on a single server; if the platform runs multiple dynos/containers, scale to
    exactly one worker as well.
    """
    global _SINGLE_INSTANCE_LOCK
    if fcntl is None:
        return
    path = os.getenv("BOT_SINGLE_INSTANCE_LOCK", "/tmp/mexc_micro_maker_bot.lock")
    fh = open(path, "w")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError(
            "Another mexc_micro_maker_bot process is already running. "
            "Kill old v0049/v0055 processes or scale workers to 1 before starting this version."
        )
    fh.write(f"pid={os.getpid()} instance={BOT_INSTANCE_ID}\n")
    fh.flush()
    _SINGLE_INSTANCE_LOCK = fh



async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """v0064: Telegram handler errors must be visible in logs and not kill polling."""
    try:
        log_error("telegram_application_error", context.error or Exception("unknown telegram error"), update=str(update)[:500])
    except Exception:
        pass
    try:
        chat_id = None
        if isinstance(update, Update) and update.effective_chat:
            chat_id = update.effective_chat.id
        if chat_id:
            await tg_wait(context.bot.send_message(chat_id=chat_id, text=f"❌ Ошибка обработчика: {str(context.error)[:900]}"), timeout=8.0)
    except Exception:
        pass

def main() -> None:
    acquire_single_instance_lock()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN env is missing")
    app = ApplicationBuilder().token(token).post_init(post_init).post_shutdown(post_shutdown).concurrent_updates(True).build()
    app.add_error_handler(telegram_error_handler)
    app.add_handler(CommandHandler("start", admin_guard(start_cmd)))
    app.add_handler(CommandHandler("menu", admin_guard(start_cmd)))
    app.add_handler(CommandHandler("ping", admin_guard(ping_cmd)))
    app.add_handler(CommandHandler("balance", admin_guard(balance_cmd)))
    app.add_handler(CommandHandler("status", admin_guard(status_cmd)))
    app.add_handler(CommandHandler("scan", admin_guard(scan_cmd)))
    app.add_handler(CommandHandler("trades", admin_guard(trades_cmd)))
    app.add_handler(CommandHandler("log_full", admin_guard(log_full_cmd)))
    app.add_handler(CommandHandler("fees", admin_guard(fees_cmd)))
    app.add_handler(CommandHandler("log", admin_guard(log_full_cmd)))
    app.add_handler(CommandHandler("log_tail", admin_guard(log_tail_cmd)))
    app.add_handler(CommandHandler("help", admin_guard(help_cmd)))
    app.add_handler(CommandHandler("panel", admin_guard(panel_cmd)))
    app.add_handler(CommandHandler("api", admin_guard(api_cmd)))
    app.add_handler(CommandHandler("preset", admin_guard(preset_cmd)))
    app.add_handler(CommandHandler("set", admin_guard(set_cmd)))
    app.add_handler(CommandHandler("symbols", admin_guard(symbols_cmd)))
    app.add_handler(CommandHandler("market_mode", admin_guard(market_mode_cmd)))
    app.add_handler(CommandHandler("mirror_test", admin_guard(mirror_test_cmd)))
    app.add_handler(CommandHandler("mirror_start", admin_guard(mirror_test_cmd)))
    app.add_handler(CommandHandler("mirror_report", admin_guard(mirror_test_cmd)))
    app.add_handler(CommandHandler("mirror_stop", admin_guard(mirror_test_cmd)))
    app.add_handler(CommandHandler("mirror_clear", admin_guard(mirror_test_cmd)))
    app.add_handler(CommandHandler("ignore", admin_guard(ignore_cmd)))
    app.add_handler(CommandHandler("clear_ignored", admin_guard(clear_ignored_cmd)))
    app.add_handler(CommandHandler("doctor", admin_guard(doctor_cmd)))
    app.add_handler(CommandHandler("panel_reset", admin_guard(panel_reset_cmd)))
    app.add_handler(CommandHandler("close_all", admin_guard(close_all_cmd)))
    app.add_handler(CommandHandler("closeall", admin_guard(close_all_cmd)))
    app.add_handler(CallbackQueryHandler(admin_guard(callback)))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_guard(api_plaintext_cmd)))
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True, close_loop=False)


if __name__ == "__main__":
    main()
