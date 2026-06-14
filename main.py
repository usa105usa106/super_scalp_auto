from __future__ import annotations

import asyncio
import os
import time
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
UI_BG_TASKS: set[asyncio.Task] = set()


def spawn_ui_task(coro, name: str = "ui_bg") -> asyncio.Task:
    """Run slow Telegram/API actions outside the callback handler so buttons do not stick."""
    task = asyncio.create_task(coro, name=name)
    UI_BG_TASKS.add(task)
    task.add_done_callback(lambda t: UI_BG_TASKS.discard(t))
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
        await handler(update, context)
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
    ])



def command_keyboard() -> ReplyKeyboardMarkup:
    """Ordinary Telegram reply keyboard, separate from inline panel buttons."""
    return ReplyKeyboardMarkup(
        [["/start", "/ping"], ["/balance", "/status"], ["/trades", "/log_full"], ["/help"]],
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
    """Shows ordinary command buttons without replacing the inline live panel.

    Telegram does not allow inline keyboard and reply keyboard on the same message,
    so we send a tiny helper message with ReplyKeyboardMarkup. By default it is
    deleted after a moment; the bot command menu is also registered in post_init.
    """
    s = STORE.load()
    if not bool(s.get("telegram_reply_keyboard")):
        return
    try:
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text="⌨️ Меню команд включено: /start /ping /balance /status /trades /log_full /help",
            reply_markup=command_keyboard(),
        )
        if bool(s.get("telegram_reply_keyboard_delete_hint")):
            asyncio.create_task(delete_later(context, chat_id, msg.message_id, delay=1.5))
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
        f"⚙️ Price Tsunami Settings ({s.get('bot_version', 'v0046')})\n\n"
        f"Active scan: ALL zero-fee *_USDT монеты | lookback {s.get('wave_price_lookback_sec')}s | WS ALL symbols\n"
        f"Votes: price up = LONG, price down = SHORT, flat/no fresh price = NEUTRAL\n"
        f"Denominator: проценты считаются от всего выбранного universe; монеты без свежей цены не выкидываются, а идут в NEUTRAL.\n"
        f"Early Wave: текущие >= {float(s.get('wave_early_min_side_ratio') or 0.65):.0%} и эта же сторона выросла на +{float(s.get('wave_accel_trigger_pct') or 15):.0f}п.п. за {float(s.get('wave_accel_lookback_sec') or 60):.0f}s → 5 сделок, 5x, NET +${float(s.get('wave_normal_target_profit_usdt') or 0.05):.2f}\n"
        f"Normal Wave: текущие >= {float(s.get('wave_min_side_ratio') or 0.75):.0%} → 5 сделок, 5x, NET +${float(s.get('wave_normal_target_profit_usdt') or 0.05):.2f}\n"
        f"Tsunami: текущие >= {float(s.get('wave_min_side_ratio') or 0.75):.0%} и эта же сторона выросла на +{float(s.get('wave_accel_trigger_pct') or 15):.0f}п.п. → 5 сделок, 10x, NET +${float(s.get('wave_tsunami_target_profit_usdt') or 0.10):.2f}\n"
        "Важно: 65%/75% — это текущее итоговое значение; +15п.п. уже внутри него, это не 65+15.\n\n"
        f"Basket: {s.get('wave_positions')} slots | size {s.get('position_margin_percent')}% equity на слот | isolated\n"
        f"Pick range: middle {float(s.get('wave_pick_start_pct') or 0.25):.0%}-{float(s.get('wave_pick_end_pct') or 0.60):.0%} of same-side candidates\n"
        f"Open retry: 5 сделок открываются с паузой {float(s.get('wave_open_batch_gap_ms') or 0)/1000:.1f}s; при MEXC rate-limit бот ждёт {float(s.get('wave_open_retry_delay_sec') or 0):.1f}s и повторяет до {s.get('wave_open_retry_rounds')} раз, потом добирает replacement.\n"
        f"After 10m: close only zero/microplus; if minus, wait recovery.\n"
        f"Stop: pause only. Close All: cancel orders + close positions.\n\n"
        "Команды: /set size 20, /set candidates 0, /set ws_symbols 0, /set panel_sec 5, /symbols clear"
    )

def settings_menu() -> InlineKeyboardMarkup:
    s = STORE.load()
    return InlineKeyboardMarkup([
        [b(("✅ " if s.get('wave_normal_leverage') == 5 else "") + "5x normal", "set:wave_normal_leverage:5"), b(("✅ " if s.get('wave_tsunami_leverage') == 10 else "") + "10x tsunami", "set:wave_tsunami_leverage:10")],
        [b("✅ Basket 5", "set:wave_positions:5"), b("Scan ALL", "set:max_zero_fee_scan_symbols:0"), b("WS ALL", "set:ws_depth_max_symbols:0")],
        [b("Early 65%", "set:wave_early_min_side_ratio:0.65"), b("Normal 75%", "set:wave_min_side_ratio:0.75"), b("Ускор. 15п.п.", "set:wave_accel_trigger_pct:15")],
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
        [b("🌊 Price Tsunami Basket v0046", "preset:plus"), b("Custom mode", "preset:custom")],
        [b("⬅️ Back to Live", "menu:main")],
    ])


def symbols_text() -> str:
    s = STORE.load()
    syms = parse_symbols(str(s.get("allowed_symbols") or ""))
    allowed = ", ".join(syms) if syms else "FULL AUTO: все API-confirmed zero-fee пары"
    ignored = s.get("ignored_symbols") or {}
    ignored_count = len(ignored) if isinstance(ignored, dict) else 0
    universe_limit = int(s.get("zero_fee_universe_max_symbols") or 0)
    universe_txt = "ALL" if universe_limit <= 0 else str(universe_limit)
    return (
        "📈 Scanner / Symbols\n\n"
        f"Auto-select: {'ON' if s.get('auto_select_symbols') else 'OFF'}\n"
        f"Only zero fee: {'ON' if s.get('only_zero_fee') else 'OFF'}\n"
        f"Manual fee fallback: {'ON' if s.get('allow_manual_fee_fallback') else 'OFF'}\n"
        f"Scan interval: {s.get('scan_interval_sec')} sec\n"
        f"Zero-fee universe rescan: {s.get('zero_fee_rescan_sec')} sec\n"
        f"Zero-fee universe cap: {universe_txt} | Active price scan cap: {('ALL' if int(s.get('max_zero_fee_scan_symbols') or 0) <= 0 else s.get('max_zero_fee_scan_symbols'))}\n"
        f"Ignored symbols: {ignored_count}\n"
        f"Min spread: {s.get('min_spread_ticks')} ticks | Max spread: {s.get('max_spread_ticks')} ticks\n"
        f"Min imbalance: {s.get('min_imbalance_ratio')} | Min score: {s.get('min_trade_score')}\n"
        f"Min depth: max({s.get('min_depth_usdt')}$, position_notional × {s.get('min_depth_multiplier')})\n"
        f"Switch threshold: +{s.get('switch_score_improvement_pct')}% | Min hold: {s.get('min_symbol_hold_sec')} sec\n\n"
        f"Allowed symbols / whitelist:\n{allowed}\n\n"
        "Zero-fee cache живёт только внутри запуска, ignored cache чистится при рестарте/редеплое.\n"
        "Плохие монеты могут попасть в ignore только на текущую сессию: region restricted / unsupported / min-max margin-volume rejects.\n\n"
        "Задать whitelist: /symbols LINK_USDT,SOL_USDT\n"
        "Очистить whitelist и вернуть FULL AUTO: /symbols clear"
    )


def symbols_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [b("Auto-select ON/OFF", "toggle:auto_select_symbols"), b("ZeroFee ON/OFF", "toggle:only_zero_fee")],
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


def panel_text(engine: MicroMakerEngine | None = None) -> str:
    e = engine or ENGINE
    if e:
        return e.quick_status_text()
    s = STORE.load()
    return (
        f"🌊 Price Tsunami {s.get('bot_version', 'v0046')}\n"
        "State: STOPPED\n\n"
        "PRICE SCAN 10s: пока нет данных.\n"
        "LONG 0% | SHORT 0% | NEUTRAL 0%\n"
        "Вывод: сидим в засаде, сделки не открываем.\n\n"
        "Правила:\n"
        "Early: сейчас >=65% и эта же сторона выросла на +15п.п. за 60s → 5 сделок, 5x, NET +$0.05\n"
        "Normal: сейчас >=75% стороны → 5 сделок, 5x, NET +$0.05\n"
        "Tsunami: сейчас >=75% и эта же сторона выросла на +15п.п. за 60s → 5 сделок, 10x, NET +$0.10\n"
        "65/75 — итог сейчас; +15п.п. уже внутри этих процентов.\n\n"
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
                await context.bot.delete_message(chat_id=old_chat_id, message_id=old_message_id)
            except TelegramError:
                pass
            old_message_id = 0
        if old_chat_id == chat_id and old_message_id:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=old_message_id,
                    text=text[:3900],
                    reply_markup=reply_markup,
                )
                await set_panel_identity(chat_id, old_message_id, mode)
                return
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    await set_panel_identity(chat_id, old_message_id, mode)
                    return
                # fall through and create a fresh panel if the old one disappeared / is not editable
            except TelegramError:
                pass
        msg = await context.bot.send_message(chat_id=chat_id, text=text[:3900], reply_markup=reply_markup)
        await set_panel_identity(chat_id, msg.message_id, mode)


async def edit_query_as_panel(q, text: str, reply_markup: InlineKeyboardMarkup, mode: str = "main") -> None:
    if q.message:
        try:
            await q.edit_message_text(text[:3900], reply_markup=reply_markup)
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
    if not chat_id or not message_id:
        return
    async with PANEL_LOCK:
        # Re-read inside the lock in case a menu callback changed mode.
        s2 = STORE.load()
        if str(s2.get("telegram_panel_mode") or "main") != "main" and not force:
            return
        try:
            await app.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=panel_text()[:3900],
                reply_markup=main_menu(),
            )
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                pass
        except (Forbidden, TelegramError):
            pass


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
    await install_command_keyboard(context, chat_id)
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
    await install_command_keyboard(context, chat_id)
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
    engine.clear_ignored_symbols()
    await upsert_panel(context, chat_id, "🌊 Price Tsunami v0046 применён: 10s price-scan, итоговые 65/75% + рост 15п.п., 5 LONG/SHORT, 5x/10x, REAL NET выход.\n\n" + settings_text(), settings_menu(), mode="settings")


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
        if isinstance(old, bool):
            val: Any = raw.lower() in {"1", "true", "yes", "on", "да", "вкл"}
        elif isinstance(old, int):
            val = int(float(raw))
        elif isinstance(old, float):
            val = float(raw)
        else:
            val = raw
        STORE.set(key, val)
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
    raw = " ".join(context.args or []).strip()
    if raw.lower() in {"clear", "auto", "all", "*"}:
        STORE.set("allowed_symbols", "")
        await upsert_panel(context, chat_id, "✅ Whitelist очищен. Включён FULL AUTO.\n\n" + symbols_text(), symbols_menu(), mode="symbols")
        return
    syms = parse_symbols(raw)
    if not syms:
        await upsert_panel(context, chat_id, symbols_text(), symbols_menu(), mode="symbols")
        return
    STORE.set("allowed_symbols", ",".join(syms))
    await upsert_panel(context, chat_id, "✅ Whitelist updated:\n" + ", ".join(syms) + "\n\n" + symbols_text(), symbols_menu(), mode="symbols")


async def ignore_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    engine = await ensure_engine(context, chat_id)
    args = [a.lower() for a in (context.args or [])]
    if args and args[0] in {"clear", "reset", "0"}:
        msg = engine.clear_ignored_symbols()
        await upsert_panel(context, chat_id, msg + "\n\n" + symbols_text(), symbols_menu(), mode="symbols")
        return
    await upsert_panel(context, chat_id, engine.ignored_symbols_text(), symbols_menu(), mode="symbols")


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
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    await install_command_keyboard(context, chat_id)
    text = ping_text(update, started)
    await upsert_panel(context, chat_id, text + "\n\n" + panel_text(), main_menu(), mode="main")


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
    await context.bot.send_message(chat_id=chat_id, text=(await balance_text(engine))[:3900])


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    await install_command_keyboard(context, chat_id)
    engine = await ensure_engine(context, chat_id)
    try:
        # Try to initialize API client so status can show balance/positions when keys exist.
        try:
            await engine._ensure_client()
        except Exception:
            pass
        txt = await engine.status_text()
    except Exception as e:
        txt = f"❌ Status error: {str(e)[:500]}"
    await upsert_panel(context, chat_id, txt, main_menu(), mode="main")


async def trades_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    await install_command_keyboard(context, chat_id)
    engine = await ensure_engine(context, chat_id)
    args = [a.lower() for a in (context.args or [])]
    if args and args[0] in {"reset", "clear", "0"}:
        STORE.update({
            "total_trades_count": 0,
            "total_wins_count": 0,
            "total_losses_count": 0,
            "total_estimated_pnl_usdt": 0.0,
        })
        await upsert_panel(context, chat_id, "✅ Total trade counter reset.\n\n" + engine.trades_counter_text(), main_menu(), mode="main")
        return
    await upsert_panel(context, chat_id, engine.trades_counter_text(), main_menu(), mode="main")


async def log_full_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    await install_command_keyboard(context, chat_id)
    engine = await ensure_engine(context, chat_id)
    args = [a.lower() for a in (context.args or [])]
    if args and args[0] in {"clear", "reset", "0"}:
        clear_full_log()
        log_event("log_full_cleared_by_user", chat_id=chat_id)
        await upsert_panel(context, chat_id, "✅ log_full очищен. Новый лог начнёт писаться сразу после следующего действия бота.", main_menu(), mode="main")
        return
    try:
        log_event("log_full_export_requested", chat_id=chat_id)
        path = export_full_log(STORE.load(), engine)
        caption = f"📄 Full debug log {STORE.load().get('bot_version', 'v0028')}\nОшибки, скан монет, переключения, ордера, закрытия и MEXC API-события."
        with open(path, "rb") as f:
            await context.bot.send_document(
                chat_id=chat_id,
                document=f,
                filename=Path(path).name,
                caption=caption[:1000],
            )
        await upsert_panel(context, chat_id, "✅ /log_full отправил .txt файл с полным логом.\n\n" + panel_text(engine), main_menu(), mode="main")
    except Exception as e:
        log_error("log_full_export_error", e, chat_id=chat_id)
        await upsert_panel(context, chat_id, f"❌ log_full error: {str(e)[:800]}", main_menu(), mode="main")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_delete_message(context, update)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
        return
    await install_command_keyboard(context, chat_id)
    s = STORE.load()
    txt = (
        f"🆘 Price Tsunami Help — {s.get('bot_version', 'v0046')}\n\n"
        "Логика торговли:\n"
        "1) Бот держит ALL active zero-fee *_USDT universe, без лимита 250.\n"
        "2) Каждые ~10 секунд сравнивает mid-price каждой монеты.\n"
        "3) Считает рынок: LONG %, SHORT %, NEUTRAL %. Проценты от всего universe; если по монете нет свежей цены/истории — она считается NEUTRAL, а не пропадает из знаменателя.\n"
        "4) Если перевес слабый — ничего не открывает.\n\n"
        "Режимы входа:\n"
        "Early Wave: сейчас >=65% одной стороны и эта же сторона выросла на +15п.п. за 60s → 5 сделок, 5x, REAL NET TP +$0.05.\n"
        "Normal Wave: сейчас >=75% одной стороны → 5 сделок, 5x, REAL NET TP +$0.05.\n"
        "Tsunami: сейчас >=75% и эта же сторона выросла на +15п.п. за 60s → 5 сделок, 10x, REAL NET TP +$0.10.\n"
        "Важно: 65% и 75% — текущий итоговый процент; +15п.п. уже внутри этого значения, это не 65+15.\n\n"
        "Выбор монет: не самый перегретый топ, а середина 25-60% same-side candidates.\n"
        "Все сделки открываются одной стороной: либо 5 LONG, либо 5 SHORT. Если MEXC режет быстрые заявки, бот ждёт и повторяет те же слоты, затем добирает заменами.\n"
        "Закрытие: вся корзина по REAL NET equity PnL. Через 10 минут закрывает только ноль/микроплюс; минус не режет, ждёт восстановления.\n\n"
        "Кнопки:\n"
        "▶️ Start Tsunami — запустить режим.\n"
        "⏸ Stop/Pause — только пауза, позиции и ордера не трогает.\n"
        "❌ Close All — отменяет ордера и закрывает позиции market.\n"
        "🔍 Price Scan — показать текущие LONG/SHORT/NEUTRAL и вывод.\n\n"
        "Команды: /api set KEY SECRET, /balance, /status, /log_full, /symbols clear, /set size 20, /set candidates 0."
    )
    await upsert_panel(context, chat_id, txt, main_menu(), mode="main")


async def _finish_panel_task(context: ContextTypes.DEFAULT_TYPE, chat_id: int | None, engine: MicroMakerEngine, text_or_coro, reply_markup: InlineKeyboardMarkup | None = None, mode: str = "main") -> None:
    """Execute a slow action and refresh panel afterwards. Used so button callbacks answer instantly."""
    if not chat_id:
        return
    try:
        if asyncio.iscoroutine(text_or_coro):
            msg = await text_or_coro
        else:
            msg = str(text_or_coro)
        await upsert_panel(context, chat_id, (msg + "\n\n" + panel_text(engine))[:3900], reply_markup or main_menu(), mode=mode)
    except Exception as e:
        log_error("telegram_background_panel_task_error", e, mode=mode)
        try:
            await upsert_panel(context, chat_id, f"❌ Ошибка фоновой команды: {str(e)[:500]}\n\n" + panel_text(engine), reply_markup or main_menu(), mode=mode)
        except Exception:
            pass


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
        await edit_query_as_panel(q, symbols_text(), symbols_menu(), mode="symbols")
        return
    if data == "menu:api":
        await edit_query_as_panel(q, api_text(), main_menu(), mode="api")
        return
    if data == "mm:start":
        await edit_query_as_panel(q, "▶️ Start принят, запускаю цикл...\n\n" + panel_text(engine), main_menu(), mode="main")
        spawn_ui_task(_finish_panel_task(context, chat_id, engine, engine.start(), main_menu(), mode="main"), name="ui_start_live")
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
        await edit_query_as_panel(q, "⏸ Stop принят мгновенно. Скан/новые сделки остановлены. Ордера и позиции НЕ трогаю.\n\n" + panel_text(engine), main_menu(), mode="main")
        spawn_ui_task(_finish_panel_task(context, chat_id, engine, engine.stop(close_positions=False), main_menu(), mode="main"), name="ui_stop_live")
        return
    if data == "mm:close_all":
        engine.running = False
        STORE.set("live_enabled", False)
        await edit_query_as_panel(q, "❌ Close All принят. Закрытие/отмена запущены в фоне...\n\n" + panel_text(engine), main_menu(), mode="main")
        spawn_ui_task(_finish_panel_task(context, chat_id, engine, engine.close_all(), main_menu(), mode="main"), name="ui_close_all")
        return
    if data == "mm:status":
        await edit_query_as_panel(q, panel_text(engine), main_menu(), mode="main")
        return
    if data == "mm:scan":
        await edit_query_as_panel(q, "🔍 Scan Now принят, сканирую в фоне...", symbols_menu(), mode="symbols")
        spawn_ui_task(_finish_panel_task(context, chat_id, engine, engine.scan_now_text(), symbols_menu(), mode="symbols"), name="ui_scan_now")
        return
    if data == "mm:balance":
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text=(await balance_text(engine))[:3900])
        return
    if data == "mm:trades":
        await edit_query_as_panel(q, engine.trades_counter_text(), main_menu(), mode="main")
        return
    if data == "mm:fees":
        async def fee_task():
            s = STORE.load()
            client = MexcFuturesClient(s.get("mexc_api_key"), s.get("mexc_api_secret"), settings=s)
            await client.sync_time()
            zeros = await client.verified_zero_fee_symbols(int(s.get("zero_fee_universe_max_symbols") or 0))
            return "🧾 API zero-fee symbols:\n" + (", ".join(zeros) if zeros else "Не нашёл API-подтверждённые zero-fee пары.")
        await edit_query_as_panel(q, "🧾 Fees принят, проверяю API в фоне...", main_menu(), mode="api")
        spawn_ui_task(_finish_panel_task(context, chat_id, engine, fee_task(), main_menu(), mode="api"), name="ui_fees")
        return
    if data == "symbols:clear":
        STORE.set("allowed_symbols", "")
        await edit_query_as_panel(q, symbols_text(), symbols_menu(), mode="symbols")
        return
    if data == "ignore:clear":
        msg = engine.clear_ignored_symbols()
        await edit_query_as_panel(q, msg + "\n\n" + symbols_text(), symbols_menu(), mode="symbols")
        return
    if data == "preset:plus":
        apply_plus_profile()
        engine.clear_ignored_symbols()
        await edit_query_as_panel(q, "🧺 Price Tsunami v0046 применён.\n\n" + settings_text(), settings_menu(), mode="settings")
        return
    if data == "preset:custom":
        STORE.set("trade_profile", "custom")
        await edit_query_as_panel(q, "✅ Custom mode включён.\n\n" + settings_text(), settings_menu(), mode="settings")
        return
    if data.startswith("preset:spread:"):
        _, _, mn, mx = data.split(":", 3)
        STORE.update({"min_spread_ticks": int(float(mn)), "max_spread_ticks": int(float(mx))})
        await edit_query_as_panel(q, symbols_text(), symbols_menu(), mode="symbols")
        return
    if data.startswith("toggle:"):
        key = data.split(":", 1)[1]
        s = STORE.load()
        STORE.set(key, not bool(s.get(key)))
        if key in {"auto_select_symbols", "allow_manual_fee_fallback", "only_zero_fee", "ws_depth_enabled"}:
            await edit_query_as_panel(q, symbols_text(), symbols_menu(), mode="symbols")
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
                await edit_query_as_panel(q, symbols_text(), symbols_menu(), mode="symbols")
            else:
                await edit_query_as_panel(q, settings_text(), settings_menu(), mode="settings")
        except Exception as e:
            await edit_query_as_panel(q, f"❌ {e}", settings_menu(), mode="settings")
        return


async def post_init(app: Application) -> None:
    global ENGINE, PANEL_UPDATE_TASK
    log_event("telegram_post_init", version=STORE.load().get("bot_version"))
    ENGINE = MicroMakerEngine(STORE)
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "Открыть live-панель"),
            BotCommand("ping", "Отклик, память, uptime, версия"),
            BotCommand("balance", "Баланс USDT и позиции"),
            BotCommand("status", "Полный статус бота"),
            BotCommand("trades", "Счётчик сделок"),
            BotCommand("log_full", "Полный .txt лог для диагностики"),
            BotCommand("help", "Справка"),
            BotCommand("preset", "Plus/custom профиль"),
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


def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN env is missing")
    app = ApplicationBuilder().token(token).post_init(post_init).post_shutdown(post_shutdown).concurrent_updates(True).build()
    app.add_handler(CommandHandler("start", admin_guard(start_cmd)))
    app.add_handler(CommandHandler("menu", admin_guard(start_cmd)))
    app.add_handler(CommandHandler("ping", admin_guard(ping_cmd)))
    app.add_handler(CommandHandler("balance", admin_guard(balance_cmd)))
    app.add_handler(CommandHandler("status", admin_guard(status_cmd)))
    app.add_handler(CommandHandler("trades", admin_guard(trades_cmd)))
    app.add_handler(CommandHandler("log_full", admin_guard(log_full_cmd)))
    app.add_handler(CommandHandler("help", admin_guard(help_cmd)))
    app.add_handler(CommandHandler("panel", admin_guard(panel_cmd)))
    app.add_handler(CommandHandler("api", admin_guard(api_cmd)))
    app.add_handler(CommandHandler("preset", admin_guard(preset_cmd)))
    app.add_handler(CommandHandler("set", admin_guard(set_cmd)))
    app.add_handler(CommandHandler("symbols", admin_guard(symbols_cmd)))
    app.add_handler(CommandHandler("ignore", admin_guard(ignore_cmd)))
    app.add_handler(CommandHandler("close_all", admin_guard(close_all_cmd)))
    app.add_handler(CommandHandler("closeall", admin_guard(close_all_cmd)))
    app.add_handler(CallbackQueryHandler(admin_guard(callback)))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_guard(api_plaintext_cmd)))
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
