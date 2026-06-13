from __future__ import annotations

import json
import os
import traceback
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from logging.handlers import RotatingFileHandler
import logging

LOG_DIR = Path(os.getenv("MICRO_MAKER_LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
FULL_LOG_PATH = LOG_DIR / "log_full.txt"
EXPORT_DIR = LOG_DIR / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

_MAX_BYTES = int(float(os.getenv("MICRO_MAKER_FULL_LOG_MAX_MB", "25")) * 1024 * 1024)
_BACKUPS = int(os.getenv("MICRO_MAKER_FULL_LOG_BACKUPS", "3") or "3")

_logger = logging.getLogger("mexc_micro_maker.full")
_logger.setLevel(logging.DEBUG)
_logger.propagate = False
def _attach_handler() -> None:
    handler = RotatingFileHandler(FULL_LOG_PATH, maxBytes=_MAX_BYTES, backupCount=_BACKUPS, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(handler)


if not _logger.handlers:
    _attach_handler()

SENSITIVE_KEYS = {
    "api_key", "apikey", "apiKey", "mexc_api_key", "mexc_api_secret", "api_secret",
    "secret", "signature", "Signature", "token", "TELEGRAM_BOT_TOKEN", "authorization", "Authorization",
    "cookie", "set-cookie", "password", "passphrase",
}


def _iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")


def _mask_value(value: Any) -> str:
    s = str(value or "")
    if not s:
        return ""
    if len(s) <= 8:
        return "***"
    return f"{s[:4]}...{s[-4:]}"


def safe_for_log(value: Any, *, max_str: int = 5000, depth: int = 0) -> Any:
    """Return JSON-safe data with secrets masked and huge strings trimmed."""
    if depth > 8:
        return "<max_depth>"
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            key = str(k)
            if key in SENSITIVE_KEYS or any(x in key.lower() for x in ("secret", "token", "signature", "apikey", "api_key", "password")):
                out[key] = _mask_value(v)
            else:
                out[key] = safe_for_log(v, max_str=max_str, depth=depth + 1)
        return out
    if isinstance(value, (list, tuple, set)):
        return [safe_for_log(x, max_str=max_str, depth=depth + 1) for x in list(value)[:500]]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    text = str(value)
    if len(text) > max_str:
        return text[:max_str] + f"...<truncated {len(text) - max_str} chars>"
    return text


def _write(level: str, event: str, **data: Any) -> None:
    try:
        payload = {
            "ts": _iso(),
            "level": level,
            "event": str(event),
            "data": safe_for_log(data),
        }
        _logger.debug(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        # Logging must never break trading.
        pass


def log_event(event: str, **data: Any) -> None:
    _write("INFO", event, **data)


def log_debug(event: str, **data: Any) -> None:
    _write("DEBUG", event, **data)


def log_error(event: str, exc: BaseException | None = None, **data: Any) -> None:
    if exc is not None:
        data = dict(data)
        data["error_type"] = type(exc).__name__
        data["error"] = str(exc)
        data["traceback"] = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    _write("ERROR", event, **data)


def clear_full_log() -> None:
    """Clear current/rotated logs and keep the logger writable.

    Important: RotatingFileHandler keeps an open file descriptor. If we only
    unlink log_full.txt, later /log_full exports may miss new lines because the
    handler is still writing into a deleted inode. We close and recreate the
    handler atomically instead.
    """
    try:
        for handler in list(_logger.handlers):
            try:
                handler.flush()
                handler.close()
            except Exception:
                pass
            try:
                _logger.removeHandler(handler)
            except Exception:
                pass
        for p in [FULL_LOG_PATH] + sorted(LOG_DIR.glob("log_full.txt.*")):
            try:
                p.unlink()
            except FileNotFoundError:
                pass
            except Exception:
                pass
        FULL_LOG_PATH.touch(exist_ok=True)
        _attach_handler()
    except Exception:
        try:
            if not _logger.handlers:
                _attach_handler()
        except Exception:
            pass


def _read_log_files(max_bytes: int = 18 * 1024 * 1024) -> str:
    for handler in list(_logger.handlers):
        try:
            handler.flush()
        except Exception:
            pass
    files = sorted(LOG_DIR.glob("log_full.txt.*"), reverse=True) + [FULL_LOG_PATH]
    chunks: list[str] = []
    used = 0
    for path in files:
        if not path.exists():
            continue
        try:
            data = path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            data = f"<failed to read {path}: {e}>\n"
        # Keep export practical for Telegram; prefer the newest tail if it is too large.
        encoded_len = len(data.encode("utf-8", errors="replace"))
        if used + encoded_len > max_bytes:
            remain = max(0, max_bytes - used)
            if remain > 0:
                raw = data.encode("utf-8", errors="replace")[-remain:]
                chunks.append(raw.decode("utf-8", errors="replace"))
            break
        chunks.append(data)
        used += encoded_len
    return "\n".join(chunks)


def export_full_log(settings: dict[str, Any] | None = None, engine: Any | None = None) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = EXPORT_DIR / f"mexc_micro_maker_log_full_{ts}.txt"
    header: list[str] = []
    header.append("MEXC MICRO MAKER FULL DEBUG LOG")
    header.append(f"Generated: {_iso()}")
    if settings:
        header.append("\n=== CURRENT SETTINGS (secrets masked) ===")
        header.append(json.dumps(safe_for_log(settings), ensure_ascii=False, indent=2))
    if engine is not None:
        header.append("\n=== ENGINE SNAPSHOT ===")
        try:
            stats = getattr(engine, "stats", None)
            header.append(json.dumps(safe_for_log(getattr(stats, "__dict__", {})), ensure_ascii=False, indent=2))
            header.append(f"running={bool(engine.is_running())}")
            header.append(f"zero_fee_cache_count={len(getattr(engine, 'zero_fee_cache', []) or [])}")
            header.append(f"last_selected_symbols={safe_for_log(getattr(engine, 'last_selected_symbols', []))}")
            depth_ws = getattr(engine, "depth_ws", None)
            if depth_ws is not None:
                header.append("ws_stats=" + json.dumps(safe_for_log(depth_ws.stats()), ensure_ascii=False))
        except Exception as e:
            header.append(f"<engine snapshot error: {e}>")
    header.append("\n=== LOG LINES ===")
    body = _read_log_files()
    out_path.write_text("\n".join(header) + "\n" + body, encoding="utf-8")
    return out_path
