from __future__ import annotations

import json
import os
import traceback
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from logging.handlers import RotatingFileHandler
import logging

LOG_DIR = Path(os.getenv("MICRO_MAKER_LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
FULL_LOG_PATH = LOG_DIR / "log_full.txt"
EXPORT_DIR = LOG_DIR / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

_MAX_BYTES = int(float(os.getenv("MICRO_MAKER_FULL_LOG_MAX_MB", "8")) * 1024 * 1024)
_BACKUPS = int(os.getenv("MICRO_MAKER_FULL_LOG_BACKUPS", "1") or "1")
_DEFAULT_RETENTION_MIN = float(os.getenv("MICRO_MAKER_FULL_LOG_RETENTION_MINUTES", "20") or "20")
_DEFAULT_TZ_OFFSET_HOURS = float(os.getenv("MICRO_MAKER_TIME_OFFSET_HOURS", "3") or "3")
_LAST_PRUNE_TS = 0.0
_PRUNING = False

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


def _now_tz(offset_hours: float | None = None) -> datetime:
    off = _DEFAULT_TZ_OFFSET_HOURS if offset_hours is None else float(offset_hours or 0)
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=off)))


def _iso(offset_hours: float | None = None) -> str:
    return _now_tz(offset_hours).isoformat(timespec="milliseconds")


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



def _parse_line_ts(line: str) -> float | None:
    try:
        obj = json.loads(line)
        ts = obj.get("ts") if isinstance(obj, dict) else None
        if not ts:
            return None
        # Python accepts +03:00 but not a bare Z on older versions.
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _recent_lines(text: str, max_age_minutes: float) -> list[str]:
    if max_age_minutes <= 0:
        return [x for x in text.splitlines() if x.strip()]
    cutoff = time.time() - max_age_minutes * 60.0
    out: list[str] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        ts = _parse_line_ts(line)
        if ts is None or ts >= cutoff:
            out.append(line)
    return out


def prune_full_log(max_age_minutes: float | None = None, max_bytes: int | None = None) -> None:
    """Physically keep only recent log lines, so /log_full stays small.

    The handler keeps the file open, so we close/reopen around the rewrite.
    Logging must remain best-effort and must never break trading.
    """
    global _PRUNING, _LAST_PRUNE_TS
    if _PRUNING:
        return
    _PRUNING = True
    try:
        retention = _DEFAULT_RETENTION_MIN if max_age_minutes is None else float(max_age_minutes or 0)
        budget = max_bytes or _MAX_BYTES
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
        files = sorted(LOG_DIR.glob("log_full.txt.*")) + [FULL_LOG_PATH]
        lines: list[str] = []
        for path in files:
            if not path.exists():
                continue
            try:
                lines.extend(_recent_lines(path.read_text(encoding="utf-8", errors="replace"), retention))
            except Exception:
                pass
        text = "\n".join(lines)
        raw = text.encode("utf-8", errors="replace")
        if budget > 0 and len(raw) > budget:
            raw = raw[-budget:]
            text = raw.decode("utf-8", errors="replace")
            # Drop possibly cut first partial line.
            if "\n" in text:
                text = text.split("\n", 1)[1]
        for p in sorted(LOG_DIR.glob("log_full.txt.*")):
            try:
                p.unlink()
            except Exception:
                pass
        FULL_LOG_PATH.write_text((text + "\n") if text else "", encoding="utf-8")
        _LAST_PRUNE_TS = time.time()
    except Exception:
        pass
    finally:
        try:
            if not _logger.handlers:
                _attach_handler()
        except Exception:
            pass
        _PRUNING = False


def _maybe_prune_full_log() -> None:
    global _LAST_PRUNE_TS
    try:
        if _DEFAULT_RETENTION_MIN <= 0:
            return
        # Not on every tick; once a minute is enough.
        if time.time() - _LAST_PRUNE_TS >= 60.0:
            prune_full_log(_DEFAULT_RETENTION_MIN, _MAX_BYTES)
    except Exception:
        pass

def _write(level: str, event: str, **data: Any) -> None:
    try:
        _maybe_prune_full_log()
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


def _read_log_files(max_bytes: int = 8 * 1024 * 1024, max_age_minutes: float = 20.0) -> str:
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
            data = "\n".join(_recent_lines(path.read_text(encoding="utf-8", errors="replace"), max_age_minutes))
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
    tz_offset = float((settings or {}).get("telegram_time_offset_hours", _DEFAULT_TZ_OFFSET_HOURS) or _DEFAULT_TZ_OFFSET_HOURS)
    retention_min = float((settings or {}).get("full_log_retention_minutes", _DEFAULT_RETENTION_MIN) or _DEFAULT_RETENTION_MIN)
    export_max_mb = float((settings or {}).get("full_log_export_max_mb", 8.0) or 8.0)
    max_bytes = max(512 * 1024, int(export_max_mb * 1024 * 1024))
    prune_full_log(retention_min, max_bytes)
    ts = _now_tz(tz_offset).strftime("%Y%m%d_%H%M%S")
    out_path = EXPORT_DIR / f"mexc_micro_maker_log_full_{ts}.txt"
    header: list[str] = []
    header.append("MEXC MICRO MAKER FULL DEBUG LOG")
    header.append(f"Generated: {_iso(tz_offset)}")
    header.append(f"Log retention: last {retention_min:g} minutes, export max {export_max_mb:g} MB")
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
    body = _read_log_files(max_bytes=max_bytes, max_age_minutes=retention_min)
    out_path.write_text("\n".join(header) + "\n" + body, encoding="utf-8")
    return out_path
