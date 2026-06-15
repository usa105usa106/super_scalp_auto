# MEXC Micro Maker Bot — v0069 Clean Rollback Stable

v0069 is a clean rollback from the last pre-Mirror stable branch. Mirror Lab / reverse bot code is removed completely.

## Main point

This build restores the original Price Tsunami logic and fixes only runtime/panel behavior:

- no Mirror Lab
- no virtual bad-bot collector
- no command-deletion spam
- `/log_full` sends a real `.txt` file by default
- `/log_tail` sends recent log lines as text
- `/scan` is read-only and answers separately
- Fees button uses cached status and does not run a heavy API recheck
- Start Tsunami does not toggle into pause
- `/start` only opens a panel; it does not start trading

## Panel lifecycle

Required behavior:

1. Start Tsunami sends one fresh scan panel down.
2. While running, the same panel is edited every 5 seconds.
3. The panel is not deleted every 5 seconds.
4. After 10 minutes, all known scan panels are deleted and one fresh panel is sent down.
5. The new panel is edited every 5 seconds for the next 10 minutes.

Settings:

```text
telegram_live_update_sec = 5
telegram_live_fast_update_sec = 5
telegram_panel_cycle_sec = 600
telegram_panel_refresh_mode = edit_rotate
telegram_delete_command_messages = false
telegram_reply_keyboard = false
```

## Diagnostics

Commands:

```text
/ping
/doctor
/log_full
/log_tail
/scan
/fees
```

## Tests run

```text
python -m py_compile *.py tests/*.py
python tests/panel_lifecycle_test.py
python tests/top10_fire_test.py
python tests/no_mirror_test.py
```

Expected:

```text
PANEL_LIFECYCLE_TEST_OK v0069
TOP10_FIRE_TEST_OK v0069
NO_MIRROR_TEST_OK v0069
```

## Version

```text
bot_version = v0069
trade_profile = wave_price_tsunami_v0069
```
