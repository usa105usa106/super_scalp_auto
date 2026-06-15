# MEXC Micro Maker Bot — v0078 Audited Runtime Stable

Clean rollback branch based on v0069, with no Mirror Lab / no reverse-bot code.

## Fixes in v0078

- ALL and TOP10 signal modes preserved:
  - `all_zero_total`
  - `top10_leaders`
- TOP10 rules preserved:
  - 7/10 = NORMAL
  - 7/10 + +2 leaders over 60s = EARLY
  - 8/10 = TSUNAMI
  - entries are still picked from the full zero-fee universe.
- Runtime scan tick watchdog:
  - one stuck scan/API tick cannot freeze the whole strategy loop.
  - default `runtime_loop_tick_timeout_sec = 22`.
- Private API throttling:
  - balance check no longer runs every 100ms loop tick.
  - open positions check no longer runs every 100ms loop tick.
  - active basket management now checks all open positions through one cached snapshot instead of per-symbol polling.
  - active basket equity snapshot is cached briefly, so `account/assets` is not requested every 450ms manage tick.
  - position margin balance read is cached briefly.
- WS freshness relaxed for the full 144-symbol universe:
  - default `ws_book_stale_ms = 1200` instead of `700`.
  - older stored values below 1200 are lifted to 1200 on load.
- Doctor command now shows loop tick count, heartbeat age, last tick ms, timeout count.
- Panel lifecycle preserved:
  - Start sends one fresh panel down.
  - Running edits that panel every 5 seconds.
  - Every 10 minutes old known panels are deleted and one fresh panel is sent down.
- `/log_full` sends TXT export.
- `/log_tail` remains available manually but is hidden from the Telegram command menu.

- Telegram UI cleanup v0078:
  - Telegram command menu is reduced to `/start`, `/scan`, `/balance`, `/status`, `/help`.
  - live inline panel keeps trading actions, Price Scan, service tools, and one ALL total/TOP10 signal toggle.
  - default signal mode is `all_zero_total`; one tap switches the live button to TOP10, another tap returns to ALL total.
  - Settings screen is balanced: size, basket, direction, targets, signal thresholds, hold and panel refresh are visible as buttons; rare advanced commands `/set`, `/symbols`, `/market_mode` still work manually.
  - Settings / Universe / API / Doctor / Log Full are sent as separate messages, so the 5s live scan refresh cannot overwrite them.

## Tests run

```bash
python -m py_compile *.py tests/*.py
python tests/no_mirror_test.py
python tests/top10_fire_test.py
python tests/panel_lifecycle_test.py
python tests/private_throttle_test.py
python tests/active_manage_throttle_test.py
python tests/loop_timeout_test.py
python tests/callback_audit.py
```

Expected:

```text
NO_MIRROR_TEST_OK v0078
TOP10_FIRE_TEST_OK v0078
PANEL_LIFECYCLE_TEST_OK v0078
PRIVATE_THROTTLE_TEST_OK v0078
ACTIVE_MANAGE_THROTTLE_TEST_OK v0078
LOOP_TIMEOUT_TEST_OK v0078
CALLBACK_AUDIT_OK ... v0078
```

## UI cleanup v0078

Telegram bot command menu contains only:

```text
/start
/scan
/balance
/status
/help
```

Live inline panel contains:

```text
▶️ Start Tsunami | ⏸ Stop/Pause
❌ Close All     | 🔍 Price Scan
✅ Signal: ALL total   # tap -> TOP10
📄 Log Full      | 🩺 Doctor
⚙️ Settings      | 📈 Universe
🔑 API
```

When switched:

```text
✅ Signal: TOP10       # tap -> ALL total
```

Balance/status/help are not duplicated in the inline panel. Service tools stay as inline buttons, not bot-menu commands, and open in separate messages.


## Settings screen v0078

Inline Settings now contains a medium set of useful controls:

```text
Signal ALL/TOP10
Dir BOTH / LONG / SHORT
Size 10 / 15 / 20%
Basket 3 / 5
NET +$0.03 / +$0.05
Tsunami +$0.10 / +$0.15
Early 60 / 65%
Normal 70 / 75%
Accel 10 / 15 p.p.
Hold 3/5 / 4/5
Panel 5s / 10s
Back to Live
```
