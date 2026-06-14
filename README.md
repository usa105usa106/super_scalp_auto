# MEXC Micro Maker Bot — v0055 Button Audit Fix

v0055 is a code/button audit build over v0054.

## What was checked/fixed

- Python syntax: `python -m py_compile *.py` OK.
- Telegram callback map checked: all buttons point to existing settings/actions.
- Slow buttons now answer immediately and run in deduped background tasks.
- Repeated taps on Start / Stop / Close All / Price Scan / Fees do not spawn duplicate background jobs.
- Background UI tasks have timeouts, so a stuck API call does not leave an endless panel update task.
- Price Scan is read-only for signal/hold state: pressing it cannot help trigger HOLD or mutate live trading state.
- Signal ALL / Signal TOP10 buttons reset old HOLD/history state and refresh the correct screen.
- `/market_mode all|top10` and `/set signal all|top10` normalize aliases correctly.
- `/clear_ignored` command added as a direct alias for `/ignore clear`.
- Fees screen cleaned and truncated; it shows counts and first pairs instead of an unreadable wall.

## Signal modes

Default:
- `all_zero_total`: market direction is decided by the full zero-fee trade universe.

Toggle:
- `/market_mode top10`: market direction is decided by TOP10 liquid non-stable zero-fee leaders.
- Entries are still picked from the full zero-fee universe.

TOP10 rules:
- 7/10 leaders in one direction = NORMAL, 5x, REAL NET +0.05.
- 7/10 leaders plus +2 leaders growth over 60s = EARLY, 5x, REAL NET +0.05.
- 8/10 leaders in one direction = TSUNAMI, 10x, REAL NET +0.10.
- Signal still needs HOLD 4/5 checks over about 10 seconds.

Version/profile:
- `bot_version = v0055`
- `trade_profile = wave_price_tsunami_v0055`
