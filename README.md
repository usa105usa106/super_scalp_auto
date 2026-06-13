# MEXC Micro Maker Bot v0027

Edge Plus v0027: live-only real-PnL mode.

Key changes vs v0025:
- Keeps v0025 real balance PnL accounting and exact fee guard.
- TP/SL default is 3/1 ticks instead of 1/4.
- Adds edge filter: top-of-book imbalance + microprice must agree with direction.
- Requires stable recheck before entry.
- Bans a symbol for the session after a real balance loss.
- Keeps API message in chat, keeps 20-minute log retention, clears ignored cache on restart.

Use:
/preset plus
/ignore clear
Start LIVE
