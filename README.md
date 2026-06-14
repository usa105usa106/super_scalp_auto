# MEXC Micro Maker LIVE v0037

## v0037 Price Tsunami Basket + Stop Pause Fix

Strategy: price-vote market wave detector. The bot samples prices, counts how many active symbols rose/fell, calculates dominance and 60s acceleration, then opens a 5-position basket in one direction.

Risk modes:
- Early wave: acceleration >= 15% and dominance >= 65% -> 5x, basket NET TP +0.05 USDT.
- Normal wave: dominance >= 75% -> 5x, basket NET TP +0.05 USDT.
- Tsunami: dominance >= 75% and acceleration >= 15% -> 10x, basket NET TP +0.10 USDT.

Operational behavior:
- Stop = hard pause only. It stops scanner/new entries and does NOT cancel orders or close positions.
- Close All = full exchange cleanup. It cancels active/limit/plan/stop orders, closes all positions by market, then cancels again.
- Telegram callbacks answer immediately; slow exchange cleanup runs in background only for Close All.
