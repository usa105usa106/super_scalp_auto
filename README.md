# MEXC Micro Maker Bot v0029

Basket Harvest v0029.

Core idea:
- keeps 3 active positions when possible;
- each position uses 10% of total USDT equity as margin;
- opens only API-confirmed / exact-contract zero-fee candidates;
- no per-position stop loss;
- closes a position only when the basket target is reached: default +0.01 USDT;
- after a position closes, the bot immediately tries to refill the free slot;
- real session PnL is counted from live USDT equity, not virtual price math.

Use:
/preset plus
/ignore clear
Start LIVE

Important: there is no per-position stop in this mode. Manual Stop leaves positions open and cancels active orders. Close All still closes everything by market manually.


## v0029 fix
- USDT-only quote filter: blocks *_USDC, *_USD, *_USD1 contracts to avoid `Balance insufficient available=0` on USDT-only accounts.
- Basket display/slot logic now limits `Current` to the configured 3 active slots.
