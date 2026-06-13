# MEXC Micro Maker LIVE Bot v0023

## v0023: Active Plus mode

v0022 was too strict for the current market: it required depth 100 USDT, imbalance 1.55 and score 55, so the scanner often found zero valid candidates.

v0023 switches the default `/preset plus` profile to a more active mode:

- TP/SL: 1/1 tick
- min depth: 50 USDT
- depth multiplier: 3x position notional
- max spread: 2 ticks
- min imbalance: 1.20
- min trade score: 25
- quick entry recheck: 120 ms x 1
- cooldown after loss: 20 sec
- cooldown after trade: 1 sec
- max trades/hour: 120
- log retention: 20 minutes
- ignored symbols are cleared on restart/redeploy
- Telegram time offset: +3 hours
- API input messages are kept in chat by request

This profile is designed to trade more often than v0022 while still avoiding the weakest books. Profit cannot be guaranteed; monitor balance and reduce aggressiveness if losses continue.

After deploy:

```text
/preset plus
/ignore clear
```

To make it even more aggressive:

```text
/set min_depth_usdt 40
/set min_imbalance_ratio 1.15
/set min_trade_score 20
```

To make it safer:

```text
/set min_depth_usdt 75
/set min_imbalance_ratio 1.30
/set min_trade_score 35
```
