# MEXC Micro Maker v0030 Basket Truth

Fixes v0029 panel/counter lie:
- Live panel now shows NET equity PnL = current USDT equity - start equity.
- Closed trades PnL is shown separately and is not presented as account profit.
- If a filled position proves actual non-zero fees via position fee/feeRates, bot aborts that invalid contract and ignores the symbol for the session.
- USDT-only basket filter remains.

Important: no-stop baskets can still hold losers indefinitely. NET equity PnL is the only truth.


## v0033 Wave Hunter Safe
- Подтверждение импульса через лидеров BTC/SOL/ETH.
- Входы корзины запускаются параллельно, не 3+2 и не по одному с большой задержкой.
- Контракты с фактической комиссией больше не закрываются мгновенно только из-за fee; цель корзины автоматически поднимается, чтобы покрыть entry/exit fee + чистый буфер.
- Исправлен scan_symbol_error duplicate bid/ask.
