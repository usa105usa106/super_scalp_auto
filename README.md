# MEXC Micro Maker LIVE v0049

## v0049 Price Tsunami — clean universe diagnostics

Главное изменение v0049: Telegram-панель больше не пишет мутное `UNIVERSE = 144`, будто это весь zero-fee список. Теперь она честно показывает весь путь:

```text
MEXC zero-fee total: 377 | blocked 233 | ignored 0 | trade universe zero-fee *_USDT: 144 | scan cap ALL
PRICE SCAN 10s: counted 144 / scan universe 144
```

Что означают цифры:

- `MEXC zero-fee total` — сырой список 0% fee contracts от MEXC до фильтров.
- `blocked` — пары, которые бот не должен торговать по текущим правилам: не `*_USDT`, `STOCK`, неподходящий quote/collateral.
- `ignored` — монеты, которые бот сам/пользователь отправил в ignore.
- `trade universe zero-fee *_USDT` — реальные разрешённые монеты для скана и торговли.
- `PRICE SCAN counted` — сколько монет реально входит в голосование LONG/SHORT/NEUTRAL.

Universe:

- `only_zero_fee = true` -> брать API-confirmed 0% fee contracts, потом оставить разрешённые `*_USDT`.
- `only_zero_fee = false` -> брать active public `*_USDT`, но pre-trade fee guard всё равно проверяет комиссии перед входом.
- `zero_fee_universe_max_symbols = 0` -> не резать сырой zero-fee universe.
- `max_zero_fee_scan_symbols = 0` -> не резать scan universe.
- `ws_depth_max_symbols = 0` -> WS подписка на весь scan universe.

Price Tsunami logic:

- каждые ~10 секунд сравнивается цена каждой монеты из trade universe;
- price up -> LONG vote;
- price down -> SHORT vote;
- flat/no history/no fresh price -> NEUTRAL vote;
- проценты LONG/SHORT/NEUTRAL считаются от всего scan universe, а не только от price-ready монет.

Risk modes:

- Early Wave: dominance сейчас >= 65% и эта же сторона выросла на +15п.п. за ~60s -> 5x, basket REAL NET TP +0.05 USDT.
- Normal Wave: dominance сейчас >= 75% -> 5x, basket REAL NET TP +0.05 USDT.
- Tsunami: dominance сейчас >= 75% и эта же сторона выросла на +15п.п. за ~60s -> 10x, basket REAL NET TP +0.10 USDT.

Signal hold v0049:

- сигнал должен подтвердиться 4 из 5 checks;
- окно удержания около 10 секунд;
- один шумовой провал не сбрасывает сигнал полностью.

Entry behavior:

- LONG: aggressive LIMIT по уже существующей ask-side ликвидности;
- SHORT: aggressive LIMIT по уже существующей bid-side ликвидности;
- `wave_entry_post_only = false`, поэтому это не maker-очередь;
- TTL около 450 ms, потом cancel остатка;
- если MEXC режет запросы/rate-limit — пауза и retry;
- если нет fill/flip/spread/liquidity — top-up scan и замена слота.

Basket behavior:

- открывается 5 монет одной стороной: либо 5 LONG, либо 5 SHORT;
- выбор из middle 25-60% same-side candidates, не из перегретого топа;
- закрытие всей корзины по REAL NET equity PnL;
- close mode по умолчанию MARKET, чтобы быстро забрать общий плюс;
- после 10 минут: если ноль/микроплюс — закрыть, если минус — ждать восстановления.

Control:

- Stop/Pause = пауза, не закрывает позиции и не отменяет ордера;
- Close All = отменяет ордера и закрывает все позиции market.
