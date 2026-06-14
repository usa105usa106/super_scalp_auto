# MEXC Micro Maker LIVE v0046

## v0046 Price Tsunami — code audit + aggressive entry safety fix

Главное:

Code-audit fixes in v0046:
- убран дубль метода `_format_wave_status`, который перекрывал старую реализацию;
- retry больше не может открыть дубль по той же монете: перед каждой повторной попыткой бот сначала проверяет уже открытую позицию;
- после aggressive LIMIT бот делает дополнительный короткий recheck позиции перед `not_filled`, чтобы не принять задержку MEXC за пустой слот;
- top-up замены теперь тоже выбираются из middle 25-60%, а не из верхушки score-списка.

- скан не привязан к 250 монетам;
- по умолчанию бот берёт ВСЕ API-confirmed 0% fee *_USDT contracts;
- price-scan считает LONG/SHORT/NEUTRAL от всего universe;
- no fresh price / no 10s history считается NEUTRAL, а не выкидывается из базы;
- вход в позицию — aggressive LIMIT по уже существующей ликвидности в стакане, не market и не passive maker.

Universe:
- `only_zero_fee = true` -> взять ВСЕ API-confirmed 0% fee *_USDT contracts;
- `zero_fee_universe_max_symbols = 0` -> без лимита universe;
- `max_zero_fee_scan_symbols = 0` -> price-scan по всему zero-fee universe;
- `ws_depth_max_symbols = 0` -> WS подписка на весь scan-universe.

Если `only_zero_fee = false`, бот сканирует все активные public *_USDT futures contracts, но pre-trade fee guard всё равно может отбраковать контракты с комиссией.

Price Tsunami logic:
- каждые ~10 секунд сравнивается цена каждой монеты из universe;
- price up -> LONG vote;
- price down -> SHORT vote;
- flat/no history/no fresh price -> NEUTRAL vote;
- панель показывает: universe count, counted, price-ready, no fresh price, LONG/SHORT/NEUTRAL.

Risk modes:
- Early Wave: dominance сейчас >= 65% и эта же сторона выросла на +15п.п. за ~60s -> 5x, basket REAL NET TP +0.05 USDT.
- Normal Wave: dominance сейчас >= 75% -> 5x, basket REAL NET TP +0.05 USDT.
- Tsunami: dominance сейчас >= 75% и эта же сторона выросла на +15п.п. за ~60s -> 10x, basket REAL NET TP +0.10 USDT.

Entry behavior v0046:
- LONG: берёт ask-side стакана, выбирает ближайший существующий ask-level с достаточной cumulative liquidity;
- SHORT: берёт bid-side стакана, выбирает ближайший существующий bid-level с достаточной cumulative liquidity;
- ставит обычный LIMIT order `type=1` по этой цене;
- `wave_entry_post_only = false`, поэтому это не maker-очередь;
- ждёт `wave_entry_order_lifetime_ms` примерно 450 ms;
- отменяет остаток;
- если MEXC rate-limit/throttle — ждёт и повторяет;
- если нет fill/flip/spread/liquidity — top-up scan и замена слота.

Entry settings:
- `wave_entry_book_sweep_levels = 5`
- `wave_entry_liquidity_multiplier = 1.0`
- `wave_entry_max_sweep_ticks = 3.0`
- `wave_open_batch_gap_ms = 1000`
- `wave_open_retry_delay_sec = 2.0`
- `wave_open_retry_rounds = 4`

Basket behavior:
- открывается 5 монет одной стороной: либо 5 LONG, либо 5 SHORT;
- выбор из middle 25-60% same-side candidates, не из перегретого топа;
- закрытие всей корзины по REAL NET equity PnL;
- close mode по умолчанию market, чтобы забрать общий плюс;
- после 10 минут: если ноль/микроплюс — закрыть, если минус — ждать восстановления.

Control:
- Stop/Pause = пауза, не закрывает позиции и не отменяет ордера;
- Close All = отменяет ордера и закрывает все позиции market.
