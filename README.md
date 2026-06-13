# MEXC Micro Maker LIVE Bot v0013

Отдельный Telegram-бот для live micro-maker / zero-fee scalping на MEXC futures.
Старые режимы вырезаны. Из старого бота оставлен рабочий механизм MEXC API: подпись запросов, открытие/закрытие сделок, отмена ордеров, баланс, позиции, плечо, zero-fee/fee checks.

## Что изменено в v0013

- Проверен запуск в виртуальной среде: compile/import/dependency smoke-test.
- Исправлен `/log_full clear`: лог теперь не теряется после очистки, handler пересоздаётся корректно.
- `/log_full` теперь flush-ит handler перед экспортом, чтобы в `.txt` попали последние строки.
- Сканер в WebSocket-режиме больше не делает массовый REST fallback по десяткам монет каждую секунду. Это защищает от REST-rate-limit и лагов.
- Добавлен параметр `ws_scan_rest_fallback_limit`, по умолчанию `0`: быстрый скан читает локальный WS-кэш, REST остаётся для активной сделки/ручной диагностики.
- WebSocket depth parser стал устойчивее к разным форматам строк стакана: list/tuple/dict.
- Исправлена floating-point ошибка: настоящий spread 1 tick больше не отбрасывается как 0.999999999999 tick.

## Что было добавлено в v0012

- Добавлена команда `/log_full`.
- `/log_full` отправляет в Telegram файл `.txt` с полным debug-логом.
- `/log_full clear` очищает текущий полный лог.
- В лог пишутся:
  - подробные ошибки с traceback;
  - пересборка zero-fee universe каждые 60 секунд;
  - список активных кандидатов и перебор монет;
  - причины отсева монет: нет стакана, плохой spread, мало depth, нет imbalance, volume reject, региональные/unsupported/min-max ошибки;
  - переключение монеты;
  - расчёт размера позиции;
  - подготовка входа;
  - выставление post-only входа;
  - отмена входа после lifetime;
  - fill / not filled;
  - сопровождение позиции;
  - выставление close-limit;
  - virtual stop / time stop;
  - market close;
  - Close All;
  - MEXC private API request/response без раскрытия API secret/token/signature.
- В `help` добавлена инструкция по `/log_full`.
- Обычная Telegram-клавиатура обновлена: `/log_full` добавлен рядом с `/trades`.
- Версия везде обновлена на `v0013`.

## Главные кнопки Telegram

Обычные кнопки Telegram, не inline:

```text
/start          /ping
/balance        /status
/trades         /log_full
/help
```

Inline-кнопки live-панели:

```text
▶️ Start LIVE        ⏸ Stop
❌ Close All         📒 Trades
📊 Live Panel        🔍 Scan Now
⚙️ Settings          📈 Scanner/Symbols
🔑 API               🧾 Fees
```

## Команды

```text
/start              открыть live-панель и включить обычные кнопки
/ping               отклик, Telegram lag, память, uptime, версия
/balance            live-чтение баланса USDT и открытых позиций
/status             полный статус стратегии, сканера, PnL и позиций
/trades             счётчик сделок: session и total
/trades reset       сбросить общий счётчик total
/log_full           прислать полный debug-лог .txt
/log_full clear     очистить полный debug-лог
/help               инструкция

/api set KEY SECRET сохранить MEXC API, сообщение с ключами удаляется
/api status         статус API
/api clear          удалить API

/symbols clear      FULL AUTO по всем API-confirmed zero-fee парам
/symbols LINK_USDT,SOL_USDT whitelist
/ignore             показать ignored symbols
/ignore clear       очистить ignored symbols

/set leverage 5     плечо 5x
/set leverage 10    плечо 10x
/set size 10        одна сделка 10% от total equity
/set positions 1    максимум открытых позиций
/set symbols 1      сколько монет торговать одновременно
/set tp 1           тейк в тиках
/set sl 3           виртуальный стоп в тиках
/set scan 1         быстрый автоскан раз в 1 секунду
/set rescan 60      пересборка zero-fee universe каждые 60 секунд
/set candidates 80  активное WS/scoring окно

/close_all          остановить бота, снести ордера и закрыть все позиции market
/closeall           то же самое
```

## Логика по умолчанию

```text
FULL AUTO: ON
OnlyZeroFee: ON
Zero-fee universe rescan: 60 sec
Active WS candidates: 80
Market data: WebSocket depth + REST fallback
Position size: 10% of TOTAL USDT equity
Leverage: 5x
Max positions: 1
Symbols limit: 1
TP: 1 tick
SL: 3 ticks
Order lifetime: 700 ms
Requote: 200 ms
```

## Stop и Close All

`⏸ Stop` — пауза торговли и скана:

```text
- новые сделки не открываются;
- фоновый скан останавливается;
- активные ордера отменяются;
- позиции market не закрываются.
```

`❌ Close All` — полная очистка биржи:

```text
- торговля останавливается;
- активные/лимитные/plan/stop ордера отменяются;
- все открытые позиции закрываются market;
- после закрытия ордера чистятся ещё раз.
```

## Full log

Лог пишется в:

```text
logs/log_full.txt
```

Команда:

```text
/log_full
```

создаёт экспорт:

```text
logs/exports/mexc_micro_maker_log_full_YYYYMMDD_HHMMSS.txt
```

и отправляет этот `.txt` в Telegram.

Секреты маскируются: API key, API secret, token, signature, password и похожие поля не выводятся полностью.

## Запуск

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# впиши TELEGRAM_BOT_TOKEN в .env
python main.py
```

API-ключи MEXC задаются только через Telegram:

```text
/api set API_KEY API_SECRET
```

Telegram token остаётся только в `.env`.

Важно: стопы/тейки виртуальные, их исполняет сам бот. Если процесс выключен, виртуальная защита не работает. Для полной очистки используй `❌ Close All` или `/close_all`.
