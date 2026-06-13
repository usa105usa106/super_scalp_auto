# MEXC Micro Maker LIVE Bot v0016

Отдельный Telegram-бот для live micro-maker / zero-fee scalping на MEXC futures.
Старые режимы вырезаны. Из старого бота оставлен рабочий механизм MEXC API: подпись запросов, открытие/закрытие сделок, отмена ордеров, баланс, позиции, плечо, zero-fee/fee checks.

## v0018: leverage/rate-limit fix

v0017 started trading, but MEXC returned `code=2019` (`Leverage adjustment unavailable while orders are open`) and then `code=510` (`Requests are too frequent`).

The fix is:

- `mexc_set_leverage_on_entry=false` by default. The bot no longer calls `change_leverage` before every entry.
- `mexc_strict_leverage=false` by default. Leverage setup errors no longer block an entry.
- `mexc_private_rate_limit=8` by default to reduce private API storms.
- Leverage is still sent in the normal `order/create` body.

Telegram commands:

```
/set set_leverage off
/set strict_leverage off
/set rate 8
```


## Что изменено в v0016

- Для Coolify теперь нужны только 2 переменные окружения:
  - `TELEGRAM_BOT_TOKEN`
  - `ADMIN_IDS`
- Все остальные значения выставлены внутри бота по умолчанию.
- MEXC API задаётся только через Telegram: `/api set API_KEY API_SECRET`.
- MEXC REST/WS endpoint, recv-window, private rate limit, public/private timeout и strict leverage теперь лежат в runtime settings и могут меняться через `/set`.
- `.env.example` очищен: там оставлены только токен Telegram и admin id.
- Версия везде обновлена на `v0016`.

## Coolify ENV

В Coolify указывай только:

```env
TELEGRAM_BOT_TOKEN=твой_токен_от_BotFather
ADMIN_IDS=твой_telegram_user_id
```

Больше ничего в Coolify добавлять не нужно.

Встроенные дефолты уже выставлены так:

```text
MEXC REST: https://api.mexc.com
MEXC WS: wss://contract.mexc.com/edge
Recv window: 20000
Private API rate limit: 18 requests / 2 sec
Public timeout: 6 sec
Private timeout: 15 sec
Strict leverage: ON
```

Эти значения можно менять из Telegram:

```text
/set rest_base https://api.mexc.com
/set ws_endpoint wss://contract.mexc.com/edge
/set recv 20000
/set rate 18
/set public_timeout 6
/set private_timeout 15
/set strict_leverage on
```

## Первичная настройка

1. Залей файлы в GitHub так, чтобы `Dockerfile` лежал в корне репозитория.
2. В Coolify добавь только `TELEGRAM_BOT_TOKEN` и `ADMIN_IDS`.
3. Запусти deploy.
4. В Telegram отправь:

```text
/start
/api set API_KEY API_SECRET
/balance
/log_full clear
```

5. Нажми `▶️ Start LIVE`.

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

/close_all          остановить бота, отменить все ордера и закрыть все позиции market

/set leverage 5     плечо
/set size 10        одна сделка = 10% от TOTAL USDT equity
/set positions 1    максимум открытых позиций
/set symbols 1      сколько монет торговать одновременно
/set tp 1           тейк в тиках
/set sl 3           виртуальный стоп в тиках
/set scan 1         интервал быстрого автоскана
/set rescan 60      пересборка zero-fee universe каждые 60 сек
/set candidates 80  сколько активных zero-fee монет держать в WS/scoring окне
/set panel_sec 5    обычная частота live-сообщения
/set panel_fast_sec 2 частота live-сообщения при открытой позиции
/set panel_stopped_sec 0 не обновлять live-панель в STOPPED

/set rest_base https://api.mexc.com
/set ws_endpoint wss://contract.mexc.com/edge
/set recv 20000
/set rate 18
/set public_timeout 6
/set private_timeout 15
/set strict_leverage on
```

## Поведение Stop / Close All

```text
⏸ Stop
- пауза торговли и фонового скана
- новые сделки не открываются
- позиции market принудительно не закрывает

❌ Close All
- останавливает бота
- отменяет все активные/лимитные/plan/stop ордера
- закрывает все позиции market
- после этого чистит ордера ещё раз
```

## Важное

- Стопы и тейки виртуальные: их исполняет сам бот. Если процесс/сервер упал, виртуальная защита не работает.
- Перед нормальной торговлей проверь `/balance`, `/log_full clear`, 1–2 сделки минимальным объёмом, потом `/close_all` и `/log_full`.
- `ADMIN_IDS` лучше обязательно указать, чтобы чужие пользователи не могли управлять ботом.
