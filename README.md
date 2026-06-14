# MEXC Micro Maker Bot — v0059 Command Direct Fix

Цель v0059: убрать баг, когда команды в Telegram выглядят как будто пропали/не отвечают, а бот вместо результата пишет только `Меню команд включено`.

## Что изменено

- `bot_version = v0059`
- `trade_profile = wave_price_tsunami_v0059`
- Команды больше не удаляются принудительно:
  - `telegram_delete_command_messages = false`
- Reply-keyboard helper отключён по умолчанию:
  - `telegram_reply_keyboard = false`
  - `telegram_reply_keyboard_delete_hint = false`
- `/log_full` теперь полностью direct:
  - сразу отвечает `команда принята`
  - не использует live-панель
  - не показывает helper menu
  - отправляет файл отдельным сообщением
  - при ошибке/timeout пишет ошибку в чат
- `/mirror_test start/report/stop/clear` теперь direct:
  - сначала ack-сообщение
  - потом результат отдельным сообщением
  - не зависит от live-панели
- `/ping`, `/status`, `/trades`, `/help`, `/scan` отвечают отдельным сообщением, а не через старую панель.
- Добавлен `/scan` как прямой read-only Price Scan.
- Добавлен global error handler: если команда падает, бот пишет ошибку в чат и в full log, а не молчит.
- Mirror Lab button теперь также отправляет report отдельным сообщением.

## Проверка после запуска

1. `/ping` — должен сразу ответить отдельным сообщением с `v0059`.
2. `/doctor` — должен показать `Doctor v0059`.
3. `/log_full` — сначала ack, потом файл.
4. `/mirror_test start` — ack + сообщение о запуске collector.
5. Через 1–3 минуты: `/mirror_test report`.
6. `/scan` — read-only scan отдельным сообщением.

Живой запуск на MEXC/Telegram не выполнялся в этой среде. Статическая проверка и `py_compile` пройдены.
