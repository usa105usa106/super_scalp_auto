# MEXC Micro Maker Bot v0064 — Stable Panel Runtime

Главная цель v0064 — не добавлять новые торговые идеи, а стабилизировать Telegram/runtime:

- live-панель в режиме RUNNING обновляется свежим сообщением снизу (`telegram_panel_refresh_mode=resend`), потому что Telegram edit не поднимает старое сообщение вниз;
- `/log_full` по умолчанию сразу присылает текстовый FAST-log и не висит на отправке файла;
- файл лога отправляется только явно: `/log_full file`;
- кнопка Fees показывает cached zero-fee статус без тяжёлой полной API-перепроверки;
- Start/Stop дают финальный ответ отдельным сообщением и не должны оставлять только “принято”;
- Start не должен превращаться в Stop/Pause при первом нажатии;
- первый scan tick после старта не блокируется повторным balance/positions private-check;
- сохранены Mirror Lab virtual тесты и команды.

## Проверки, прогнанные локально

```bash
python -m py_compile *.py tests/*.py
python tests/virtual_smoke_test.py
python tests/freeze_watchdog_test.py
python tests/telegram_runtime_test.py
python tests/callback_audit.py
```

Ожидаемый результат:

```text
VIRTUAL_SMOKE_TEST_OK v0064
FREEZE_WATCHDOG_TEST_OK v0064
TELEGRAM_RUNTIME_TEST_OK v0064
CALLBACK_AUDIT_OK callbacks=86
```

Живой MEXC/Telegram из среды сборки не запускался.
