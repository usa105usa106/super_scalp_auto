# MEXC Micro Maker Bot v0063 — Virtual Harness Tested

Фокус версии: остановить слепые правки и проверять бот в локальной виртуальной среде перед выдачей.

## Что изменено относительно v0062

- Версия и профиль обновлены до `v0063` / `wave_price_tsunami_v0063`.
- В архив добавлены локальные smoke-тесты без Telegram и без MEXC:
  - `tests/virtual_smoke_test.py`
  - `tests/freeze_watchdog_test.py`
- Тесты используют fake Telegram + fake MEXC market, гоняют:
  - командные handlers: `/ping`, `/doctor`, `/log_tail`, `/log_full`, `/mirror_test start/report/stop`;
  - engine start/stop;
  - scan loop heartbeat;
  - read-only `scan_now_text`;
  - Mirror Lab collector/report;
  - watchdog зависшего scan tick.
- Основная логика v0062 freeze guard сохранена:
  - run-loop tick timeout;
  - throttled private balance/position checks;
  - FAST `/log_full` без тяжёлых MEXC-запросов;
  - direct command replies без live-panel зависимости.

## Проверки, которые были прогнаны локально

```bash
python -m py_compile *.py
python tests/virtual_smoke_test.py
python tests/freeze_watchdog_test.py
```

Ожидаемый результат:

```text
VIRTUAL_SMOKE_TEST_OK v0063
FREEZE_WATCHDOG_TEST_OK v0063
```

Также статически проверены callback-кнопки: все `set:`/`toggle:` ключи существуют в `DEFAULTS`, префиксы обработчиков известные.

## Важно

Живой MEXC/Telegram API из этой среды не запускался. Проверена локальная виртуальная среда: команды, файл логов, Mirror Lab, engine-loop, скан и watchdog зависаний.
