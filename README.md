# MEXC Micro Maker Bot — v0088 TOP15 Reserve Leader-Set Guard

## Что изменено в v0088

- Добавлен guard для TOP15 reserve: если из-за stale/no-fresh меняется выбранная десятка лидеров, бот сбрасывает 60s acceleration/hold history. Это убирает ложный +2 leader acceleration от самой замены монет, а не от движения рынка.

- Версия везде обновлена до `v0088`, profile: `wave_price_tsunami_v0088`.
- Убрана сырая схема `TOP10 -> top30 replacement`.
- Убрана зависимость от REST-repair по умолчанию для TOP10 сигнала.
- `TOP10 leaders` теперь работает через контролируемое окно `TOP15`:
  - первые 10 монет = основные лидеры;
  - следующие 5 монет = резерв;
  - если 1–5 основных лидеров получили `stale/no fresh`, бот временно добирает свежих из резерва;
  - если основной лидер ожил, он автоматически возвращается в TOP10 на следующем скане, а резервная монета выпадает.
- Если свежих монет не хватает даже в TOP15, недостающие основные лидеры остаются `neutral/stale`, и сигнал честно ждёт.
- Все фиксы v0084/v0085 сохранены: partial target scaling, no fee-bump, Last closed отдельно, чистое command menu.

## TOP10 freshness logic

```text
Primary TOP10:  L0 L1 L2 L3 L4 L5 L6 L7 L8 L9
Reserve +5:     L10 L11 L12 L13 L14

Если L1, L4, L8 stale:
Selected TOP10: L0 L2 L3 L5 L6 L7 L9 L10 L11 L12

Если L1 ожил:
Selected TOP10: L0 L1 L2 L3 L5 L6 L7 L9 L10 L11
```

То есть резерв используется только временно. Основной TOP10 всегда имеет приоритет, когда данные снова свежие.

## Tests

```text
ACTIVE_MANAGE_THROTTLE_TEST_OK v0088
BATCH_OPEN_SMOKE_TEST_OK v0088
CALLBACK_AUDIT_OK callbacks=36 v0088
COMMAND_MENU_CLEANUP_TEST_OK v0088
LOOP_TIMEOUT_TEST_OK v0088
NO_MIRROR_TEST_OK v0088
PANEL_LIFECYCLE_TEST_OK v0088
PARTIAL_TARGET_SCALING_TEST_OK v0088
PRIVATE_THROTTLE_TEST_OK v0088
SETTINGS_PERSIST_TEST_OK v0088
TOP10_FIRE_TEST_OK v0088
TOP15_RESERVE_REPLACEMENT_TEST_OK v0088
TOP15_SELECTION_CHANGE_GUARD_TEST_OK v0088
UI_TEXT_AUDIT_OK v0088
WAVE_PARTIAL_BATCH_OPEN_TEST_OK v0088
```
