# MEXC Micro Maker Bot — v0090 TOP15 Reserve Leader-Set Guard

## Что изменено в v0090

- Сохранена и усилена логика TOP15 reserve: сигнал голосует только выбранной десяткой, но десятка каждый скан собирается из окна primary TOP10 + reserve 5.
- Добавлена явная диагностика на панели: `TOP15 window: primary 10 + reserve 5; used X/5; primary stale Y/10; selected fresh Z/10`, чтобы было видно, что резервные 5 не удалены.
- Добавлен guard для TOP15 reserve: если из-за stale/no-fresh меняется выбранная десятка лидеров, бот сбрасывает 60s acceleration/hold history. Это убирает ложный +2 leader acceleration от самой замены монет, а не от движения рынка.

- Версия везде обновлена до `v0090`, profile: `wave_price_tsunami_v0090`.
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
ACTIVE_MANAGE_THROTTLE_TEST_OK v0090
BATCH_OPEN_SMOKE_TEST_OK v0090
CALLBACK_AUDIT_OK callbacks=35 v0090
COMMAND_MENU_CLEANUP_TEST_OK v0090
LOOP_TIMEOUT_TEST_OK v0090
NO_MIRROR_TEST_OK v0090
PANEL_LIFECYCLE_TEST_OK v0090
PARTIAL_TARGET_SCALING_TEST_OK v0090
PRIVATE_THROTTLE_TEST_OK v0090
SETTINGS_PERSIST_TEST_OK v0090
TOP10_FIRE_TEST_OK v0090
TOP15_RESERVE_REPLACEMENT_TEST_OK v0090
TOP15_RESERVE_PRIMARY_RESTORE_OK v0090
TOP15_SELECTION_CHANGE_GUARD_TEST_OK v0090
UI_TEXT_AUDIT_OK v0090
WAVE_PARTIAL_BATCH_OPEN_TEST_OK v0090
```
