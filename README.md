# MEXC Micro Maker LIVE Bot v0022

## Что изменено в v0022

v0022 делает четыре правки по запросу:

1. **Full log больше не разрастается бесконечно.**
   - По умолчанию хранится только последние `20` минут.
   - `/log_full` экспортирует только свежий хвост, максимум `8 MB`.
   - Подробные scan-details по каждой монете по умолчанию выключены, чтобы лог не становился огромным.

2. **Ignored symbols чистятся при рестарте/редеплое.**
   - Если бот перезапущен, старый `ignored_symbols` сбрасывается.
   - Это не режет universe после деплоя и не ухудшает новый скан из-за старых ошибок/нехватки маржи.

3. **Время в live panel переведено на UTC+3.**
   - Настройка: `telegram_time_offset_hours = 3`.
   - Можно поменять: `/set time_offset 3`.

4. **Profit mode v0022 вместо частых слабых сделок.**
   - Цель: меньше входов, более строгие стаканы, пауза после минуса.
   - Гарантировать прибыль невозможно, но профиль теперь не пытается брать все подряд.

## Профиль v0022 по умолчанию

```text
leverage = 5
position_margin_percent = 10
max_positions = 1
symbols_limit = 1
TP/SL = 1/1 tick
max_spread_ticks = 1
min_depth_usdt = 100
min_depth_multiplier = 5
min_imbalance_ratio = 1.55
min_trade_score = 55
entry_recheck_ms = 300
entry_recheck_count = 2
order_lifetime_ms = 350
max_position_lifetime_sec = 25
max_position_hard_lifetime_sec = 75
cooldown_after_loss_sec = 180
cooldown_after_trade_sec = 12
max_trades_per_hour = 12
daily_loss_limit_usdt = 0.35
max_consecutive_losses = 2
full_log_retention_minutes = 20
full_log_export_max_mb = 8
telegram_time_offset_hours = 3
```

## Команды

```text
/preset plus
/ignore clear
/set time_offset 3
/set log_retention 20
/set log_mb 8
/set imb 1.55
/set score 55
/set depth 100
/set recheck 300
/set recheck_count 2
/set cooldown_loss 180
```

## API

По текущему требованию пользователя API-сообщение в чате **не удаляется**:

```text
API_KEY API_SECRET
```

Бот сохраняет ключи и отвечает:

```text
✅ API saved
```

