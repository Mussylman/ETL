---
tags: [паттерн, ETL, режимы]
date: 2026-04-10
---
# Три режима загрузки — full incremental consistency

## full_period
- Полная перезагрузка по диапазону дат
- Используется при первом запуске или пересборке
- `_Period BETWEEN start AND end`
- INSERT ONLY (предполагает пустой target)

## incremental
- Инкрементальная по изменениям
- Каждые 5 минут проверяет retail DB на обновления
- `DataChecker` находит UID с `updated_at > last_load`
- UPSERT по ключевой колонке

## consistency
- Проверка целостности
- Каждые 4 часа
- Ищет пропущенные или рассинхронизированные записи

## Реализация
- `ETLCore` — базовый класс с 3 режимами
- `ETLEngine` — конфигурационный (из etl_meta)
- `SalesETL` — специализированный для header+detail

## Ссылки
- [[Header-detail загрузка через JOIN]]
- [[Upsert по составному ключу]]
