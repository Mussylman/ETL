---
tags: [сессия, etl-config]
date: 2026-04-10
---
# 2026-04-10 — ETL Config MVP

## Что сделано
- Авто-преобразования по MSSQL типам (binary_auto, fix_year)
- MSSQL INFORMATION_SCHEMA → автоподстановка target_type + transform
- VT чекбоксы стали независимыми (не каскадные)
- fields_cache JSONB — кэш полей 1С + типы MSSQL
- Column Builder: при выборе поля автоматом ставятся тип и transform
- 3 вкладки: Источники, Колонки, Статус
- UI частично на русском
- Obsidian vault создан как контрольный центр разработки

## Проблемы найдены
- Повторный Discover → duplicate key error
- Column Builder и Source Detail — два места для маппинга
- Пароли захардкожены в коде

## Следующая сессия
- Исправить idempotent Discover
- Полное тестирование flow
- Доперевести UI на русский
