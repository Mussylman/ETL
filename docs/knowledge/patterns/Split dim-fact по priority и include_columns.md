---
tags: [паттерн, ETL, dim-fact, split]
date: 2026-04-22
---
# Split dim-fact по priority и include_columns

## Паттерн
Header-detail документ разделяется на две target-таблицы:
- **dim_** (измерение) — одна строка на документ, колонки из header-источника
- **fact_** (факт) — строки из detail-источника + FK на dim

Пример: чек ККМ → `dim_sales` (документ) + `fact_sales_products` (позиции).

## Конфигурация в register_targets
- `target_role` — `'dimension'` или `'fact'` (определяет назначение)
- `priority` — порядок загрузки, меньше → раньше (dim перед fact)
- `include_columns` (TEXT[]) — колонки, которые попадают в эту target-таблицу
- `post_load_sql` — опциональный SQL после загрузки (resolve FK, cleanup)

## Как работает
1. `ETLEngine._get_active_targets` сортирует targets по `priority` — dim загружается первой.
2. После `build_dataframe` и transform'ов `ETLEngine` фильтрует `df` по `include_columns`: в dim-target идут только колонки измерения, в fact — только факта. `etl_loaded_at` добавляется всегда.
3. Loader пишет в таблицу по `load_mode` (обычно upsert по id/составному ключу).
4. `post_load_sql` выполняется после INSERT — например, резолвит FK по natural key: `UPDATE fact SET dim_id = (SELECT id FROM dim WHERE ...)`.

## Авто-заполнение include_columns
ETL Config App синхронизирует `include_columns` после Column Builder:
- Маппинги из источников с `source_type='header'` → в dimension-target
- Маппинги из `source_type='detail'` → в fact-target
- `custom_python` колонки не трогаются автоматически — они добавляются вручную через `/api/registers/{id}/add-computed-column`
- `standalone` источники не меняют split автоматически — ручное назначение

Реализовано в `etl_config_app/app.py` в обработчике `sync_register_after_columns`.

## Почему priority
Без явного порядка engine мог бы попытаться загрузить fact до dim и упасть на FK-проверке в `post_load_sql`. Priority гарантирует детерминированный порядок: dim=0, fact=1 — стандарт.

## Ссылки
- [[Схема базы данных etl_meta]]
- [[Header-detail загрузка через JOIN]]
- [[Custom Python Transform для вычисляемых колонок]]
