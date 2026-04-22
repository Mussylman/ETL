---
tags: [БД, схема, etl_meta]
date: 2026-04-10
---
# Схема базы данных etl_meta

Схема `etl_meta` в PostgreSQL (test, 10.10.1.142:5432) — конфигурация ETL pipeline.

## Таблицы

### registers
Главная сущность — группа источников и целей.
- `id`, `code` (уникальный), `name`, `description`
- `default_mode`: incremental / full_period / consistency
- `parent_id` → FK на registers (иерархия: sales → sales_positions)
- `parent_join_key`, `child_join_key` — ключи связи
- `recorder_type_map` (JSONB) — маппинг RecorderTRef → Document table
- `retail_table`, `retail_uid_column` — для инкрементальной загрузки
- `is_active`, `created_at`, `updated_at`

### register_sources
Источники данных (таблицы MSSQL).
- `id`, `register_id` → FK
- `source_code` (уникальный в рамках register)
- `source_type`: header / detail / standalone
- `mssql_schema` (dbo), `mssql_table` (_Document476)
- `onec_name` (Документ.ЧекККМ)
- `parent_source_id` → FK на register_sources (VT → шапка)
- `join_type`, `join_key_source`, `join_key_parent`
- `where_clause`, `priority`
- `fields_cache` (JSONB) — кэш полей 1С + MSSQL типы

### column_mappings
Маппинг колонок source → target.
- `id`, `source_id` → FK
- `source_column` (_Fld13628RRef), `target_column` (nomenclature)
- `is_expression` — true если source_column это SQL-выражение
- `target_type` (uuid, varchar, timestamp...)
- `transform_type` (binary_to_uuid, fix_year, binary_to_int...)
- `transform_params` (JSON), `default_value`
- `onec_name` (Номенклатура) — русское имя из 1С
- `is_nullable`, `is_active`

### source_unions
Объединение нескольких источников (UNION ALL).
- `id`, `register_id` → FK
- `union_code`, `description`
- `output_columns` (TEXT[]) — колонки результата

### source_union_members
Участники union.
- `id`, `union_id` → FK, `source_id` → FK
- `priority`, `where_clause`, `is_active`

### register_targets
Целевые таблицы PostgreSQL.
- `id`, `register_id` → FK
- `target_schema` (public), `target_table`
- `source_id` или `union_id` — откуда данные
- `load_mode`: upsert / insert / replace
- `upsert_keys` (TEXT[])
- `pre_load_sql` — DuckDB агрегация перед загрузкой
- `post_load_sql` — SQL после загрузки (resolve FK, cleanup)
- `include_columns` (TEXT[]) — подмножество колонок df для этой target (split dim/fact)
- `priority` (int) — порядок загрузки targets одного регистра (меньше → раньше)
- `target_role` — `dimension` / `fact` / NULL (назначение таблицы при split)

### load_history
История загрузок.
- `id`, `register_id`, `target_id`
- `started_at`, `finished_at`, `status`, `rows_loaded`, `error_message`

## Связи
```
registers 1──N register_sources
registers 1──N source_unions
registers 1──N register_targets
registers 1──1 registers (parent_id)
register_sources 1──N column_mappings
register_sources 1──1 register_sources (parent_source_id)
source_unions 1──N source_union_members
source_union_members N──1 register_sources
register_targets N──1 source_unions
register_targets N──1 register_sources
```

## Ссылки
- [[ETL Config App управляет конфигурацией на порту 5555]]
- [[Union объединяет документы в одну витрину]]
- [[Split dim-fact по priority и include_columns]]
- [[Custom Python Transform для вычисляемых колонок]]
