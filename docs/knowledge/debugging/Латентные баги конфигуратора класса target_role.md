---
tags: [баг, аудит, etl_meta, config_loader, движок]
date: 2026-06-10
---
# Латентные баги класса «поле есть в etl_meta, но не доходит до движка»

Аудит 2026-06-10: сверка живой схемы etl_meta (10.10.1.142/test) ↔
`config_loader.py` ↔ `etl_engine.py`. Класс назван по прецеденту с
`target_role`, который не SELECT-ился в ConfigLoader.

## Критичные (стрельнут при wizard'ах B/C/D)

1. **`etl_engine.py:223` — `etl_table="public.sales"` захардкожен.**
   Watermark инкремента ВСЕГДА берётся из public.sales. Любой второй
   incremental-регистр получит чужой watermark → пропуск/перегон данных.
2. **«INNER JOIN JOIN»**: wizard (`app.py:1850`) и
   `dao.batch_create_document_sources` (dao.py:1206) пишут
   `join_type='INNER JOIN'`, а `query_builder.py:165` добавляет ` JOIN` →
   невалидный SQL. Латентен: оба target wizard'а A висят на standalone,
   JOIN-путь не выполняется. Схема ожидает 'INNER'/'LEFT'/'RIGHT'.
3. **Header/VT источники wizard'а A — мёртвый конфиг.** Оба target ссылаются
   на main_src (AccumRg); mappings документов/VT не попадают ни в SQL движка
   (`_build_sql_for_target` идёт от target.source_id), ни в DDL Sync
   (`_collect_mappings_for_target` — тоже). Аналитик «настроил» документы —
   движок их не грузит.
4. **Sync создаёт таблицы без PK/UNIQUE/id** (`dao.compute_sync_plan`:
   только колонки + etl_loaded_at + etl_hash). load_mode=upsert падает на
   ON CONFLICT, post_load_sql FK-резолв требует d.id. Эталон sales создавали
   руками именно поэтому.
5. **Фильтр периода прибит к `_Period`** (`query_builder.py:189`):
   у `_Document*` колонка `_Date_Time`, у `_Reference*` периода нет вообще →
   full_period для них генерит невалидный SQL.
6. **`_delete_missing` (etl_engine.py:460) и delete_insert_by_recorder
   хардкодят колонку `recorder`** — у справочников/документов ключ `id`/`id_ref`.
7. **post_load_sql wizard'а хардкодит `sales_id`** (app.py:1872) для любого
   регистра + `SYSTEM_COLS` в dao.py:901 содержит 'sales_id'.

## Некритичные / мёртвые поля

8. `column_mappings.default_value` — грузится в ColumnMapping, движком
   нигде не применяется (чистый target_role-класс).
9. `registers.parent_id`, `parent_join_key`, `child_join_key` — пишутся
   формой регистра, ConfigLoader не читает. Мёртвые.
10. `registers.recorder_type_map` — ConfigLoader не читает; карта попадает
    в движок только как копия в `transform_params` в момент
    `auto_create_mappings`. Обновили карту позже → существующие маппинги
    остались со stale-копией.
11. `Loaders.replace` = TRUNCATE → упадёт на dim, на которую смотрит FK.
12. `sources.fields_cache` без инвалидации (изменение структуры 1С не
    подхватывается). UI-only, в движок не идёт — by design.
13. union.output_columns ↔ target.include_columns дрейфуют: delete-target-column
    чистит union, но не include_columns (orphan-колонки).
14. dao.py хранит пароль PG в коде (DB_CONFIG).

Решение: см. [[Wizard'ы по типам 1С строятся над единым RegisterSpec и одним writer]].

## Статус 2026-06-10 (этап 0.2 реализован)

Исправлены: №1 (watermark из dim-target, `_get_watermark_table`),
№2 (нормализация join_type в QueryBuilder + spec), №4 (Sync-DDL: PK/UNIQUE/
updated_at/FK), №5 (period_column, миграция 004), №6 (missing-delete по
upsert_keys[0]). Добавлен transform invert_bool.
Остаются: №3 (мёртвые header/VT в wizard A — этап A′), №7 (sales_id-шаблон —
этап A′), №8–14.
