-- Прямой путь SOURCE → ClickHouse: без промежуточного хранения фактов в PostgreSQL.
-- PostgreSQL остаётся control plane и реестром ключей.

-- Новый тип источника: регистр 1С, описанный в registers/register_sources/column_mappings.
-- Извлечение идёт через существующий ETLEngine.extract_frame — та же бизнес-логика,
-- что у первого hop'а, без копии.
ALTER TABLE etl_meta.ch_sync DROP CONSTRAINT IF EXISTS ch_sync_source_type;
ALTER TABLE etl_meta.ch_sync ADD CONSTRAINT ch_sync_source_type
    CHECK (source_type IN ('postgres', 'mssql', 'onec_register'));

-- document_patch: партиция пересобирается как «текущая партиция минус строки
-- изменившихся документов плюс их свежие строки из источника». Месяц целиком из
-- боевой 1С не перечитывается — в среднем там меняется 32 строки за цикл, а
-- извлечение месяца стоит 211 с (p90 382 с).
ALTER TABLE etl_meta.ch_sync DROP CONSTRAINT IF EXISTS ch_sync_load_mode;
ALTER TABLE etl_meta.ch_sync ADD CONSTRAINT ch_sync_load_mode
    CHECK (load_mode IN ('full', 'partitioned', 'document_patch'));

-- Параметры источника, которые не укладываются в общие колонки:
--   {"target": "sales_positions",          цель регистра, чей фрейм берём
--    "doc_key": ["recorder"],              ключ документа — по нему патчится партиция
--    "own_id": true,                       проставить id шапки из реестра документов
--    "parent": "sales",                    денормализовать колонки шапки в строки
--    "parent_key": ["recorder", "recorder_type"],
--    "parent_prefix": "hdr_"}
ALTER TABLE etl_meta.ch_sync ADD COLUMN IF NOT EXISTS source_params jsonb;

-- Watermark источника — в control plane, а не MAX() по таблице факта. У первого
-- hop'а он живёт в самих данных (MAX(retail_updated_at) из public.sales): убери факт
-- из PostgreSQL — исчезнет и watermark. Ключ — источник, а не цель: один регистр
-- питает несколько целей и читается один раз.
CREATE TABLE IF NOT EXISTS etl_meta.ch_source_state (
    source_key   varchar PRIMARY KEY,         -- 'onec_register:sales'
    watermark    timestamp,                    -- окно изменений читается с этого момента
    last_to_ts   timestamp,                    -- верхняя граница последнего успешного окна
    updated_at   timestamp NOT NULL DEFAULT now(),
    details      jsonb
);

-- Группа конфигураций — единица оркестрации. DAG вызывает runner с именем группы,
-- а не перечисляет таблицы: новый объект попадает в загрузку записью конфигурации.
ALTER TABLE etl_meta.ch_sync ADD COLUMN IF NOT EXISTS sync_group varchar;
