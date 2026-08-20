-- ═══════════════════════════════════════════════════════════════════════
-- ЧИСТЫЙ РЕБИЛД РЕГИСТРА sales (one-source split dim/fact)
-- Источник: _AccumRg17844 (плоский регистр накопления продаж)
-- Цель:    public.sales (dim) + public.sales_positions (fact)
-- ═══════════════════════════════════════════════════════════════════════
-- ВНИМАНИЕ: НЕ выполнять автоматически. Перед запуском убедиться что:
--   1) public.sales и public.sales_positions СОЗДАНЫ руками по DDL
--   2) тип _Fld24665 уточнён (uuid+binary_to_uuid ИЛИ text)
--      — поправить строку с responsible_id ниже соответственно
-- ═══════════════════════════════════════════════════════════════════════

BEGIN;

-- 1. Регистр sales
INSERT INTO etl_meta.registers (code, name, description, default_mode, is_active)
VALUES (
    'sales',
    'Продажа',
    'Регистр накопления продаж — split на dim (sales) и fact (sales_positions)',
    'full_period',
    TRUE
)
RETURNING id;
-- запоминаем :reg_id

-- 2. Источник _AccumRg17844 (standalone — чтобы auto-fill не ломал split)
INSERT INTO etl_meta.register_sources (
    register_id, source_code, source_type, mssql_schema, mssql_table,
    onec_name, priority, is_active
)
SELECT id, 'regnak_prodazhi', 'standalone', 'dbo', '_AccumRg17844',
       'РегистрНакопления.Продажи', 0, TRUE
FROM etl_meta.registers WHERE code='sales';

-- 3. Маппинги — 12 полей (+ doc_type через recorder_type_lookup, добавляем отдельно)
WITH src AS (SELECT id FROM etl_meta.register_sources WHERE source_code='regnak_prodazhi')
INSERT INTO etl_meta.column_mappings (
    source_id, source_column, target_column, target_type, transform_type, onec_name, is_nullable
)
SELECT src.id, sc, tc, tt, trn, onec, nullable FROM src, (VALUES
    -- системные ключи
    ('_Period',         'period',                   'timestamp', 'fix_year',       'Период',           FALSE),
    ('_RecorderRRef',   'recorder',                 'uuid',      'binary_to_uuid', 'Регистратор',      FALSE),
    ('_RecorderTRef',   'doc_type',                 'varchar',   'recorder_type_lookup', 'РегистраторТип', TRUE),
    ('_LineNo',         'line_no',                  'integer',   NULL,             'НомерСтроки',      FALSE),
    -- измерения (dim)
    ('_Fld17850RRef',   'division_id',              'uuid',      'binary_to_uuid', 'Подразделение',    TRUE),
    ('_Fld17853RRef',   'partner_id',               'uuid',      'binary_to_uuid', 'Контрагент',       TRUE),
    -- ↓ responsible_id — TYPE и transform УТОЧНИТЬ после INFORMATION_SCHEMA проверки
    ('_Fld24665',       'responsible_id',           'uuid',      'binary_to_uuid', 'Ответственный',    TRUE),
    -- позиционные (fact)
    ('_Fld17845RRef',   'nomenclature_id',          'uuid',      'binary_to_uuid', 'Номенклатура',     TRUE),
    ('_Fld17854',       'quantity',                 'numeric',   NULL,             'Количество',       TRUE),
    ('_Fld17855',       'cost',                     'numeric',   NULL,             'Стоимость',        TRUE),
    ('_Fld17856',       'sales_without_discounts',  'numeric',   NULL,             'СтоимостьБезСкидок', TRUE),
    ('_Fld17857',       'vat',                      'numeric',   NULL,             'НДС',              TRUE),
    -- составной тип ЗаказПокупателя — берём _RRRef компонент
    ('_Fld17847_RRRef', 'order_id',                 'uuid',      'binary_to_uuid', 'ЗаказПокупателя',  TRUE)
) AS m(sc, tc, tt, trn, onec, nullable);

-- 4. recorder_type_map (заполняется обычно через Discover, но Discover требует parent_id)
--    Пока пустой — recorder_type_lookup без map отдаст int (binary_to_int fallback).
--    Если doc_type нужен как строка ('Документ.ЧекККМ' и т.п.) — заполнить
--    JSONB вида {"476": "Документ.ЧекККМ", "316": "Документ.РеализацияТоваровУслуг", ...}
--    через etl_meta.update_register_type_map() или прямой UPDATE.

-- 5. Target sales (dim, priority=0)
INSERT INTO etl_meta.register_targets (
    register_id, target_schema, target_table,
    source_id, load_mode, upsert_keys, priority, target_role,
    include_columns, is_active
)
SELECT
    r.id, 'public', 'sales',
    s.id, 'upsert', ARRAY['recorder']::TEXT[], 0, 'dimension',
    ARRAY['period','recorder','doc_type','division_id','partner_id','responsible_id']::TEXT[],
    TRUE
FROM etl_meta.registers r, etl_meta.register_sources s
WHERE r.code='sales' AND s.source_code='regnak_prodazhi';

-- 6. Target sales_positions (fact, priority=1) + post_load_sql для FK
INSERT INTO etl_meta.register_targets (
    register_id, target_schema, target_table,
    source_id, load_mode, upsert_keys, priority, target_role,
    include_columns, post_load_sql, is_active
)
SELECT
    r.id, 'public', 'sales_positions',
    s.id, 'upsert', ARRAY['recorder','line_no']::TEXT[], 1, 'fact',
    ARRAY[
        'recorder','line_no','nomenclature_id','quantity','cost',
        'sales_without_discounts','vat','order_id'
    ]::TEXT[],
    -- post_load_sql: резолв FK по natural key recorder
    -- ВНИМАНИЕ: проверь что в твоём DDL public.sales_positions есть колонка sales_id
    -- и public.sales имеет id (SERIAL PRIMARY KEY) + UNIQUE INDEX по recorder
    $POST$
UPDATE public.sales_positions AS f
SET    sales_id = d.id
FROM   public.sales AS d
WHERE  f.recorder = d.recorder
  AND  f.sales_id IS NULL;
$POST$,
    TRUE
FROM etl_meta.registers r, etl_meta.register_sources s
WHERE r.code='sales' AND s.source_code='regnak_prodazhi';

COMMIT;

-- ═══════════════════════════════════════════════════════════════════════
-- Верификация:
-- SELECT r.code, s.source_code, s.source_type, t.target_table, t.target_role,
--        t.priority, t.upsert_keys, t.include_columns, t.post_load_sql IS NOT NULL AS has_post
-- FROM etl_meta.registers r
-- JOIN etl_meta.register_sources s ON s.register_id=r.id
-- JOIN etl_meta.register_targets t ON t.register_id=r.id
-- WHERE r.code='sales'
-- ORDER BY t.priority;
-- ═══════════════════════════════════════════════════════════════════════
