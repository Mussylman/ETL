-- ============================================================
-- ПРИМЕР: Регистр продаж (sales_register)
-- Объединяет: ЧекККМ, Реализация, Возврат
-- ============================================================

-- 1. Регистр
INSERT INTO etl_meta.registers (code, name, description, default_mode, retail_table, retail_uid_column)
VALUES (
    'sales_register',
    'Регистр продаж',
    'Объединённые данные продаж из ЧекККМ, Реализации и Возвратов',
    'incremental',
    'sales',
    'document_uid'
);

-- ============================================================
-- 2. ИСТОЧНИКИ: ЧекККМ (header + details)
-- ============================================================

-- Header: ЧекККМ
INSERT INTO etl_meta.register_sources
    (register_id, source_code, source_type, mssql_table, priority)
SELECT r.id, 'check_h', 'header', '_Document394', 1
FROM etl_meta.registers r WHERE r.code = 'sales_register';

-- Details: ЧекККМ.Товары
INSERT INTO etl_meta.register_sources
    (register_id, source_code, source_type, mssql_table,
     parent_source_id, join_type, join_key_source, join_key_parent, priority)
SELECT
    r.id,
    'check_d',
    'detail',
    '_Document394_VT17845',
    (SELECT id FROM etl_meta.register_sources WHERE source_code = 'check_h'),
    'INNER',
    '_Document394_IDRRef',
    '_IDRRef',
    1
FROM etl_meta.registers r WHERE r.code = 'sales_register';

-- ============================================================
-- 3. ИСТОЧНИКИ: Реализация (header + details)
-- ============================================================

-- Header: Реализация
INSERT INTO etl_meta.register_sources
    (register_id, source_code, source_type, mssql_table, priority)
SELECT r.id, 'real_h', 'header', '_Document395', 2
FROM etl_meta.registers r WHERE r.code = 'sales_register';

-- Details: Реализация.Товары
INSERT INTO etl_meta.register_sources
    (register_id, source_code, source_type, mssql_table,
     parent_source_id, join_type, join_key_source, join_key_parent, priority)
SELECT
    r.id,
    'real_d',
    'detail',
    '_Document395_VT17850',
    (SELECT id FROM etl_meta.register_sources WHERE source_code = 'real_h'),
    'INNER',
    '_Document395_IDRRef',
    '_IDRRef',
    2
FROM etl_meta.registers r WHERE r.code = 'sales_register';

-- ============================================================
-- 4. ИСТОЧНИКИ: Возврат (header + details)
-- ============================================================

-- Header: Возврат
INSERT INTO etl_meta.register_sources
    (register_id, source_code, source_type, mssql_table, priority)
SELECT r.id, 'return_h', 'header', '_Document396', 3
FROM etl_meta.registers r WHERE r.code = 'sales_register';

-- Details: Возврат.Товары (с отрицательным количеством)
INSERT INTO etl_meta.register_sources
    (register_id, source_code, source_type, mssql_table,
     parent_source_id, join_type, join_key_source, join_key_parent, priority)
SELECT
    r.id,
    'return_d',
    'detail',
    '_Document396_VT17855',
    (SELECT id FROM etl_meta.register_sources WHERE source_code = 'return_h'),
    'INNER',
    '_Document396_IDRRef',
    '_IDRRef',
    3
FROM etl_meta.registers r WHERE r.code = 'sales_register';

-- ============================================================
-- 5. МАППИНГ КОЛОНОК: ЧекККМ
-- ============================================================

-- Header columns (check_h)
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_IDRRef', 'document_uid', 'binary_to_uuid'
FROM etl_meta.register_sources s WHERE s.source_code = 'check_h';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Date_Time', 'document_date', 'fix_year'
FROM etl_meta.register_sources s WHERE s.source_code = 'check_h';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Number', 'document_number', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'check_h';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17840RRef', 'store_id', 'binary_to_uuid'
FROM etl_meta.register_sources s WHERE s.source_code = 'check_h';

-- Detail columns (check_d)
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_LineNo', 'line_number', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'check_d';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17846RRef', 'product_id', 'binary_to_uuid'
FROM etl_meta.register_sources s WHERE s.source_code = 'check_d';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17847', 'quantity', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'check_d';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17848', 'price', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'check_d';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17849', 'amount', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'check_d';

-- ============================================================
-- 6. МАППИНГ КОЛОНОК: Реализация (аналогично)
-- ============================================================

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_IDRRef', 'document_uid', 'binary_to_uuid'
FROM etl_meta.register_sources s WHERE s.source_code = 'real_h';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Date_Time', 'document_date', 'fix_year'
FROM etl_meta.register_sources s WHERE s.source_code = 'real_h';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Number', 'document_number', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'real_h';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17860RRef', 'store_id', 'binary_to_uuid'
FROM etl_meta.register_sources s WHERE s.source_code = 'real_h';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_LineNo', 'line_number', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'real_d';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17865RRef', 'product_id', 'binary_to_uuid'
FROM etl_meta.register_sources s WHERE s.source_code = 'real_d';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17866', 'quantity', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'real_d';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17867', 'price', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'real_d';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17868', 'amount', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'real_d';

-- ============================================================
-- 7. МАППИНГ КОЛОНОК: Возврат (с отрицательными значениями)
-- ============================================================

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_IDRRef', 'document_uid', 'binary_to_uuid'
FROM etl_meta.register_sources s WHERE s.source_code = 'return_h';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Date_Time', 'document_date', 'fix_year'
FROM etl_meta.register_sources s WHERE s.source_code = 'return_h';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Number', 'document_number', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'return_h';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17870RRef', 'store_id', 'binary_to_uuid'
FROM etl_meta.register_sources s WHERE s.source_code = 'return_h';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_LineNo', 'line_number', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'return_d';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17875RRef', 'product_id', 'binary_to_uuid'
FROM etl_meta.register_sources s WHERE s.source_code = 'return_d';

-- Отрицательное количество для возвратов
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, is_expression, transform_type)
SELECT s.id, '-1 * _Fld17876', 'quantity', TRUE, NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'return_d';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Fld17877', 'price', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'return_d';

INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, is_expression, transform_type)
SELECT s.id, '-1 * _Fld17878', 'amount', TRUE, NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'return_d';

-- ============================================================
-- 8. UNION: Объединение всех документов продаж
-- ============================================================

INSERT INTO etl_meta.source_unions (register_id, union_code, description, output_columns)
SELECT
    r.id,
    'all_sales',
    'Объединение ЧекККМ + Реализация + Возврат',
    ARRAY['document_uid', 'document_date', 'document_number', 'store_id',
          'line_number', 'product_id', 'quantity', 'price', 'amount']
FROM etl_meta.registers r WHERE r.code = 'sales_register';

-- Добавляем источники в UNION (берём detail-таблицы, т.к. они содержат JOIN с header)
INSERT INTO etl_meta.source_union_members (union_id, source_id, priority)
SELECT
    u.id,
    s.id,
    1
FROM etl_meta.source_unions u, etl_meta.register_sources s
WHERE u.union_code = 'all_sales' AND s.source_code = 'check_d';

INSERT INTO etl_meta.source_union_members (union_id, source_id, priority)
SELECT
    u.id,
    s.id,
    2
FROM etl_meta.source_unions u, etl_meta.register_sources s
WHERE u.union_code = 'all_sales' AND s.source_code = 'real_d';

INSERT INTO etl_meta.source_union_members (union_id, source_id, priority)
SELECT
    u.id,
    s.id,
    3
FROM etl_meta.source_unions u, etl_meta.register_sources s
WHERE u.union_code = 'all_sales' AND s.source_code = 'return_d';

-- ============================================================
-- 9. ЦЕЛЕВЫЕ ТАБЛИЦЫ
-- ============================================================

-- Основная таблица: sales_register (построчно)
INSERT INTO etl_meta.register_targets
    (register_id, target_schema, target_table, union_id, load_mode, upsert_keys)
SELECT
    r.id,
    'public',
    'sales_register',
    u.id,
    'upsert',
    ARRAY['document_uid', 'line_number']
FROM etl_meta.registers r, etl_meta.source_unions u
WHERE r.code = 'sales_register' AND u.union_code = 'all_sales';

-- Агрегированная таблица: sales_daily (по дням)
INSERT INTO etl_meta.register_targets
    (register_id, target_schema, target_table, union_id, load_mode, upsert_keys, pre_load_sql)
SELECT
    r.id,
    'public',
    'sales_daily',
    u.id,
    'upsert',
    ARRAY['sale_date', 'store_id', 'product_id'],
    '
    SELECT
        DATE(document_date) AS sale_date,
        store_id,
        product_id,
        SUM(quantity) AS total_quantity,
        SUM(amount) AS total_amount,
        COUNT(*) AS line_count
    FROM __df__
    GROUP BY DATE(document_date), store_id, product_id
    '
FROM etl_meta.registers r, etl_meta.source_unions u
WHERE r.code = 'sales_register' AND u.union_code = 'all_sales';
