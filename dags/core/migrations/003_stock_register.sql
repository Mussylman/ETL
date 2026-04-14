-- ============================================================
-- Регистр остатков (stock_register)
-- Основа: регистр накопления 1С
-- Дополнительно: документы движения для обогащения
-- ============================================================

-- ВАЖНО: Замени placeholder-ы на реальные имена таблиц:
--   _AccumRgXXXXX      -> имя регистра накопления остатков
--   _DocumentYYY       -> документы движения (Поступление, Списание и т.д.)
--   _FldZZZZZ          -> имена полей в таблицах

-- ============================================================
-- 1. Регистр
-- ============================================================
INSERT INTO etl_meta.registers (code, name, description, default_mode, retail_table, retail_uid_column)
VALUES (
    'stock_register',
    'Регистр остатков',
    'Остатки товаров на складах из регистра накопления 1С',
    'full_period',
    NULL,  -- для остатков обычно нет retail-таблицы
    NULL
);

-- ============================================================
-- 2. ИСТОЧНИК: Регистр накопления остатков (основной)
-- ============================================================

-- Регистр накопления (standalone — без JOIN)
INSERT INTO etl_meta.register_sources
    (register_id, source_code, source_type, mssql_table, priority)
SELECT r.id, 'stock_reg', 'standalone', '_AccumRgXXXXX', 1  -- ЗАМЕНИ _AccumRgXXXXX
FROM etl_meta.registers r WHERE r.code = 'stock_register';

-- ============================================================
-- 3. МАППИНГ КОЛОНОК: Регистр остатков
-- ============================================================

-- Период
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Period', 'period', 'fix_year'
FROM etl_meta.register_sources s WHERE s.source_code = 'stock_reg';

-- Регистратор (документ-основание)
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_RecorderRRef', 'document_uid', 'binary_to_uuid'
FROM etl_meta.register_sources s WHERE s.source_code = 'stock_reg';

-- Номер строки
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_LineNo', 'line_number', NULL
FROM etl_meta.register_sources s WHERE s.source_code = 'stock_reg';

-- Вид движения (приход/расход)
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_RecordKind', 'record_kind', NULL  -- 0=приход, 1=расход
FROM etl_meta.register_sources s WHERE s.source_code = 'stock_reg';

-- Активность записи
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_Active', 'is_active', 'binary_to_bool'
FROM etl_meta.register_sources s WHERE s.source_code = 'stock_reg';

-- Склад (измерение)
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_FldXXXX1RRef', 'store_id', 'binary_to_uuid'  -- ЗАМЕНИ _FldXXXX1RRef
FROM etl_meta.register_sources s WHERE s.source_code = 'stock_reg';

-- Номенклатура (измерение)
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_FldXXXX2RRef', 'product_id', 'binary_to_uuid'  -- ЗАМЕНИ _FldXXXX2RRef
FROM etl_meta.register_sources s WHERE s.source_code = 'stock_reg';

-- Количество (ресурс)
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_FldXXXX3', 'quantity', NULL  -- ЗАМЕНИ _FldXXXX3
FROM etl_meta.register_sources s WHERE s.source_code = 'stock_reg';

-- Сумма (ресурс, если есть)
INSERT INTO etl_meta.column_mappings (source_id, source_column, target_column, transform_type)
SELECT s.id, '_FldXXXX4', 'amount', NULL  -- ЗАМЕНИ _FldXXXX4 или удали если нет
FROM etl_meta.register_sources s WHERE s.source_code = 'stock_reg';

-- ============================================================
-- 4. ЦЕЛЕВЫЕ ТАБЛИЦЫ
-- ============================================================

-- Детальная таблица: stock_movements (все движения)
INSERT INTO etl_meta.register_targets
    (register_id, target_schema, target_table, source_id, load_mode, upsert_keys)
SELECT
    r.id,
    'public',
    'stock_movements',
    s.id,
    'upsert',
    ARRAY['document_uid', 'line_number']
FROM etl_meta.registers r, etl_meta.register_sources s
WHERE r.code = 'stock_register' AND s.source_code = 'stock_reg';

-- Агрегированная таблица: stock_current (текущие остатки)
INSERT INTO etl_meta.register_targets
    (register_id, target_schema, target_table, source_id, load_mode, upsert_keys, pre_load_sql)
SELECT
    r.id,
    'public',
    'stock_current',
    s.id,
    'replace',  -- полная перезагрузка текущих остатков
    ARRAY['store_id', 'product_id'],
    '
    SELECT
        store_id,
        product_id,
        SUM(CASE WHEN record_kind = 0 THEN quantity ELSE -quantity END) AS current_quantity,
        SUM(CASE WHEN record_kind = 0 THEN amount ELSE -amount END) AS current_amount,
        MAX(period) AS last_movement_date
    FROM __df__
    WHERE is_active = TRUE
    GROUP BY store_id, product_id
    HAVING SUM(CASE WHEN record_kind = 0 THEN quantity ELSE -quantity END) != 0
    '
FROM etl_meta.registers r, etl_meta.register_sources s
WHERE r.code = 'stock_register' AND s.source_code = 'stock_reg';

-- Агрегированная таблица: stock_daily (остатки по дням)
INSERT INTO etl_meta.register_targets
    (register_id, target_schema, target_table, source_id, load_mode, upsert_keys, pre_load_sql)
SELECT
    r.id,
    'public',
    'stock_daily',
    s.id,
    'upsert',
    ARRAY['stock_date', 'store_id', 'product_id'],
    '
    SELECT
        DATE(period) AS stock_date,
        store_id,
        product_id,
        SUM(CASE WHEN record_kind = 0 THEN quantity ELSE 0 END) AS income_qty,
        SUM(CASE WHEN record_kind = 1 THEN quantity ELSE 0 END) AS expense_qty,
        SUM(CASE WHEN record_kind = 0 THEN quantity ELSE -quantity END) AS balance_change
    FROM __df__
    WHERE is_active = TRUE
    GROUP BY DATE(period), store_id, product_id
    '
FROM etl_meta.registers r, etl_meta.register_sources s
WHERE r.code = 'stock_register' AND s.source_code = 'stock_reg';


-- ============================================================
-- DDL для целевых таблиц (выполнить отдельно)
-- ============================================================

/*
-- Движения товаров
CREATE TABLE IF NOT EXISTS public.stock_movements (
    document_uid    UUID NOT NULL,
    line_number     INTEGER NOT NULL,
    period          TIMESTAMP,
    record_kind     SMALLINT,  -- 0=приход, 1=расход
    is_active       BOOLEAN,
    store_id        UUID,
    product_id      UUID,
    quantity        NUMERIC(15,3),
    amount          NUMERIC(15,2),
    etl_loaded_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (document_uid, line_number)
);

-- Текущие остатки
CREATE TABLE IF NOT EXISTS public.stock_current (
    store_id            UUID NOT NULL,
    product_id          UUID NOT NULL,
    current_quantity    NUMERIC(15,3),
    current_amount      NUMERIC(15,2),
    last_movement_date  TIMESTAMP,
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (store_id, product_id)
);

-- Остатки по дням
CREATE TABLE IF NOT EXISTS public.stock_daily (
    stock_date      DATE NOT NULL,
    store_id        UUID NOT NULL,
    product_id      UUID NOT NULL,
    income_qty      NUMERIC(15,3),
    expense_qty     NUMERIC(15,3),
    balance_change  NUMERIC(15,3),
    etl_loaded_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (stock_date, store_id, product_id)
);
*/
