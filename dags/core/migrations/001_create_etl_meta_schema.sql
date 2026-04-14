-- ============================================================
-- ETL Meta Schema - Конфигурация универсальной ETL-системы
-- ============================================================

CREATE SCHEMA IF NOT EXISTS etl_meta;

-- ============================================================
-- 1. РЕГИСТРЫ (главная сущность)
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_meta.registers (
    id              SERIAL PRIMARY KEY,
    code            VARCHAR(100) NOT NULL UNIQUE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,

    default_mode    VARCHAR(50) DEFAULT 'incremental',

    retail_table        VARCHAR(100),
    retail_uid_column   VARCHAR(100),

    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE etl_meta.registers IS 'Регистры данных (sales_register, stock_register и т.д.)';

-- ============================================================
-- 2. ИСТОЧНИКИ ДАННЫХ (таблицы 1С)
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_meta.register_sources (
    id              SERIAL PRIMARY KEY,
    register_id     INTEGER NOT NULL REFERENCES etl_meta.registers(id) ON DELETE CASCADE,

    source_code     VARCHAR(100) NOT NULL,
    source_type     VARCHAR(50) NOT NULL,

    mssql_schema    VARCHAR(100) DEFAULT 'dbo',
    mssql_table     VARCHAR(255) NOT NULL,

    parent_source_id INTEGER REFERENCES etl_meta.register_sources(id),
    join_type       VARCHAR(20),
    join_key_source VARCHAR(100),
    join_key_parent VARCHAR(100),

    where_clause    TEXT,
    priority        INTEGER DEFAULT 0,

    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_register_source_code
    ON etl_meta.register_sources(register_id, source_code);

COMMENT ON TABLE etl_meta.register_sources IS 'Источники данных (таблицы 1С) для регистров';
COMMENT ON COLUMN etl_meta.register_sources.source_type IS 'header / detail / standalone';
COMMENT ON COLUMN etl_meta.register_sources.join_type IS 'INNER / LEFT / RIGHT для связи с parent';

-- ============================================================
-- 3. МАППИНГ КОЛОНОК (source -> target)
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_meta.column_mappings (
    id              SERIAL PRIMARY KEY,
    source_id       INTEGER NOT NULL REFERENCES etl_meta.register_sources(id) ON DELETE CASCADE,

    source_column   VARCHAR(255) NOT NULL,
    is_expression   BOOLEAN DEFAULT FALSE,

    target_column   VARCHAR(255) NOT NULL,
    target_type     VARCHAR(50),

    transform_type  VARCHAR(50),
    transform_params JSONB,

    default_value   TEXT,
    is_nullable     BOOLEAN DEFAULT TRUE,
    is_active       BOOLEAN DEFAULT TRUE,

    created_at      TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE etl_meta.column_mappings IS 'Маппинг колонок из 1С в целевую таблицу';
COMMENT ON COLUMN etl_meta.column_mappings.transform_type IS 'binary_to_uuid / fix_year / cast / constant';

-- ============================================================
-- 4. UNION-ы (объединение источников)
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_meta.source_unions (
    id              SERIAL PRIMARY KEY,
    register_id     INTEGER NOT NULL REFERENCES etl_meta.registers(id) ON DELETE CASCADE,

    union_code      VARCHAR(100) NOT NULL,
    description     TEXT,

    output_columns  TEXT[] NOT NULL,

    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE etl_meta.source_unions IS 'Определение UNION-ов для объединения источников';

-- ============================================================
-- 5. ЧЛЕНЫ UNION-а
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_meta.source_union_members (
    id              SERIAL PRIMARY KEY,
    union_id        INTEGER NOT NULL REFERENCES etl_meta.source_unions(id) ON DELETE CASCADE,
    source_id       INTEGER NOT NULL REFERENCES etl_meta.register_sources(id) ON DELETE CASCADE,

    priority        INTEGER DEFAULT 0,
    where_clause    TEXT,

    is_active       BOOLEAN DEFAULT TRUE
);

COMMENT ON TABLE etl_meta.source_union_members IS 'Связь источников с UNION-ами';

-- ============================================================
-- 6. ЦЕЛЕВЫЕ ТАБЛИЦЫ
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_meta.register_targets (
    id              SERIAL PRIMARY KEY,
    register_id     INTEGER NOT NULL REFERENCES etl_meta.registers(id) ON DELETE CASCADE,

    target_schema   VARCHAR(100) DEFAULT 'public',
    target_table    VARCHAR(255) NOT NULL,

    union_id        INTEGER REFERENCES etl_meta.source_unions(id),
    source_id       INTEGER REFERENCES etl_meta.register_sources(id),

    load_mode       VARCHAR(50) DEFAULT 'upsert',
    upsert_keys     TEXT[],

    pre_load_sql    TEXT,

    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),

    CONSTRAINT chk_target_source CHECK (
        (union_id IS NOT NULL AND source_id IS NULL) OR
        (union_id IS NULL AND source_id IS NOT NULL)
    )
);

COMMENT ON TABLE etl_meta.register_targets IS 'Целевые таблицы для загрузки регистров';
COMMENT ON COLUMN etl_meta.register_targets.load_mode IS 'insert / upsert / replace';

-- ============================================================
-- 7. ИСТОРИЯ ЗАГРУЗОК
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_meta.load_history (
    id              SERIAL PRIMARY KEY,
    register_id     INTEGER NOT NULL REFERENCES etl_meta.registers(id),
    target_id       INTEGER REFERENCES etl_meta.register_targets(id),

    run_mode        VARCHAR(50) NOT NULL,
    start_date      TIMESTAMP,
    end_date        TIMESTAMP,

    status          VARCHAR(50) NOT NULL,
    rows_extracted  INTEGER,
    rows_loaded     INTEGER,

    started_at      TIMESTAMP DEFAULT NOW(),
    finished_at     TIMESTAMP,
    error_message   TEXT,

    checkpoint_value TEXT,

    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_load_history_register
    ON etl_meta.load_history(register_id, started_at DESC);

COMMENT ON TABLE etl_meta.load_history IS 'История загрузок для мониторинга и checkpoint';
