-- 019: статус сверки 1С / источник ↔ ClickHouse по месяцам (REAL_SCHEMA, только добавление).
-- Пишет DAG reconcile_status (core.tools.reconcile_status): лёгкая проверка — ежечасно (строки + суммы мер),
-- полная — раз в сутки (отпечаток onec_reconcile). Читает страница «Сверка» конфигуратора.
-- Одна строка = таблица × партиция × вид проверки; хранится последнее состояние, не история.
BEGIN;
CREATE TABLE IF NOT EXISTS etl_meta.reconcile_status (
    target_table       varchar(128) NOT NULL,              -- таблица ClickHouse (ch_sync.target_table)
    partition_key      varchar(16)  NOT NULL,              -- YYYYMM; 'all' — таблица без партиций (справочники)
    check_kind         varchar(8)   NOT NULL CHECK (check_kind IN ('light', 'full')),
    status             varchar(8)   NOT NULL CHECK (status IN ('PASS', 'WAIT', 'FAIL')),
    diff_pct           numeric(12, 4),                      -- max(|источник − CH| / |источник|) по строкам и мерам, %
    source_values      jsonb,                               -- {"rows": …, "<мера>": …}
    ch_values          jsonb,
    fixer              text,                                -- кто должен исправить: patch / hot / sweep / цикл
    note               text,
    first_mismatch_at  timestamptz,                         -- когда расхождение впервые замечено (сброс при PASS)
    checked_at         timestamptz  NOT NULL DEFAULT now(),
    duration_ms        integer,
    PRIMARY KEY (target_table, partition_key, check_kind)
);
CREATE INDEX IF NOT EXISTS reconcile_status_partition ON etl_meta.reconcile_status (partition_key);
COMMIT;
