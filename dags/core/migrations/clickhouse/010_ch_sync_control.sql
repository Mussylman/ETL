-- Control plane синхронизации ClickHouse. Живёт в том же PostgreSQL, что и остальной ETL.
-- В ClickHouse управляющих таблиц нет и не будет: там только аналитические данные.
--
-- Это НЕ второй control plane и не второй ETL. Это конфигурация второго hop'а:
--   hop 1 (уже есть): 1С MSSQL → PostgreSQL, описан в registers/register_sources/register_targets
--   hop 2 (здесь):    любой источник → ClickHouse
-- Разделены потому, что register_targets.register_id NOT NULL — у cost_daily из retail
-- и у витрин MSSQL PowerBI регистра 1С нет, и выдумывать его означало бы врать о
-- происхождении данных.
--
-- etl_meta.load_history первого hop'а не изменяется ни схемой, ни данными.

CREATE TABLE IF NOT EXISTS etl_meta.ch_sync (
    id                     serial PRIMARY KEY,
    code                   varchar NOT NULL UNIQUE,
    description            text,

    -- ИСТОЧНИК. Креды не здесь — только conn_id из Airflow.
    source_conn_id         varchar NOT NULL,
    source_type            varchar NOT NULL,
    source_schema          varchar,
    source_object          varchar,          -- таблица или VIEW
    source_query           text,             -- либо явный SELECT, взаимоисключающе

    -- ЦЕЛЬ. partition_expr — выражение ТОЛЬКО ClickHouse, на источнике не выполняется:
    -- источнику адаптер строит свой диалект из partition_column + partition_granularity.
    target_database        varchar NOT NULL DEFAULT 'analytics_poc',
    target_table           varchar NOT NULL,
    partition_expr         varchar NOT NULL DEFAULT 'tuple()',
    order_by               text[]  NOT NULL,

    -- ЗАГРУЗКА
    load_mode              varchar NOT NULL,
    partition_column       varchar,
    partition_granularity  varchar,
    watermark_column       varchar,
    business_key           text[],
    batch_size             integer NOT NULL DEFAULT 100000,

    -- Пустая партиция в источнике: остановиться или штатно очистить цель.
    -- По умолчанию fail — «данные пропали» чаще означает сломанный источник,
    -- чем законное отсутствие строк.
    empty_partition_policy varchar NOT NULL DEFAULT 'fail',

    -- ПОИСК ЗАТРОНУТЫХ ПАРТИЦИЙ
    -- hot_window: последние N партиций проверяются отпечатком каждый запуск.
    -- sweep_interval_min: как часто отпечатки сверяются по ВСЕЙ истории — только
    -- это и ловит удаление строки, у которой watermark'а не осталось.
    hot_window             integer NOT NULL DEFAULT 3,
    sweep_interval_min     integer NOT NULL DEFAULT 60,

    -- СВЕРКА. Ответственность разделена намеренно:
    --   checksum_columns — ключи и измерения, MD5 по канонической строке. Ловит
    --     изменение любого id при неизменных суммах, переезд между партициями, удаление.
    --   measure_columns  — меры, sum и sum квадратов. Меры в MD5 не включаются:
    --     текстовое представление numeric в PostgreSQL и Decimal в ClickHouse
    --     расходится хвостовыми нулями, и сумма ломалась бы на форматировании.
    checksum_columns       text[],
    measure_columns        text[],
    reconcile_metrics      jsonb NOT NULL DEFAULT '{}'::jsonb,

    priority               integer NOT NULL DEFAULT 100,
    is_active              boolean NOT NULL DEFAULT false,
    created_at             timestamp DEFAULT now(),
    updated_at             timestamp DEFAULT now(),

    CONSTRAINT ch_sync_source_one_of   CHECK ((source_object IS NULL) <> (source_query IS NULL)),
    CONSTRAINT ch_sync_part_needs_col  CHECK (load_mode <> 'partitioned' OR partition_column IS NOT NULL),
    CONSTRAINT ch_sync_source_type     CHECK (source_type IN ('postgres','mssql')),
    CONSTRAINT ch_sync_load_mode       CHECK (load_mode IN ('full','partitioned')),
    CONSTRAINT ch_sync_granularity     CHECK (partition_granularity IS NULL
                                              OR partition_granularity IN ('month','day')),
    CONSTRAINT ch_sync_empty_policy    CHECK (empty_partition_policy IN ('fail','clear'))
);

-- Из этой таблицы генерируется DDL таблицы ClickHouse. Без неё «подключить объект
-- записью конфигурации» невозможно — CREATE TABLE всё равно писали бы руками.
CREATE TABLE IF NOT EXISTS etl_meta.ch_sync_columns (
    id            serial PRIMARY KEY,
    sync_id       integer NOT NULL REFERENCES etl_meta.ch_sync(id) ON DELETE CASCADE,
    ordinal       integer NOT NULL,
    source_expr   varchar NOT NULL,   -- 'coalesce(s.sklad_id, 0)' либо просто 'sklad_id'
    target_column varchar NOT NULL,
    target_type   varchar NOT NULL,   -- 'UInt32', 'Decimal(18,4)', 'LowCardinality(String)'
    codec         varchar,
    CONSTRAINT ch_sync_columns_uk_col  UNIQUE (sync_id, target_column),
    CONSTRAINT ch_sync_columns_uk_ord  UNIQUE (sync_id, ordinal)
);

-- Состояние каждой партиции цели. Отсюда движок знает, что уже сошлось,
-- с каким watermark'ом и с каким отпечатком.
CREATE TABLE IF NOT EXISTS etl_meta.ch_sync_partition_state (
    id                 bigserial PRIMARY KEY,
    sync_id            integer NOT NULL REFERENCES etl_meta.ch_sync(id) ON DELETE CASCADE,
    partition_key      varchar NOT NULL,     -- '202608'; для load_mode=full — 'all'
    status             varchar NOT NULL,     -- ok | stale | loading | failed
    row_count          bigint,
    source_watermark   timestamp,
    source_fingerprint jsonb,                -- {rows, checksum, sums, sq_sums}
    last_success_at    timestamp,
    last_checked_at    timestamp,
    last_error         text,
    CONSTRAINT ch_sync_pstate_uk     UNIQUE (sync_id, partition_key),
    CONSTRAINT ch_sync_pstate_status CHECK (status IN ('ok','stale','loading','failed'))
);

-- Журнал запусков второго hop'а. Отдельный от load_history, чтобы не трогать
-- работающий журнал первого: там register_id NOT NULL, и ослаблять его ради
-- ClickHouse означало бы менять контракт живого ETL.
CREATE TABLE IF NOT EXISTS etl_meta.ch_sync_history (
    id             bigserial PRIMARY KEY,
    sync_id        integer NOT NULL REFERENCES etl_meta.ch_sync(id),
    run_mode       varchar NOT NULL,      -- plan | apply | sweep
    partition_key  varchar,               -- NULL = прогон целиком
    status         varchar NOT NULL,      -- success | failed | skipped
    rows_source    bigint,
    rows_target    bigint,
    method         varchar,               -- REPLACE PARTITION | MOVE PARTITION TO TABLE
    reconcile      jsonb,                 -- полный результат сверки, включая расхождения
    started_at     timestamp NOT NULL DEFAULT now(),
    finished_at    timestamp,
    duration_ms    integer,
    error_message  text,
    CONSTRAINT ch_sync_history_status CHECK (status IN ('success','failed','skipped'))
);

CREATE INDEX IF NOT EXISTS idx_ch_sync_history_sync   ON etl_meta.ch_sync_history (sync_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ch_sync_pstate_stale   ON etl_meta.ch_sync_partition_state (sync_id, status);
