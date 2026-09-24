-- 013: реестр ключей документов и признак записи фактов первым hop'ом.
--
-- Прямой путь 1С → ClickHouse не хранит фактов в PostgreSQL, но факты ссылаются друг на
-- друга по суррогатному id (возврат → реализация, продажа → заказ). Выдавать id должен
-- ровно один источник, и id документа не меняется никогда. public.sales / public.orders
-- для этого не годятся: period NOT NULL (не завести ключ документа, на который сослались
-- раньше, чем его извлекли) и старый путь удаляет исчезнувшие документы — вернувшийся
-- документ получил бы новый id.
--
-- Здесь — только структура. Засев (id из фактов с сохранением значений) и передача выдачи
-- id делаются в момент переключения регистра, одномоментно с остановкой записи старого пути.
-- Идемпотентна.

BEGIN;

-- чьи id и из какой последовательности выдаются новые. Последовательность — та же, что
-- у таблицы фактов старого пути: даже при ошибке в порядке переключения числа не столкнутся.
CREATE TABLE IF NOT EXISTS etl_meta.doc_key_scope (
    doc_table      varchar(63) PRIMARY KEY,      -- = ref_target в transform_params / own_table шапки
    sequence_name  text        NOT NULL,         -- regclass-имя последовательности id
    issuer         varchar(16) NOT NULL DEFAULT 'pg_facts'
                   CHECK (issuer IN ('pg_facts', 'registry')),
                   -- pg_facts — id выдаёт старый путь (реестр только читает факты)
                   -- registry — id выдаёт реестр (после переключения)
    seeded_at      timestamptz,
    seeded_rows    bigint,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- строки не удаляются никогда: на id ссылаются другие факты и BI
CREATE TABLE IF NOT EXISTS etl_meta.doc_key (
    doc_table      varchar(63) NOT NULL REFERENCES etl_meta.doc_key_scope (doc_table),
    recorder       uuid        NOT NULL,
    recorder_type  integer     NOT NULL,
    id             bigint      NOT NULL,
    is_stub        boolean     NOT NULL DEFAULT false,   -- сослались, а сам документ ещё не извлекали
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (doc_table, recorder, recorder_type),
    UNIQUE (doc_table, id)
);

INSERT INTO etl_meta.doc_key_scope (doc_table, sequence_name) VALUES
    ('sales',  'public.sales_id_seq'),
    ('orders', 'public.orders_id_seq')
ON CONFLICT (doc_table) DO NOTHING;

-- первый hop (ETLEngine в incremental_prod и sales_reconcile) пишет факты регистра в PostgreSQL.
-- Отдельно от is_active: конфиг регистра читает и прямой путь, is_active=false уронил бы его.
ALTER TABLE etl_meta.registers
    ADD COLUMN IF NOT EXISTS pg_fact_write boolean NOT NULL DEFAULT true;

COMMIT;
