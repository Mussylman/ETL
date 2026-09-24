-- 014: реестр групп оркестрации — какой DAG какие группы ch_sync ведёт и в каком порядке.
--
-- Единица оркестрации — группа (ch_sync.sync_group), а не таблица. Принадлежность группы
-- DAG'у — атрибут группы, а не каждой конфигурации: переход группы со старого DAG
-- (clickhouse_sync) на единый (analytics_sync) — правка одной строки здесь, без правки кода.
--
-- Сейчас analytics_sync ведёт только shadow прямого пути; второй hop (core_pg_to_ch, retail)
-- остаётся за clickhouse_sync до переключения. Идемпотентна.

BEGIN;

CREATE TABLE IF NOT EXISTS etl_meta.ch_sync_group (
    sync_group   varchar(64) PRIMARY KEY,
    dag_id       varchar(64) NOT NULL,            -- кто ведёт группу
    position     integer     NOT NULL,            -- порядок задач в DAG: справочники раньше фактов
    is_active    boolean     NOT NULL DEFAULT true,
    description  text,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

INSERT INTO etl_meta.ch_sync_group (sync_group, dag_id, position, is_active, description) VALUES
    ('core_pg_to_ch',        'clickhouse_sync', 10, true,  'второй hop PostgreSQL → ClickHouse: справочники и факты'),
    ('retail',               'clickhouse_sync', 20, true,  'retail → ClickHouse (cost_daily)'),
    ('shadow_1c',            'analytics_sync',  30, false, 'shadow прямого пути 1С → ClickHouse; включается отдельно'),
    ('out_of_scope_powerbi', 'none',            90, false, 'OUT OF SCOPE — не активировать')
ON CONFLICT (sync_group) DO NOTHING;

COMMIT;
