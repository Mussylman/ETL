-- 016: исполнитель группы оркестрации.
--
--   ch_sync       — конфигурации etl_meta.ch_sync → ClickHouse (как было);
--   reference_dim — справочники 1С → реестр PostgreSQL (dim_*): все активные регистры
--                   pipeline_type='reference_dim', load_dim_from_config в режиме incremental.
--
-- Загрузка справочников переходит из incremental_prod в analytics_sync: единый DAG ведёт
-- и реестр справочников (position 5), и его перенос в ClickHouse (core_pg_to_ch, 10).
-- Группа создаётся выключенной: включается при переключении. Идемпотентна.

BEGIN;

ALTER TABLE etl_meta.ch_sync_group
    ADD COLUMN IF NOT EXISTS runner varchar(16) NOT NULL DEFAULT 'ch_sync';

DO $$ BEGIN
    ALTER TABLE etl_meta.ch_sync_group ADD CONSTRAINT ch_sync_group_runner_chk
        CHECK (runner IN ('ch_sync', 'reference_dim'));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

INSERT INTO etl_meta.ch_sync_group (sync_group, dag_id, position, is_active, runner, description)
VALUES ('dim_registry', 'analytics_sync', 5, false, 'reference_dim',
        'справочники 1С → реестр PostgreSQL (dim_*), раньше их переноса в ClickHouse')
ON CONFLICT (sync_group) DO NOTHING;

COMMIT;
