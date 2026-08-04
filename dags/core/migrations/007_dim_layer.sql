-- ═══════════════════════════════════════════════════════════════════════
-- 007: DIM-слой (guid→id) для витрины продаж
-- ═══════════════════════════════════════════════════════════════════════
-- Что создаёт:
--   • 8 справочников public.dim_* по единому паттерну
--       id      integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY  — раздаёт ТОЛЬКО справочник
--       guid    uuid NOT NULL UNIQUE  — канонический ключ проекта; без UNIQUE stub-резолв
--                                       (ON CONFLICT (guid) DO NOTHING) наплодит дубли
--       name/code               — NULL пока строка stub, заполняет загрузчик имён
--       is_stub                 — true до обогащения из справочника 1С
--       etl_updated_at          — Asia/Almaty (см. docs/sales_load_modes.md)
--   • 9 FK-колонок в фактах: nullable, БЕЗ NOT NULL и БЕЗ FK-констрейнта —
--     строка факта не ждёт справочник, id проставляет post_load_sql
--     (обоснование: reports/dwh_int_fk_readiness_2026-07-27.md)
--   • индексы под join-ы BI и под чистку устаревших строк
--
-- Идемпотентна: повторный запуск ничего не меняет и не пересоздаёт id.
-- НЕ содержит данных: справочники наполняются stub-резолвом из фактов
-- (post_load_sql) и загрузчиком имён (dags/core/tools/load_dim_names.py).
--
-- Связано: 006_etl_audit_columns.sql, reports/pilot_dim_nomenklatura_plan_2026-07-27.md
-- ═══════════════════════════════════════════════════════════════════════

BEGIN;

-- ─────────────────────────────────────────────────────────────────────
-- 1. Справочники
-- ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.dim_nomenklatura (
    id             integer   GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    guid           uuid      NOT NULL UNIQUE,
    name           text,
    code           text,
    is_stub        boolean   NOT NULL DEFAULT true,
    etl_updated_at timestamp NOT NULL DEFAULT timezone('Asia/Almaty', now())
);

CREATE TABLE IF NOT EXISTS public.dim_sklad (
    id             integer   GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    guid           uuid      NOT NULL UNIQUE,
    name           text,
    code           text,
    is_stub        boolean   NOT NULL DEFAULT true,
    etl_updated_at timestamp NOT NULL DEFAULT timezone('Asia/Almaty', now())
);

CREATE TABLE IF NOT EXISTS public.dim_kontragent (
    id             integer   GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    guid           uuid      NOT NULL UNIQUE,
    name           text,
    code           text,
    is_stub        boolean   NOT NULL DEFAULT true,
    etl_updated_at timestamp NOT NULL DEFAULT timezone('Asia/Almaty', now())
);

CREATE TABLE IF NOT EXISTS public.dim_podrazdelenie (
    id             integer   GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    guid           uuid      NOT NULL UNIQUE,
    name           text,
    code           text,
    is_stub        boolean   NOT NULL DEFAULT true,
    etl_updated_at timestamp NOT NULL DEFAULT timezone('Asia/Almaty', now())
);

CREATE TABLE IF NOT EXISTS public.dim_organizatsiya (
    id             integer   GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    guid           uuid      NOT NULL UNIQUE,
    name           text,
    code           text,
    is_stub        boolean   NOT NULL DEFAULT true,
    etl_updated_at timestamp NOT NULL DEFAULT timezone('Asia/Almaty', now())
);

CREATE TABLE IF NOT EXISTS public.dim_dogovor (
    id             integer   GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    guid           uuid      NOT NULL UNIQUE,
    name           text,
    code           text,
    is_stub        boolean   NOT NULL DEFAULT true,
    etl_updated_at timestamp NOT NULL DEFAULT timezone('Asia/Almaty', now())
);

CREATE TABLE IF NOT EXISTS public.dim_otvetstvennyy (
    id             integer   GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    guid           uuid      NOT NULL UNIQUE,
    name           text,
    code           text,
    is_stub        boolean   NOT NULL DEFAULT true,
    etl_updated_at timestamp NOT NULL DEFAULT timezone('Asia/Almaty', now())
);

CREATE TABLE IF NOT EXISTS public.dim_kachestvo (
    id             integer   GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    guid           uuid      NOT NULL UNIQUE,
    name           text,
    code           text,
    is_stub        boolean   NOT NULL DEFAULT true,
    etl_updated_at timestamp NOT NULL DEFAULT timezone('Asia/Almaty', now())
);

-- ─────────────────────────────────────────────────────────────────────
-- 1b. Добор констрейнтов, если таблица была создана раньше без них.
--     ADD CONSTRAINT IF NOT EXISTS в PG нет — проверяем через pg_constraint.
--     UNIQUE(guid) критичен: на нём держится ON CONFLICT в stub-резолвере.
-- ─────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'dim_nomenklatura', 'dim_sklad', 'dim_kontragent', 'dim_podrazdelenie',
        'dim_organizatsiya', 'dim_dogovor', 'dim_otvetstvennyy', 'dim_kachestvo'
    ] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = r.relnamespace
            WHERE n.nspname = 'public' AND r.relname = t AND c.contype = 'u'
        ) THEN
            EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I UNIQUE (guid)', t, t || '_guid_key');
            RAISE NOTICE '007: добавлен UNIQUE(guid) на %', t;
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = r.relnamespace
            WHERE n.nspname = 'public' AND r.relname = t AND c.contype = 'p'
        ) THEN
            EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I PRIMARY KEY (id)', t, t || '_pkey');
            RAISE NOTICE '007: добавлен PRIMARY KEY(id) на %', t;
        END IF;
    END LOOP;
END $$;

-- ─────────────────────────────────────────────────────────────────────
-- 2. FK-колонки в фактах (nullable, без FK-констрейнта — см. шапку)
--
--    ВАЖНО ПРО ПОРЯДОК: DDL самих фактов (public.sales / sales_positions)
--    не живёт в миграциях — таблицы создаёт Sync конфигуратора из мэппингов
--    (etl_config_app/dao.py::apply_sync_plan). Поэтому 007 применяется ПОСЛЕ
--    Sync. Проверяем явно, чтобы вместо невнятной ошибки ALTER TABLE была
--    понятная причина.
-- ─────────────────────────────────────────────────────────────────────
DO $$
BEGIN
    IF to_regclass('public.sales') IS NULL OR to_regclass('public.sales_positions') IS NULL THEN
        RAISE EXCEPTION
            'Миграция 007: нет таблиц фактов public.sales / public.sales_positions. '
            'Сначала выполните Sync регистра sales в конфигураторе (он создаёт факты '
            'из мэппингов), затем повторите 007. См. dags/core/tools/rebuild_sales.py';
    END IF;
END $$;

ALTER TABLE public.sales
    ADD COLUMN IF NOT EXISTS kontragent_id    integer,
    ADD COLUMN IF NOT EXISTS podrazdelenie_id integer,
    ADD COLUMN IF NOT EXISTS sklad_id         integer,
    ADD COLUMN IF NOT EXISTS organizatsiya_id integer,
    ADD COLUMN IF NOT EXISTS dogovor_id       integer,
    ADD COLUMN IF NOT EXISTS otvetstvennyy_id integer;

ALTER TABLE public.sales_positions
    ADD COLUMN IF NOT EXISTS nomenklatura_id integer,
    ADD COLUMN IF NOT EXISTS sklad_id        integer,
    ADD COLUMN IF NOT EXISTS kachestvo_id    integer;

-- ─────────────────────────────────────────────────────────────────────
-- 3. Индексы
--    organizatsiya_id (1 значение) и kachestvo_id (5 значений) — намеренно
--    БЕЗ индекса: селективность нулевая, планировщик их не возьмёт.
--    idx_..._etl_updated_at — под чистку строк, исчезнувших при
--    перепроведении (post_load_sql, окно 60 мин): 48 мс вместо 566 мс.
-- ─────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_sales_kontragent_id    ON public.sales (kontragent_id);
CREATE INDEX IF NOT EXISTS idx_sales_podrazdelenie_id ON public.sales (podrazdelenie_id);
CREATE INDEX IF NOT EXISTS idx_sales_sklad_id         ON public.sales (sklad_id);
CREATE INDEX IF NOT EXISTS idx_sales_dogovor_id       ON public.sales (dogovor_id);
CREATE INDEX IF NOT EXISTS idx_sales_otvetstvennyy_id ON public.sales (otvetstvennyy_id);

CREATE INDEX IF NOT EXISTS idx_sales_positions_nomenklatura_id ON public.sales_positions (nomenklatura_id);
CREATE INDEX IF NOT EXISTS idx_sales_positions_sklad_id        ON public.sales_positions (sklad_id);
CREATE INDEX IF NOT EXISTS idx_sales_positions_etl_updated_at  ON public.sales_positions (etl_updated_at);

COMMIT;
