-- 018: справочники — английские имена (REAL_SCHEMA). Таблицы, их sequence, ограничения и индексы
-- переименовываются, id и данные не меняются; все ссылки control plane переводятся на новые имена.
--
-- Одна транзакция: загрузчик находит справочник по имени (refs.dim_links: transform_params.dim
-- или dim_<ключ>, и только если таблица существует). Если бы переименование и перевод ссылок шли
-- раздельно, прогон между ними молча потерял бы ссылку на справочник.
--
-- ClickHouse-реплики переименовываются отдельно (018_dim_english_names.ch.sql, ch_admin) — сразу после
-- этой миграции, при приостановленном analytics_sync. Откат — 018_dim_english_names.down.sql.
BEGIN;

CREATE TEMP TABLE _dim_rename (old_name text PRIMARY KEY, new_name text NOT NULL UNIQUE, ref_key text NOT NULL)
    ON COMMIT DROP;
INSERT INTO _dim_rename VALUES
    ('dim_nomenklatura',  'dim_product',            'nomenklatura'),
    ('dim_sklad',         'dim_warehouse',          'sklad'),
    ('dim_podrazdelenie', 'dim_department',         'podrazdelenie'),
    ('dim_kachestvo',     'dim_quality',            'kachestvo'),
    ('dim_kontragent',    'dim_counterparty',       'kontragent'),
    ('dim_dogovor',       'dim_contract',           'dogovor'),
    ('dim_organizatsiya', 'dim_organization',       'organizatsiya'),
    ('dim_otvetstvennyy', 'dim_responsible_person', 'otvetstvennyy');

DO $$
DECLARE r record; o record; n int;
BEGIN
    -- предусловия: старые есть, новых нет
    FOR r IN SELECT * FROM _dim_rename LOOP
        IF to_regclass('public.' || r.old_name) IS NULL THEN
            RAISE EXCEPTION 'нет public.% — миграция уже применена или схема другая', r.old_name;
        END IF;
        IF to_regclass('public.' || r.new_name) IS NOT NULL THEN
            RAISE EXCEPTION 'public.% уже существует', r.new_name;
        END IF;
    END LOOP;

    -- 1. ссылки фактов на справочники — явно (transform_params.dim), до переименования таблиц
    UPDATE etl_meta.column_mappings cm
       SET transform_params = coalesce(cm.transform_params, '{}'::jsonb) || jsonb_build_object('dim', d.new_name)
      FROM _dim_rename d
     WHERE cm.target_column = 'raw_refs.' || d.ref_key AND NOT coalesce(cm.transform_params, '{}'::jsonb) ? 'dim';
    UPDATE etl_meta.column_mappings cm
       SET transform_params = jsonb_set(cm.transform_params, '{dim}', to_jsonb(d.new_name))
      FROM _dim_rename d
     WHERE cm.transform_params->>'dim' = d.old_name;

    -- 2. таблицы, sequence, ограничения (с их индексами) и прочие индексы
    FOR r IN SELECT * FROM _dim_rename LOOP
        EXECUTE format('ALTER TABLE public.%I RENAME TO %I', r.old_name, r.new_name);
        FOR o IN SELECT c.relname FROM pg_class c JOIN pg_depend d ON d.objid = c.oid
                  WHERE c.relkind = 'S' AND d.refobjid = ('public.' || r.new_name)::regclass AND d.deptype IN ('a', 'i') LOOP
            EXECUTE format('ALTER SEQUENCE public.%I RENAME TO %I', o.relname, replace(o.relname, r.old_name, r.new_name));
        END LOOP;
        FOR o IN SELECT conname FROM pg_constraint WHERE conrelid = ('public.' || r.new_name)::regclass
                  AND conname LIKE r.old_name || '%' LOOP
            EXECUTE format('ALTER TABLE public.%I RENAME CONSTRAINT %I TO %I', r.new_name, o.conname,
                           replace(o.conname, r.old_name, r.new_name));
        END LOOP;
        FOR o IN SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = r.new_name
                  AND indexname LIKE '%' || r.old_name || '%' LOOP
            EXECUTE format('ALTER INDEX public.%I RENAME TO %I', o.indexname, replace(o.indexname, r.old_name, r.new_name));
        END LOOP;
    END LOOP;

    -- 3. реестр справочников (dim_registry): код регистра и таблица цели
    UPDATE etl_meta.registers g SET code = d.new_name, updated_at = now() FROM _dim_rename d WHERE g.code = d.old_name;
    UPDATE etl_meta.register_targets t SET target_table = d.new_name
      FROM _dim_rename d WHERE t.target_schema = 'public' AND t.target_table = d.old_name;

    -- 4. репликация PostgreSQL → ClickHouse (core_pg_to_ch) и lookup'и
    UPDATE etl_meta.ch_sync s SET code = d.new_name, source_object = d.new_name, target_table = d.new_name, updated_at = now()
      FROM _dim_rename d WHERE s.code = d.old_name AND s.source_object = d.old_name AND s.target_table = d.old_name;
    FOR r IN SELECT * FROM _dim_rename LOOP
        UPDATE etl_meta.ch_sync SET lookup = jsonb_set(lookup, '{table}', to_jsonb(regexp_replace(lookup->>'table',
                   '(^|\.)' || r.old_name || '$', '\1' || r.new_name)))
         WHERE lookup->>'table' ~ ('(^|\.)' || r.old_name || '$');
        -- 5. post_load_sql замороженного пути PG-фактов (ROLLBACK_KEEP) — чтобы откат на него не сломался
        UPDATE etl_meta.register_targets SET post_load_sql = regexp_replace(post_load_sql, '\m' || r.old_name || '\M', r.new_name, 'g')
         WHERE post_load_sql ~ ('\m' || r.old_name || '\M');
    END LOOP;

    -- постусловия: в control plane старых имён не осталось
    SELECT count(*) INTO n FROM (
        SELECT code AS v FROM etl_meta.registers UNION ALL SELECT target_table FROM etl_meta.register_targets
        UNION ALL SELECT coalesce(post_load_sql, '') FROM etl_meta.register_targets
        UNION ALL SELECT code || ' ' || coalesce(source_object, '') || ' ' || target_table || ' ' || coalesce(lookup::text, '') FROM etl_meta.ch_sync
        UNION ALL SELECT coalesce(transform_params::text, '') FROM etl_meta.column_mappings) x, _dim_rename d
     WHERE x.v ~ ('\m' || d.old_name || '\M');
    IF n > 0 THEN RAISE EXCEPTION 'в control plane осталось % ссылок на старые имена', n; END IF;
    SELECT count(*) INTO n FROM etl_meta.column_mappings cm, _dim_rename d
     WHERE cm.target_column = 'raw_refs.' || d.ref_key AND cm.transform_params->>'dim' IS DISTINCT FROM d.new_name;
    IF n > 0 THEN RAISE EXCEPTION '% ссылок raw_refs без явного dim', n; END IF;
END $$;

COMMIT;
