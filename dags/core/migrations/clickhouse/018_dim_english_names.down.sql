-- Откат 018: обратное переименование справочников и ссылок control plane (id и данные не меняются).
-- Выполнять при приостановленном analytics_sync. ClickHouse — обратный RENAME (ниже, под ch_admin).
--
-- Ссылки raw_refs остаются с явным transform_params.dim (на старые имена) — по поведению загрузчика
-- это то же самое, что неявное dim_<ключ> до 018.
BEGIN;

CREATE TEMP TABLE _dim_rename (old_name text PRIMARY KEY, new_name text NOT NULL UNIQUE)
    ON COMMIT DROP;
INSERT INTO _dim_rename VALUES
    ('dim_nomenklatura',  'dim_product'),
    ('dim_sklad',         'dim_warehouse'),
    ('dim_podrazdelenie', 'dim_department'),
    ('dim_kachestvo',     'dim_quality'),
    ('dim_kontragent',    'dim_counterparty'),
    ('dim_dogovor',       'dim_contract'),
    ('dim_organizatsiya', 'dim_organization'),
    ('dim_otvetstvennyy', 'dim_responsible_person');

DO $$
DECLARE r record; o record;
BEGIN
    FOR r IN SELECT * FROM _dim_rename LOOP
        IF to_regclass('public.' || r.new_name) IS NULL OR to_regclass('public.' || r.old_name) IS NOT NULL THEN
            RAISE EXCEPTION 'состояние не после 018: public.% / public.%', r.new_name, r.old_name;
        END IF;
    END LOOP;
    UPDATE etl_meta.column_mappings cm SET transform_params = jsonb_set(cm.transform_params, '{dim}', to_jsonb(d.old_name))
      FROM _dim_rename d WHERE cm.transform_params->>'dim' = d.new_name;
    FOR r IN SELECT * FROM _dim_rename LOOP
        EXECUTE format('ALTER TABLE public.%I RENAME TO %I', r.new_name, r.old_name);
        FOR o IN SELECT c.relname FROM pg_class c JOIN pg_depend d ON d.objid = c.oid
                  WHERE c.relkind = 'S' AND d.refobjid = ('public.' || r.old_name)::regclass AND d.deptype IN ('a', 'i') LOOP
            EXECUTE format('ALTER SEQUENCE public.%I RENAME TO %I', o.relname, replace(o.relname, r.new_name, r.old_name));
        END LOOP;
        FOR o IN SELECT conname FROM pg_constraint WHERE conrelid = ('public.' || r.old_name)::regclass
                  AND conname LIKE r.new_name || '%' LOOP
            EXECUTE format('ALTER TABLE public.%I RENAME CONSTRAINT %I TO %I', r.old_name, o.conname,
                           replace(o.conname, r.new_name, r.old_name));
        END LOOP;
        FOR o IN SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = r.old_name
                  AND indexname LIKE '%' || r.new_name || '%' LOOP
            EXECUTE format('ALTER INDEX public.%I RENAME TO %I', o.indexname, replace(o.indexname, r.new_name, r.old_name));
        END LOOP;
        UPDATE etl_meta.ch_sync SET lookup = jsonb_set(lookup, '{table}', to_jsonb(regexp_replace(lookup->>'table',
                   '(^|\.)' || r.new_name || '$', '\1' || r.old_name)))
         WHERE lookup->>'table' ~ ('(^|\.)' || r.new_name || '$');
        UPDATE etl_meta.register_targets SET post_load_sql = regexp_replace(post_load_sql, '\m' || r.new_name || '\M', r.old_name, 'g')
         WHERE post_load_sql ~ ('\m' || r.new_name || '\M');
    END LOOP;
    UPDATE etl_meta.registers g SET code = d.old_name, updated_at = now() FROM _dim_rename d WHERE g.code = d.new_name;
    UPDATE etl_meta.register_targets t SET target_table = d.old_name
      FROM _dim_rename d WHERE t.target_schema = 'public' AND t.target_table = d.new_name;
    UPDATE etl_meta.ch_sync s SET code = d.old_name, source_object = d.old_name, target_table = d.old_name, updated_at = now()
      FROM _dim_rename d WHERE s.code = d.new_name;
END $$;

COMMIT;

-- ClickHouse (ch_admin), затем права: ch_ddl --code <old> --apply для каждого справочника.
-- RENAME TABLE
--     analytics_poc.dim_product TO analytics_poc.dim_nomenklatura, analytics_poc.dim_product_stage TO analytics_poc.dim_nomenklatura_stage,
--     analytics_poc.dim_warehouse TO analytics_poc.dim_sklad, analytics_poc.dim_warehouse_stage TO analytics_poc.dim_sklad_stage,
--     analytics_poc.dim_department TO analytics_poc.dim_podrazdelenie, analytics_poc.dim_department_stage TO analytics_poc.dim_podrazdelenie_stage,
--     analytics_poc.dim_quality TO analytics_poc.dim_kachestvo, analytics_poc.dim_quality_stage TO analytics_poc.dim_kachestvo_stage,
--     analytics_poc.dim_counterparty TO analytics_poc.dim_kontragent, analytics_poc.dim_counterparty_stage TO analytics_poc.dim_kontragent_stage,
--     analytics_poc.dim_contract TO analytics_poc.dim_dogovor, analytics_poc.dim_contract_stage TO analytics_poc.dim_dogovor_stage,
--     analytics_poc.dim_organization TO analytics_poc.dim_organizatsiya, analytics_poc.dim_organization_stage TO analytics_poc.dim_organizatsiya_stage,
--     analytics_poc.dim_responsible_person TO analytics_poc.dim_otvetstvennyy, analytics_poc.dim_responsible_person_stage TO analytics_poc.dim_otvetstvennyy_stage;
