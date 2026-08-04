-- ═══════════════════════════════════════════════════════════════════════
-- 008: регистрация dim-справочников в etl_meta (видимость в UI конфигуратора)
-- ═══════════════════════════════════════════════════════════════════════
-- Проблема: справочники создавались напрямую SQL (миграция 007) и наполнялись
-- stub-резолвом + загрузчиком имён, минуя конфигуратор. UI строит список из
-- etl_meta.registers (app.py::register_list → dao.list_registers), а не из
-- таблиц базы — поэтому восемь наполненных справочников не отображались.
--
-- Эта миграция заводит для каждого dim триаду:
--     registers        — сам справочник (pipeline_type='reference_dim')
--     register_sources — источник _Reference* в 1С (period_column='' —
--                        у справочников нет периодного фильтра, см. 004)
--     register_targets — целевая таблица public.dim_* (role=dimension,
--                        upsert_keys={guid} — канонический ключ проекта)
--     column_mappings  — guid←_IDRRef, name←_Description, code←_Code
--
-- ЗАЧЕМ МЭППИНГИ, если справочники грузит load_dim_names.py:
-- без них Sync конфигуратора считал бы колонки guid/name/code «лишними»
-- (их нет в мэппингах) и предложил бы DROP. С мэппингами план Sync пустой.
-- Служебный is_stub защищён отдельно — dao.SYSTEM_COLS.
--
-- Номера _Reference* верны для базы UPP_JAN. В другой базе 1С они другие:
-- поправьте здесь либо положитесь на load_dim_names.py — он резолвит таблицу
-- через meta API по русскому имени, а номер использует лишь как fallback.
--
-- Идемпотентна: повторный запуск ничего не дублирует.
-- ═══════════════════════════════════════════════════════════════════════

BEGIN;

DO $$
DECLARE
    rec        record;
    v_reg_id   integer;
    v_src_id   integer;
    v_tgt_id   integer;
BEGIN
    FOR rec IN
        SELECT * FROM (VALUES
            ('dim_nomenklatura',  'Номенклатура',           'Справочник.Номенклатура',         '_Reference123'),
            ('dim_sklad',         'Склады',                 'Справочник.Склады',               '_Reference169'),
            ('dim_kontragent',    'Контрагенты',            'Справочник.Контрагенты',          '_Reference108'),
            ('dim_podrazdelenie', 'Подразделения',          'Справочник.Подразделения',        '_Reference141'),
            ('dim_organizatsiya', 'Организации',            'Справочник.Организации',          '_Reference131'),
            ('dim_dogovor',       'Договоры контрагентов',  'Справочник.ДоговорыКонтрагентов', '_Reference75'),
            ('dim_otvetstvennyy', 'Ответственные',          'Справочник.Пользователи',         '_Reference145'),
            ('dim_kachestvo',     'Качество',               'Справочник.Качество',             '_Reference97')
        ) AS t(code, ru_name, onec_name, mssql_table)
    LOOP
        -- 1. Регистр
        SELECT id INTO v_reg_id FROM etl_meta.registers WHERE code = rec.code;
        IF v_reg_id IS NULL THEN
            INSERT INTO etl_meta.registers
                (code, name, description, default_mode, pipeline_type, is_active)
            VALUES (
                rec.code,
                rec.ru_name,
                'Справочник 1С: ' || rec.onec_name ||
                '. Наполняется stub-резолвом из фактов (post_load_sql) и именами '
                'через dags/core/tools/load_dim_names.py',
                'full_period',
                'reference_dim',
                TRUE
            )
            RETURNING id INTO v_reg_id;
            RAISE NOTICE '008: зарегистрирован справочник %', rec.code;
        END IF;

        -- 2. Источник в 1С
        SELECT id INTO v_src_id
        FROM etl_meta.register_sources
        WHERE register_id = v_reg_id AND source_code = 'reference';
        IF v_src_id IS NULL THEN
            INSERT INTO etl_meta.register_sources
                (register_id, source_code, source_type, mssql_schema, mssql_table,
                 onec_name, period_column, priority, is_active)
            VALUES (v_reg_id, 'reference', 'standalone', 'dbo', rec.mssql_table,
                    rec.onec_name, '', 0, TRUE)
            RETURNING id INTO v_src_id;
        END IF;

        -- 3. Целевая таблица
        SELECT id INTO v_tgt_id
        FROM etl_meta.register_targets
        WHERE register_id = v_reg_id AND target_table = rec.code;
        IF v_tgt_id IS NULL THEN
            INSERT INTO etl_meta.register_targets
                (register_id, target_schema, target_table, source_id, load_mode,
                 upsert_keys, include_columns, target_role, priority, is_active)
            VALUES (v_reg_id, 'public', rec.code, v_src_id, 'upsert',
                    ARRAY['guid'], ARRAY['guid', 'name', 'code'], 'dimension', 0, TRUE)
            RETURNING id INTO v_tgt_id;
        END IF;

        -- 4. Мэппинги (без них Sync предложит дропнуть колонки)
        INSERT INTO etl_meta.column_mappings
            (source_id, source_column, target_column, target_type, transform_type,
             is_nullable, is_active, register_id, target_id, is_auto)
        SELECT v_src_id, m.src, m.tgt, m.typ, m.tr, m.nullable, TRUE, v_reg_id, v_tgt_id, TRUE
        FROM (VALUES
            ('_IDRRef',      'guid', 'uuid', 'binary_to_uuid', FALSE),
            ('_Description', 'name', 'text', NULL,             TRUE),
            ('_Code',        'code', 'text', NULL,             TRUE)
        ) AS m(src, tgt, typ, tr, nullable)
        WHERE NOT EXISTS (
            SELECT 1 FROM etl_meta.column_mappings cm
            WHERE cm.source_id = v_src_id AND cm.target_column = m.tgt
        );
    END LOOP;
END $$;

COMMIT;
