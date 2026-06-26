-- ═══════════════════════════════════════════════════════════════════
-- Минимальный атомарный шаг: новая модель «Витрина / Dataset»
-- Все колонки NULLABLE — backward-compat с существующими данными.
-- ═══════════════════════════════════════════════════════════════════

-- registers.pipeline_type — тип шаблона создания витрины
--   accumrg_with_documents | accumrg_flat | document_header_detail |
--   reference_dim | info_register_history | custom | NULL (legacy)
ALTER TABLE etl_meta.registers
    ADD COLUMN IF NOT EXISTS pipeline_type VARCHAR(64);

-- column_mappings.target_id — явная привязка маппинга к target
-- column_mappings.register_id — денормализация для быстрых запросов
-- column_mappings.is_required — обязательное поле
-- column_mappings.is_auto — создан автоматически
ALTER TABLE etl_meta.column_mappings
    ADD COLUMN IF NOT EXISTS target_id   INTEGER,
    ADD COLUMN IF NOT EXISTS register_id INTEGER,
    ADD COLUMN IF NOT EXISTS is_required BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_auto     BOOLEAN NOT NULL DEFAULT TRUE;

-- FK на targets/registers — мягкие, ON DELETE SET NULL (не блокировать)
DO $$
BEGIN
    -- target_id FK
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema='etl_meta'
          AND table_name='column_mappings'
          AND constraint_name='column_mappings_target_id_fkey'
    ) THEN
        ALTER TABLE etl_meta.column_mappings
            ADD CONSTRAINT column_mappings_target_id_fkey
            FOREIGN KEY (target_id)
            REFERENCES etl_meta.register_targets(id)
            ON DELETE SET NULL;
    END IF;

    -- register_id FK
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema='etl_meta'
          AND table_name='column_mappings'
          AND constraint_name='column_mappings_register_id_fkey'
    ) THEN
        ALTER TABLE etl_meta.column_mappings
            ADD CONSTRAINT column_mappings_register_id_fkey
            FOREIGN KEY (register_id)
            REFERENCES etl_meta.registers(id)
            ON DELETE CASCADE;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_col_mappings_target_id   ON etl_meta.column_mappings(target_id);
CREATE INDEX IF NOT EXISTS idx_col_mappings_register_id ON etl_meta.column_mappings(register_id);

-- register_targets.parent_target_id — связь dim ← fact (для UI группировки)
ALTER TABLE etl_meta.register_targets
    ADD COLUMN IF NOT EXISTS parent_target_id INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema='etl_meta'
          AND table_name='register_targets'
          AND constraint_name='register_targets_parent_target_id_fkey'
    ) THEN
        ALTER TABLE etl_meta.register_targets
            ADD CONSTRAINT register_targets_parent_target_id_fkey
            FOREIGN KEY (parent_target_id)
            REFERENCES etl_meta.register_targets(id)
            ON DELETE SET NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_register_targets_parent_target_id
    ON etl_meta.register_targets(parent_target_id);

-- Проверка
SELECT
    (SELECT count(*) FROM information_schema.columns
     WHERE table_schema='etl_meta' AND table_name='registers' AND column_name='pipeline_type') AS reg_pipeline_type,
    (SELECT count(*) FROM information_schema.columns
     WHERE table_schema='etl_meta' AND table_name='column_mappings'
       AND column_name IN ('target_id','register_id','is_required','is_auto')) AS cm_new_cols,
    (SELECT count(*) FROM information_schema.columns
     WHERE table_schema='etl_meta' AND table_name='register_targets' AND column_name='parent_target_id') AS tgt_parent_id
;
