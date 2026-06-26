-- ============================================================
-- 004: register_sources.period_column (этап 0.2, правка движка №3)
--
-- Колонка периода для фильтрации в QueryBuilder:
--   NULL  -> '_Period' (легаси-поведение, fallback)
--   ''    -> периодного фильтра НЕТ (справочники _Reference*)
--   иначе -> имя колонки ('_Period' у регистров, '_Date_Time' у _Document*)
-- ============================================================

ALTER TABLE etl_meta.register_sources
    ADD COLUMN IF NOT EXISTS period_column VARCHAR(100);

COMMENT ON COLUMN etl_meta.register_sources.period_column IS
    'Колонка периода для фильтра: NULL=_Period (legacy), ''''=без фильтра, иначе имя колонки (_Date_Time для документов)';
