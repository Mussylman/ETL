-- ═══════════════════════════════════════════════════════════════════════
-- 009: retail_updated_at в справочниках — метка изменения на стороне retail
-- ═══════════════════════════════════════════════════════════════════════
-- Зачем отдельная колонка, если есть etl_updated_at:
--   etl_updated_at — время, когда НАШ ETL тронул строку (аудит). На роль
--   watermark не годится: после любой заливки он уедет в её момент, и
--   инкремент перестанет видеть записи, изменённые в retail раньше.
--   retail_updated_at — РОДНАЯ дата изменения из retail (UTC→Almaty),
--   именно по ней считается MAX() при инкременте справочников.
--
-- Решение (см. память проекта / ADR узел 9): из retail берём ТОЛЬКО
-- guid + updated_at — связку и watermark-дату. Каталог и имена не тянем.
--
-- Идемпотентна. Обратима: ALTER TABLE ... DROP COLUMN retail_updated_at.
-- ═══════════════════════════════════════════════════════════════════════

ALTER TABLE public.dim_nomenklatura
    ADD COLUMN IF NOT EXISTS retail_updated_at timestamp;

-- Индекс под MAX(retail_updated_at) — то же назначение, что у
-- idx_sales_retail_updated_at из миграции 006.
CREATE INDEX IF NOT EXISTS idx_dim_nomenklatura_retail_updated_at
    ON public.dim_nomenklatura (retail_updated_at)
    WHERE retail_updated_at IS NOT NULL;
