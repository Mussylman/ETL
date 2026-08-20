-- ═══════════════════════════════════════════════════════════════════════
-- 010: retail_updated_at в остальных справочниках с retail-источником
-- ═══════════════════════════════════════════════════════════════════════
-- Продолжение 009 (там колонка появилась у dim_nomenklatura).
--
-- Назначение колонки: РОДНАЯ дата изменения из retail (UTC→Almaty), по ней
-- считается watermark инкремента справочников. etl_updated_at для этого не
-- годится — он время НАШЕЙ записи и уедет при любой заливке.
--
-- Колонку заполняет dags/core/tools/set_dim_retail_marks.py — только UPDATE
-- по существующим guid. Имена/коды/is_stub он не трогает: это зона 1С-пути
-- (load_dim_names.py). Строки в dim создают факты (stub-резолв post_load).
--
-- Идемпотентна. Обратима: DROP COLUMN retail_updated_at.
-- ═══════════════════════════════════════════════════════════════════════

ALTER TABLE public.dim_sklad         ADD COLUMN IF NOT EXISTS retail_updated_at timestamp;
ALTER TABLE public.dim_podrazdelenie ADD COLUMN IF NOT EXISTS retail_updated_at timestamp;
ALTER TABLE public.dim_kachestvo     ADD COLUMN IF NOT EXISTS retail_updated_at timestamp;

CREATE INDEX IF NOT EXISTS idx_dim_sklad_retail_updated_at
    ON public.dim_sklad (retail_updated_at) WHERE retail_updated_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dim_podrazdelenie_retail_updated_at
    ON public.dim_podrazdelenie (retail_updated_at) WHERE retail_updated_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dim_kachestvo_retail_updated_at
    ON public.dim_kachestvo (retail_updated_at) WHERE retail_updated_at IS NOT NULL;
