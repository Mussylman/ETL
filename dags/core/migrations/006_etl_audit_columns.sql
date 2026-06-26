-- Migration 006: явные ETL-аудит колонки для sales / sales_positions
--
-- Контекст: до этой миграции в sales существовала только одна колонка `updated_at`,
-- которая в full_period получала retail_snapshot_at, а в incremental — per-row
-- retail.updated_at. Это перегружало одно имя двумя смыслами.
--
-- Новые поля (см. docs/sales_load_modes.md):
--   retail_snapshot_at — MAX(updated_at) из retail на момент full_period
--   retail_updated_at  — per-row retail.updated_at (только incremental)
--   etl_updated_at     — момент когда строка тронута ETL-процессом
--
-- Старая `updated_at` остаётся как legacy и НЕ удаляется этой миграцией.

-- ── sales ──
ALTER TABLE public.sales            ADD COLUMN IF NOT EXISTS retail_snapshot_at timestamp;
ALTER TABLE public.sales            ADD COLUMN IF NOT EXISTS retail_updated_at  timestamp;
ALTER TABLE public.sales            ADD COLUMN IF NOT EXISTS etl_updated_at     timestamp;

-- ── sales_positions ──
-- Дублируем эти три поля и в fact (несмотря на FK sales_id → sales):
-- так BI/Power BI может фильтровать позиции по retail_snapshot_at без join.
ALTER TABLE public.sales_positions  ADD COLUMN IF NOT EXISTS retail_snapshot_at timestamp;
ALTER TABLE public.sales_positions  ADD COLUMN IF NOT EXISTS retail_updated_at  timestamp;
ALTER TABLE public.sales_positions  ADD COLUMN IF NOT EXISTS etl_updated_at     timestamp;

-- ── Backfill из legacy updated_at, если он есть и заполнен ──
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE  table_schema='public' AND table_name='sales' AND column_name='updated_at'
    ) THEN
        UPDATE public.sales
        SET    retail_snapshot_at = updated_at
        WHERE  retail_snapshot_at IS NULL AND updated_at IS NOT NULL;
    END IF;
END $$;

-- Индекс по retail_updated_at — для быстрого MAX() при incremental watermark.
CREATE INDEX IF NOT EXISTS idx_sales_retail_updated_at
    ON public.sales (retail_updated_at) WHERE retail_updated_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sales_retail_snapshot_at
    ON public.sales (retail_snapshot_at) WHERE retail_snapshot_at IS NOT NULL;
