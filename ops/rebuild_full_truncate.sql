-- ═══════════════════════════════════════════════════════════════════════
-- ШАГ 1 полного пересбора: очистка витрины продаж
-- ═══════════════════════════════════════════════════════════════════════
-- ВЫПОЛНЯТЬ ТОЛЬКО ПОСЛЕ ПАУЗЫ DAG sales_incremental_5min!
-- Иначе тик увидит пустую sales → watermark упадёт на DEFAULT_SINCE
-- (1970-01-01, data_checker.py:53) и потянет всю retail-таблицу.
--
-- FK на эти таблицы отсутствуют (проверено в pg_constraint) — снимать
-- нечего, TRUNCATE проходит без CASCADE.
--
-- RESTART IDENTITY сбрасывает счётчики id. Это допустимо ТОЛЬКО потому,
-- что пересобирается всё согласованно: факты получают новые *_id из
-- заново созданных справочников. При частичной очистке так делать нельзя.
--
-- НЕ входят в очистку (другой регистр / legacy, не часть витрины продаж):
--   order, order_positions   — регистр 'order', пуст, вне scope
--   salesTEST_dim/_pos, wt_sales, wt_sales_positions, stock_positions
-- ═══════════════════════════════════════════════════════════════════════

BEGIN;

TRUNCATE TABLE
    -- факты
    public.sales_positions,
    public.sales,
    -- справочники (8 шт.)
    public.dim_nomenklatura,
    public.dim_sklad,
    public.dim_kontragent,
    public.dim_podrazdelenie,
    public.dim_organizatsiya,
    public.dim_dogovor,
    public.dim_otvetstvennyy,
    public.dim_kachestvo
RESTART IDENTITY;

COMMIT;

-- контроль: всё должно быть по нулям
SELECT 'sales' t, count(*) FROM public.sales
UNION ALL SELECT 'sales_positions',   count(*) FROM public.sales_positions
UNION ALL SELECT 'dim_nomenklatura',  count(*) FROM public.dim_nomenklatura
UNION ALL SELECT 'dim_sklad',         count(*) FROM public.dim_sklad
UNION ALL SELECT 'dim_kontragent',    count(*) FROM public.dim_kontragent
UNION ALL SELECT 'dim_podrazdelenie', count(*) FROM public.dim_podrazdelenie
UNION ALL SELECT 'dim_organizatsiya', count(*) FROM public.dim_organizatsiya
UNION ALL SELECT 'dim_dogovor',       count(*) FROM public.dim_dogovor
UNION ALL SELECT 'dim_otvetstvennyy', count(*) FROM public.dim_otvetstvennyy
UNION ALL SELECT 'dim_kachestvo',     count(*) FROM public.dim_kachestvo
ORDER BY 1;
