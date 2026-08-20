-- ═══════════════════════════════════════════════════════════════════════
-- ШАГ 5 полного пересбора: end-to-end проверка
-- Все проверки — только SELECT. Ожидаемые значения указаны в комментариях.
-- ═══════════════════════════════════════════════════════════════════════

\echo '=== 5.1 Наполнение таблиц (все > 0; products ~124к из retail) ==='
SELECT 'sales' t, count(*) строк, NULL::bigint stub FROM public.sales
UNION ALL SELECT 'sales_positions',   count(*), NULL FROM public.sales_positions
UNION ALL SELECT 'dim_nomenklatura',  count(*), count(*) FILTER (WHERE is_stub) FROM public.dim_nomenklatura
UNION ALL SELECT 'dim_sklad',         count(*), count(*) FILTER (WHERE is_stub) FROM public.dim_sklad
UNION ALL SELECT 'dim_kontragent',    count(*), count(*) FILTER (WHERE is_stub) FROM public.dim_kontragent
UNION ALL SELECT 'dim_podrazdelenie', count(*), count(*) FILTER (WHERE is_stub) FROM public.dim_podrazdelenie
UNION ALL SELECT 'dim_organizatsiya', count(*), count(*) FILTER (WHERE is_stub) FROM public.dim_organizatsiya
UNION ALL SELECT 'dim_dogovor',       count(*), count(*) FILTER (WHERE is_stub) FROM public.dim_dogovor
UNION ALL SELECT 'dim_otvetstvennyy', count(*), count(*) FILTER (WHERE is_stub) FROM public.dim_otvetstvennyy
UNION ALL SELECT 'dim_kachestvo',     count(*), count(*) FILTER (WHERE is_stub) FROM public.dim_kachestvo
ORDER BY 1;

\echo ''
\echo '=== 5.2 Факты ссылаются на ЖИВЫЕ id (все значения = 0) ==='
SELECT 'orphan: positions.sales_id → sales'          AS проверка,
       count(*) AS нарушений
FROM public.sales_positions p LEFT JOIN public.sales s ON s.id = p.sales_id
WHERE p.sales_id IS NOT NULL AND s.id IS NULL
UNION ALL
SELECT 'orphan: positions.nomenklatura_id → dim', count(*)
FROM public.sales_positions p LEFT JOIN public.dim_nomenklatura d ON d.id = p.nomenklatura_id
WHERE p.nomenklatura_id IS NOT NULL AND d.id IS NULL
UNION ALL
SELECT 'orphan: positions.sklad_id → dim', count(*)
FROM public.sales_positions p LEFT JOIN public.dim_sklad d ON d.id = p.sklad_id
WHERE p.sklad_id IS NOT NULL AND d.id IS NULL
UNION ALL
SELECT 'orphan: positions.kachestvo_id → dim', count(*)
FROM public.sales_positions p LEFT JOIN public.dim_kachestvo d ON d.id = p.kachestvo_id
WHERE p.kachestvo_id IS NOT NULL AND d.id IS NULL
UNION ALL
SELECT 'orphan: sales.kontragent_id → dim', count(*)
FROM public.sales s LEFT JOIN public.dim_kontragent d ON d.id = s.kontragent_id
WHERE s.kontragent_id IS NOT NULL AND d.id IS NULL
UNION ALL
SELECT 'orphan: sales.podrazdelenie_id → dim', count(*)
FROM public.sales s LEFT JOIN public.dim_podrazdelenie d ON d.id = s.podrazdelenie_id
WHERE s.podrazdelenie_id IS NOT NULL AND d.id IS NULL
UNION ALL
SELECT 'orphan: sales.sklad_id → dim', count(*)
FROM public.sales s LEFT JOIN public.dim_sklad d ON d.id = s.sklad_id
WHERE s.sklad_id IS NOT NULL AND d.id IS NULL
UNION ALL
SELECT 'orphan: sales.organizatsiya_id → dim', count(*)
FROM public.sales s LEFT JOIN public.dim_organizatsiya d ON d.id = s.organizatsiya_id
WHERE s.organizatsiya_id IS NOT NULL AND d.id IS NULL
UNION ALL
SELECT 'orphan: sales.dogovor_id → dim', count(*)
FROM public.sales s LEFT JOIN public.dim_dogovor d ON d.id = s.dogovor_id
WHERE s.dogovor_id IS NOT NULL AND d.id IS NULL
UNION ALL
SELECT 'orphan: sales.otvetstvennyy_id → dim', count(*)
FROM public.sales s LEFT JOIN public.dim_otvetstvennyy d ON d.id = s.otvetstvennyy_id
WHERE s.otvetstvennyy_id IS NOT NULL AND d.id IS NULL;

\echo ''
\echo '=== 5.3 Резолв FK отработал: guid есть, а id нет (все = 0) ==='
SELECT 'positions.nomenklatura_id не проставлен' AS проверка, count(*) AS нарушений
FROM public.sales_positions WHERE nomenklatura_id IS NULL AND nomenklatura IS NOT NULL
UNION ALL SELECT 'positions.sales_id не проставлен', count(*)
FROM public.sales_positions WHERE sales_id IS NULL
UNION ALL SELECT 'sales.kontragent_id не проставлен', count(*)
FROM public.sales WHERE kontragent_id IS NULL AND kontragent IS NOT NULL
     AND kontragent <> '00000000-0000-0000-0000-000000000000'::uuid
UNION ALL SELECT 'sales.otvetstvennyy_id не проставлен', count(*)
FROM public.sales WHERE otvetstvennyy_id IS NULL AND otvetstvennyy_uid IS NOT NULL
     AND otvetstvennyy_uid <> '00000000-0000-0000-0000-000000000000'::uuid;

\echo ''
\echo '=== 5.4 МЕТКИ, от которых поедет инкремент ==='
\echo '-- products: метка из retail (retail_updated_at) — стартовая точка справочников'
SELECT count(*) строк,
       count(retail_updated_at) с_меткой,
       min(retail_updated_at) min_метка,
       max(retail_updated_at) AS "MAX_retail_updated_at (watermark справочника)"
FROM public.dim_nomenklatura;

\echo '-- sales: метка из 1С-прогона (retail_snapshot_at) — стартовая точка фактов'
SELECT count(*) строк,
       count(retail_snapshot_at) с_меткой,
       max(retail_snapshot_at) AS "MAX_retail_snapshot_at (watermark фактов)",
       max(retail_updated_at)  AS "MAX_retail_updated_at (NULL после full_period — норма)",
       max(etl_updated_at)     AS последняя_запись_ETL
FROM public.sales;

\echo ''
\echo '=== 5.5 Дубли по natural-ключам (все = 0) ==='
SELECT 'дубли sales(recorder,recorder_type)' проверка, count(*) нарушений FROM (
  SELECT 1 FROM public.sales GROUP BY recorder, recorder_type HAVING count(*)>1) x
UNION ALL
SELECT 'дубли positions(recorder,type,line_no)', count(*) FROM (
  SELECT 1 FROM public.sales_positions GROUP BY recorder, recorder_type, line_no HAVING count(*)>1) x
UNION ALL
SELECT 'дубли dim_nomenklatura(guid)', count(*) FROM (
  SELECT 1 FROM public.dim_nomenklatura GROUP BY guid HAVING count(*)>1) x;
