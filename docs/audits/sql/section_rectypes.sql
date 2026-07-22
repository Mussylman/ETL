-- ============================================================================
-- АУДИТ SALES ETL — Секция «Типы документов recorder_type (п. 4 ТЗ)»
-- Дата: 2026-07-21, ~12:3x–13:xx Asia/Almaty (07:3x–08:xx UTC)
-- Режим: READ-ONLY (только SELECT; PG-сессии default_transaction_read_only=on)
-- Подключения:
--   test   = 10.10.1.142:5432/test  (витрина public.sales/_positions + etl_meta)
--   ims_db = 10.10.1.99:5432/ims_db (retail-источник, updated_at = naive UTC)
--   mssql  = 10.10.1.61:1433/UPP_JAN (1С; _Period хранится с годом +2000: 4026=2026;
--            значения _Period — локальное время Almaty, как и DWH period после fix_year)
-- Фильтр движка (реплика из dags/core/builder/query_builder.py::build_accumrg_with_documents):
--   WHERE [sales].[_Period] BETWEEN :start AND :end   -- и ВСЁ.
--   where_clause у main-источника (etl_meta.register_sources id=256) = NULL,
--   фильтра по _Active НЕТ, фильтра по _RecorderTRef в WHERE НЕТ (типы — только в ON у LEFT JOIN).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 0. Конфиг движка (что реально фильтруется)
-- ---------------------------------------------------------------------------

-- [test] Источники регистра sales (id=62): where_clause everywhere = NULL
SELECT id, register_id, source_code, source_type, mssql_schema, mssql_table,
       period_column, where_clause, join_type, is_active
FROM etl_meta.register_sources WHERE register_id=62 ORDER BY id;
-- => main id=256 _AccumRg17844 standalone, where_clause=NULL, period_column=NULL (движок падает в '_Period');
--    headers: _Document254, _Document415, _Document476; VT: 254_VT3991, 254_VT4045, 415_VT11053, 415_VT11106, 476_VT13626
--    => "известные" типы по конфигу = 254, 415, 476. Тип 352 header'а НЕ имеет.

-- [test] Маппинг _Active
SELECT cm.id, rs.source_code, cm.source_column, cm.target_column
FROM etl_meta.column_mappings cm JOIN etl_meta.register_sources rs ON rs.id=cm.source_id
WHERE rs.register_id=62 AND cm.source_column='_Active';
-- => id=1364: _Active -> is_active (маппинг есть), НО:

-- [test] include_columns целей — is_active отсутствует в обоих targets
SELECT id, target_table, include_columns FROM etl_meta.register_targets WHERE register_id=62 ORDER BY id;
-- => sales: [...без is_active...], sales_positions: [...без is_active...]
-- ВЫВОД: движок НЕ фильтрует _Active и НЕ грузит is_active в DWH:
--   строки с _Active=0x00 (если есть) грузятся в sales_positions неотличимо от активных.


-- [test@142] 0.re: регистр sales — pipeline_type, id
SELECT id, code, pipeline_type, is_active FROM etl_meta.registers WHERE code='sales' OR id=62

-- [test@142] 0.re: источники регистра 62 — where_clause/period_column (фильтры движка)
SELECT id, source_code, source_type, mssql_table, period_column, where_clause, join_type, is_active
       FROM etl_meta.register_sources WHERE register_id=62 ORDER BY id

-- [test@142] 0.re: маппинги main AccumRg (id=256) — какие _Fld идут в stoimost/nds/kolichestvo
SELECT cm.source_column, cm.target_column, cm.is_expression
       FROM etl_meta.column_mappings cm WHERE cm.source_id=256 ORDER BY cm.target_column

-- [test@142] 0.re: откуда берётся summa/nds по всем источникам регистра 62
SELECT rs.source_code, rs.source_type, cm.source_column, cm.target_column
       FROM etl_meta.column_mappings cm JOIN etl_meta.register_sources rs ON rs.id=cm.source_id
       WHERE rs.register_id=62 AND cm.target_column IN ('summa','nds','stoimost')
       ORDER BY cm.target_column, rs.id

-- [test@142] 0.re: targets регистра 62 — include_columns (ищем is_active)
SELECT id, target_table, target_role, include_columns FROM etl_meta.register_targets WHERE register_id=62 ORDER BY id

-- [test@142] 1.1: DWH полный объём — по recorder_type: docs, positions, суммы (NaN -> NULL через NULLIF)
WITH d AS (
  SELECT recorder_type, COUNT(DISTINCT recorder) AS docs, MIN(period) AS min_period, MAX(period) AS max_period
  FROM public.sales GROUP BY recorder_type
),
p AS (
  SELECT recorder_type,
         COUNT(*) AS positions,
         SUM(NULLIF(stoimost, 'NaN'::float8)) AS sum_stoimost,
         SUM(NULLIF(nds,      'NaN'::float8)) AS sum_nds,
         SUM(NULLIF(summa,    'NaN'::float8)) AS sum_summa,
         COUNT(*) FILTER (WHERE stoimost = 'NaN'::float8) AS nan_stoimost,
         COUNT(*) FILTER (WHERE summa    = 'NaN'::float8) AS nan_summa
  FROM public.sales_positions GROUP BY recorder_type
)
SELECT d.recorder_type, d.docs, p.positions, p.sum_stoimost, p.sum_nds, p.sum_summa,
       p.nan_stoimost, p.nan_summa, d.min_period, d.max_period
FROM d FULL JOIN p USING (recorder_type) ORDER BY recorder_type

-- [test@142] 1.2: DWH срез period >= '2026-06-25' — по recorder_type (positions через join к sales по recorder+type)
WITH d AS (
  SELECT recorder_type, COUNT(DISTINCT recorder) AS docs
  FROM public.sales WHERE period >= '2026-06-25' GROUP BY recorder_type
),
p AS (
  SELECT sp.recorder_type,
         COUNT(*) AS positions,
         SUM(NULLIF(sp.stoimost, 'NaN'::float8)) AS sum_stoimost,
         SUM(NULLIF(sp.nds,      'NaN'::float8)) AS sum_nds,
         SUM(NULLIF(sp.summa,    'NaN'::float8)) AS sum_summa
  FROM public.sales_positions sp
  JOIN public.sales s ON s.recorder = sp.recorder AND s.recorder_type = sp.recorder_type
  WHERE s.period >= '2026-06-25'
  GROUP BY sp.recorder_type
)
SELECT d.recorder_type, d.docs, p.positions, p.sum_stoimost, p.sum_nds, p.sum_summa
FROM d FULL JOIN p USING (recorder_type) ORDER BY recorder_type

-- [test@142] 1.3: перепроверка docs — distinct recorder из sales_positions (независимый источник)
SELECT recorder_type, COUNT(DISTINCT recorder) AS docs_from_positions
FROM public.sales_positions GROUP BY recorder_type ORDER BY recorder_type

-- [MSSQL UPP_JAN@61] 2.1: AccumRg17844 по типам за окно движка [4026-06-15, 4026-07-22): строки, distinct docs, суммы, разрез _Active
SELECT CONVERT(int, [_RecorderTRef]) AS tref,
       COUNT(*)                       AS rows_total,
       COUNT(DISTINCT [_RecorderRRef]) AS docs,
       SUM([_Fld17855])               AS sum_stoimost,
       SUM([_Fld17857])               AS sum_nds,
       SUM(CASE WHEN [_Active] = 0x00 THEN 1 ELSE 0 END)            AS rows_inactive,
       SUM(CASE WHEN [_Active] = 0x00 THEN [_Fld17855] ELSE 0 END)  AS inactive_stoimost,
       SUM(CASE WHEN [_Active] = 0x00 THEN [_Fld17857] ELSE 0 END)  AS inactive_nds
FROM [UPP_JAN].[dbo].[_AccumRg17844]
WHERE [_Period] >= '4026-06-15' AND [_Period] < '4026-07-22'
GROUP BY CONVERT(int, [_RecorderTRef])
ORDER BY tref

-- [MSSQL UPP_JAN@61] 2.2: неактивные строки (_Active=0x00): distinct docs по типам + границы периода
SELECT CONVERT(int, [_RecorderTRef]) AS tref,
       COUNT(*) AS rows_inactive,
       COUNT(DISTINCT [_RecorderRRef]) AS docs_inactive,
       MIN([_Period]) AS min_period, MAX([_Period]) AS max_period
FROM [UPP_JAN].[dbo].[_AccumRg17844]
WHERE [_Period] >= '4026-06-15' AND [_Period] < '4026-07-22' AND [_Active] = 0x00
GROUP BY CONVERT(int, [_RecorderTRef])
ORDER BY tref

-- [MSSQL UPP_JAN@61] 2.3: то же окно, но _Period >= '4026-06-25' (сопоставление с DWH-срезом >= 2026-06-25)
SELECT CONVERT(int, [_RecorderTRef]) AS tref,
       COUNT(*) AS rows_total,
       COUNT(DISTINCT [_RecorderRRef]) AS docs,
       SUM([_Fld17855]) AS sum_stoimost,
       SUM([_Fld17857]) AS sum_nds
FROM [UPP_JAN].[dbo].[_AccumRg17844]
WHERE [_Period] >= '4026-06-25' AND [_Period] < '4026-07-22'
GROUP BY CONVERT(int, [_RecorderTRef])
ORDER BY tref

-- [MSSQL UPP_JAN@61] 3.1: doc-level агрегат MSSQL — по каждому (tref, recorder): min период, строк, суммы
SELECT CONVERT(int, [_RecorderTRef]) AS tref,
       [_RecorderRRef]               AS rref,
       MIN([_Period])                AS min_period,
       COUNT(*)                      AS rows_cnt,
       SUM([_Fld17855])              AS sum_stoimost,
       SUM([_Fld17857])              AS sum_nds
FROM [UPP_JAN].[dbo].[_AccumRg17844]
WHERE [_Period] >= '4026-06-15' AND [_Period] < '4026-07-22'
GROUP BY CONVERT(int, [_RecorderTRef]), [_RecorderRRef]

-- [test@142] 3.2: DWH — все доки sales (recorder, type, day)
SELECT recorder_type AS tref, recorder, period::date AS day FROM public.sales

-- [test@142] 4.1: перепроверка — сколько из missing-uuid реально есть в sales (параметр = полный список missing по типу)
SELECT COUNT(*) FROM public.sales WHERE recorder_type=%s AND recorder = ANY(%s::uuid[]) -- executed per type

-- [test@142] 4.2: пересчёт docs DWH на текущий момент (дрейф живой БД)
SELECT recorder_type, COUNT(DISTINCT recorder) AS docs, MAX(period) AS maxp FROM public.sales GROUP BY recorder_type ORDER BY 1

-- [MSSQL UPP_JAN@61] 4.3: _Active=0x00 по ВСЕЙ таблице _AccumRg17844 (вне окна тоже)
SELECT TOP 5 [_Period], CONVERT(int,[_RecorderTRef]) AS tref FROM [UPP_JAN].[dbo].[_AccumRg17844] WHERE [_Active]=0x00 ORDER BY [_Period] DESC

-- [MSSQL UPP_JAN@61] 4.4: все TRef в регистре с 4026-01-01 (шире окна) — есть ли другие типы
SELECT CONVERT(int,[_RecorderTRef]) AS tref, COUNT(*) AS rows_cnt, MIN([_Period]) AS minp, MAX([_Period]) AS maxp
FROM [UPP_JAN].[dbo].[_AccumRg17844] WHERE [_Period] >= '4026-01-01'
GROUP BY CONVERT(int,[_RecorderTRef]) ORDER BY tref

-- [ims_db@99] 5.1: retail-окно — distinct document_uid c doc_type/sale_type/status
SELECT lower(document_uid) AS uid,
       COUNT(*)            AS change_rows,
       MAX(doc_type)       AS doc_type,
       MAX(sale_type)      AS sale_type,
       MAX(document_status) AS status,
       MIN(updated_at)     AS first_upd,
       MAX(updated_at)     AS last_upd
FROM public.sales
WHERE updated_at >= '2026-06-25 07:56:46'
GROUP BY lower(document_uid)
