-- ============================================================================
-- АУДИТ SALES ETL — Секция «Incremental update и missing-логика (п. 7 ТЗ)»
-- Дата: 2026-07-21, Asia/Almaty (UTC+5)
-- Режим: READ-ONLY (только SELECT; psycopg2-сессии readonly=True; MSSQL — только SELECT)
-- Подключения:
--   test   = 10.10.1.142:5432/test   (витрина public.sales / public.sales_positions)
--   ims_db = 10.10.1.99:5432/ims_db  (retail-источник сигналов; updated_at = naive UTC)
--   MSSQL  = 10.10.1.61:1433/UPP_JAN (1С: dbo._AccumRg17844, _Document476/415/254; год +2000)
-- TZ-семантика: ims_db.updated_at = UTC; public.sales.retail_*/etl_updated_at = Almaty (UTC+5);
--               public.sales.etl_loaded_at = UTC у incremental-строк (побочный эффект
--               transform_utils.datetime.utcnow(), см. секцию integrity);
--               MSSQL _Period: год +2000 (4026-06-25 = 2026-06-25).
-- Окно инкремента (ТЗ): retail.updated_at >= '2026-06-25 07:56:46' UTC (= 12:56:46 Almaty).
-- ============================================================================

-- [ims_db@99] Контроль времени retail-сервера (UTC vs Almaty)
SELECT now() AT TIME ZONE 'UTC' AS now_utc, now() AT TIME ZONE 'Asia/Almaty' AS now_almaty

-- [test@142] Контроль времени DWH-сервера (UTC vs Almaty)
SELECT now() AT TIME ZONE 'UTC' AS now_utc, now() AT TIME ZONE 'Asia/Almaty' AS now_almaty

-- [ims_db@99] Основная выгрузка окна
-- 1.1 Все document_uid окна инкремента с числом DISTINCT updated_at (версий),
--     первой/последней версией и числом строк; updated_at здесь naive UTC
SELECT document_uid,
       COUNT(DISTINCT updated_at)              AS n_upd,
       MIN(updated_at)                         AS first_upd_utc,
       MAX(updated_at)                         AS last_upd_utc,
       COUNT(*)                                AS n_rows,
       (ARRAY_AGG(doc_type        ORDER BY updated_at DESC))[1] AS doc_type,
       (ARRAY_AGG(document_status ORDER BY updated_at DESC))[1] AS document_status
FROM public.sales
WHERE updated_at >= '2026-06-25 07:56:46'
GROUP BY document_uid

-- [ims_db@99] Перепроверка count multi-update
-- 1.2 ПЕРЕПРОВЕРКА: то же число независимым запросом с HAVING
SELECT COUNT(*) AS n_docs_multi
FROM (
  SELECT document_uid
  FROM public.sales
  WHERE updated_at >= '2026-06-25 07:56:46'
  GROUP BY document_uid
  HAVING COUNT(DISTINCT updated_at) > 1
) q

-- [ims_db@99] Span версий multi-update доков
-- 1.3 Для multi-update: распределение span'а версий (max-min) — отличать
--     реальные повторные апдейты от 'дребезга' записи позиций
SELECT width_bucket(EXTRACT(EPOCH FROM span), ARRAY[1,60,3600,86400,604800]) AS bucket,
       COUNT(*) AS docs
FROM (
  SELECT document_uid, MAX(updated_at)-MIN(updated_at) AS span
  FROM public.sales
  WHERE updated_at >= '2026-06-25 07:56:46'
  GROUP BY document_uid
  HAVING COUNT(DISTINCT updated_at) > 1
) q
GROUP BY 1 ORDER BY 1

-- [ims_db@99] Схема retail.sales — есть ли created_at и т.п.
-- 1b.1 Полная схема public.sales retail-источника
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='public' AND table_name='sales' ORDER BY ordinal_position

-- [ims_db@99] Строк на документ vs distinct updated_at в окне
-- 1b.2 Распределение: строк на документ / distinct updated_at на документ (окно)
SELECT n_rows_bucket, n_upd, COUNT(*) AS docs
FROM (
  SELECT document_uid,
         LEAST(COUNT(*), 5)            AS n_rows_bucket,
         COUNT(DISTINCT updated_at)    AS n_upd
  FROM public.sales
  WHERE updated_at >= '2026-06-25 07:56:46'
  GROUP BY document_uid
) q
GROUP BY 1,2 ORDER BY 1,2

-- [ims_db@99] Docs с >1 distinct updated_at по всей таблице
-- 1b.3 По всей таблице: сколько доков имеют >1 DISTINCT updated_at
--      (если 0 — updated_at перезаписывается на месте у всех строк дока разом)
SELECT COUNT(*) AS docs_multi_ts
FROM (
  SELECT document_uid
  FROM public.sales
  GROUP BY document_uid
  HAVING COUNT(DISTINCT updated_at) > 1
) q

-- [ims_db@99] updated_at > created_at — признак повторного апдейта
-- 1b.4 В окне: доки, у которых updated_at > created_at (была минимум одна повторная запись)
SELECT COUNT(DISTINCT document_uid) FILTER (WHERE upd > cre + interval '1 second') AS docs_reupdated,
       COUNT(DISTINCT document_uid)                                                AS docs_total
FROM (
  SELECT document_uid, MAX(updated_at) AS upd, MIN(created_at) AS cre
  FROM public.sales
  WHERE updated_at >= '2026-06-25 07:56:46'
  GROUP BY document_uid
) q

-- [ims_db@99] Есть ли в ims_db таблицы истории/лога
-- 1b.5 Поиск таблиц истории изменений в ims_db
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema')
ORDER BY 1,2

-- [ims_db@99] Структура event_logs
-- 1b.6 Структура event_logs — возможный лог апдейтов sales
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='public' AND table_name='event_logs' ORDER BY ordinal_position

-- [ims_db@99] Примеры event_logs по sales
-- 1b.7 event_logs: какие entity/event бывают (примерка на историю sales)
SELECT * FROM public.event_logs ORDER BY id DESC LIMIT 5

-- [ims_db@99] Окно инкремента с created_at (прокси multi-update)
-- 2.1 Окно инкремента: по 1 строке на документ; lag = updated_at - created_at
--     (updated_at/created_at naive UTC; lag > 1s => документ записывался повторно)
SELECT document_uid, doc_type, sale_type, document_status, date AS doc_date,
       created_at AS created_utc, updated_at AS last_upd_utc,
       updated_at - created_at AS upd_lag
FROM public.sales
WHERE updated_at >= '2026-06-25 07:56:46'

-- [test@142] DWH sales целиком (для merge по recorder)
-- 2.2 DWH: строки public.sales (recorder/type/audit-поля); retail_*/etl_updated_at = Almaty,
--     etl_loaded_at = UTC у incremental-строк (известный дефект TZ)
SELECT id, recorder::text AS recorder, recorder_type, period,
       retail_updated_at, retail_snapshot_at, etl_loaded_at, etl_updated_at
FROM public.sales

-- [test@142] now Almaty для допуска свежести
-- 2.3 Текущий момент в Almaty (для допуска 15 минут)
SELECT (now() AT TIME ZONE 'Asia/Almaty')::timestamp AS now_almaty

-- [test@142] load_history: full load и первые инкременты 2026-06-25
-- 3.1 load_history регистра sales за 2026-06-25 (started_at/finished_at в UTC!)
SELECT h.id, h.run_mode, h.status, h.started_at, h.finished_at,
       h.rows_extracted, h.rows_loaded, LEFT(h.checkpoint_value, 120) AS checkpoint
FROM etl_meta.load_history h JOIN etl_meta.registers r ON r.id = h.register_id
WHERE r.code='sales' AND h.started_at < '2026-06-26 00:00:00'
ORDER BY h.started_at
LIMIT 40

-- [test@142] ПЕРЕПРОВЕРКА 798 прямым запросом через dblink недоступен — считаем в DWH
-- 3.3 ПЕРЕПРОВЕРКА: столько же строк public.sales имеют retail_updated_at IS NULL
--     и period >= '2026-06-25' 13:00 — грубая рамка (точная сверка сделана merge'м в pandas)
SELECT COUNT(*) AS null_ru_total,
       COUNT(*) FILTER (WHERE retail_snapshot_at IS NOT NULL) AS with_snapshot
FROM public.sales WHERE retail_updated_at IS NULL

-- [test@142] DWH: counts позиций по recorder
-- 3.4 Число позиций на документ в DWH (для сверки с MSSQL _AccumRg17844)
SELECT recorder::text AS recorder, recorder_type, COUNT(*) AS n_pos,
       COUNT(DISTINCT line_no) AS n_lines
FROM public.sales_positions
GROUP BY recorder, recorder_type

-- [test@142] id-диапазоны: full load vs incremental
-- 3.5 Диапазоны id: строки, никогда не тронутые инкрементом (retail_updated_at IS NULL)
--     vs тронутые. Если upsert = UPDATE на месте, у тронутых id остаётся «старым».
SELECT (retail_updated_at IS NULL) AS never_touched,
       MIN(id) AS min_id, MAX(id) AS max_id, COUNT(*) AS rows
FROM public.sales GROUP BY 1

-- [MSSQL UPP_JAN@61] Агрегат движений по recorder за июнь-июль (год +2000!)
-- 4.1 _AccumRg17844: строки движений на каждый recorder, _Period >= '4026-06-01'
--     (= 2026-06-01; в конфиге источника where_clause НЕТ — движок грузит все строки
--      документа без фильтра по _Active и без фильтра периода в incremental)
SELECT _RecorderRRef, _RecorderTRef,
       COUNT(*)                                        AS n_ms,
       SUM(CASE WHEN _Active = 0x01 THEN 1 ELSE 0 END) AS n_active,
       MIN(_Period) AS min_period, MAX(_Period) AS max_period
FROM dbo._AccumRg17844
WHERE _Period >= '4026-06-01'
GROUP BY _RecorderRRef, _RecorderTRef

-- [test@142] Конфиг источников регистра sales (where_clause?)
-- 5.1 Источники регистра sales: подтверждаем отсутствие/наличие where_clause
SELECT s.id, s.source_code, s.mssql_table, s.where_clause, s.is_active
FROM etl_meta.sources s JOIN etl_meta.registers r ON r.id = s.register_id
WHERE r.code = 'sales' ORDER BY s.id

-- [test@142] Список таблиц etl_meta (уточнение имён конфиг-таблиц)
-- 5.0 Какие таблицы есть в etl_meta
SELECT table_name FROM information_schema.tables WHERE table_schema='etl_meta' ORDER BY 1

-- [test@142] Схема etl_meta.register_sources
-- 5.0b Колонки etl_meta.register_sources
SELECT column_name FROM information_schema.columns WHERE table_schema='etl_meta' AND table_name='register_sources' ORDER BY ordinal_position

-- [test@142] Схема etl_meta.register_targets
-- 5.0b Колонки etl_meta.register_targets
SELECT column_name FROM information_schema.columns WHERE table_schema='etl_meta' AND table_name='register_targets' ORDER BY ordinal_position

-- [test@142] Конфиг источников регистра sales (where_clause?)
-- 5.1 Источники регистра sales: подтверждаем отсутствие/наличие where_clause
SELECT s.id, s.source_code, s.mssql_table, s.where_clause, s.is_active
FROM etl_meta.register_sources s JOIN etl_meta.registers r ON r.id = s.register_id
WHERE r.code = 'sales' ORDER BY s.id

-- [test@142] Конфиг target-ов регистра sales (load_mode/upsert_keys)
-- 5.2 Target-ы регистра sales: режимы загрузки и ключи upsert
SELECT t.id, t.target_table, t.target_role, t.load_mode, t.upsert_keys, t.priority, t.is_active
FROM etl_meta.register_targets t JOIN etl_meta.registers r ON r.id = t.register_id
WHERE r.code = 'sales' ORDER BY t.priority

-- [test@142] load_history summary: full_period после 2026-06-25
-- 5.3 Сводка прогонов sales c 2026-06-25: сколько инкрементов/фуллов, фейлы,
--     последние full_period (зомби 07-03 мог бы вычиститься полным перегоном)
SELECT run_mode, status, COUNT(*) AS n, MIN(started_at) AS first_run, MAX(started_at) AS last_run
FROM etl_meta.load_history h JOIN etl_meta.registers r ON r.id = h.register_id
WHERE r.code='sales' AND h.started_at >= '2026-06-25'
GROUP BY run_mode, status ORDER BY run_mode, status

-- [test@142] Последние full_period прогоны
-- 5.3b Последние full_period: покрывали ли период зомби (2026-07-03)
SELECT h.id, h.status, h.started_at, h.rows_loaded, LEFT(h.checkpoint_value,120) AS checkpoint
FROM etl_meta.load_history h JOIN etl_meta.registers r ON r.id = h.register_id
WHERE r.code='sales' AND h.run_mode='full_period'
ORDER BY h.started_at DESC LIMIT 10

-- [test@142] ПЕРЕПРОВЕРКА точки 4 прямым SQL
-- 5.5 Точка 4: строки, вставленные full load 2026-06-25 17:59:24 (Almaty) и позже
--     тронутые инкрементом (retail_updated_at IS NOT NULL): у ВСЕХ ли
--     etl_updated_at > etl_loaded_at (upsert реально обновлял строку)
SELECT COUNT(*)                                             AS updated_by_incr,
       COUNT(*) FILTER (WHERE etl_updated_at >  etl_loaded_at) AS upd_gt_loaded,
       COUNT(*) FILTER (WHERE etl_updated_at <= etl_loaded_at) AS upd_le_loaded
FROM public.sales
WHERE retail_updated_at IS NOT NULL
  AND etl_loaded_at = '2026-06-25 17:59:24.454701'

-- [test@142] ПЕРЕПРОВЕРКА позиций 9 mismatch-доков
-- 5.6 ПЕРЕПРОВЕРКА (2-й независимый запрос): позиции DWH по 9 подозреваемым recorder
SELECT s.recorder, s.recorder_type, s.period, s.retail_updated_at, s.etl_updated_at,
       COUNT(p.recorder) AS n_pos, COUNT(DISTINCT p.line_no) AS n_lines
FROM public.sales s
LEFT JOIN public.sales_positions p ON p.recorder = s.recorder
WHERE s.recorder IN ('277169c0-6bc0-11f1-81c0-04d4c4d2bb6f'::uuid, 'd622cb25-7458-11f1-8274-a8a1594c5fd8'::uuid, '18898783-7458-11f1-866c-7085c2428d7f'::uuid, 'd776e52f-7457-11f1-8e28-00e04c684b14'::uuid, 'c3d7e5e2-7457-11f1-866c-7085c2428d7f'::uuid, '40f435b1-7458-11f1-81c3-04d4c4d2bb6f'::uuid, '50b5b93f-7458-11f1-8db9-74563c11b8bb'::uuid, '7d2c5953-7458-11f1-8274-a8a1594c5fd8'::uuid, 'fbba0a68-6595-11f1-81bf-04d4c4d2bb6f'::uuid)
GROUP BY s.recorder, s.recorder_type, s.period, s.retail_updated_at, s.etl_updated_at
ORDER BY s.recorder

-- [test@142] Зомби: строка в DWH sales + позиции
-- 5.7 Зомби-кандидат: его строка в public.sales и позиции
SELECT s.recorder, s.recorder_type, s.period, s.retail_updated_at, s.retail_snapshot_at,
       s.etl_loaded_at, s.etl_updated_at,
       (SELECT COUNT(*) FROM public.sales_positions p WHERE p.recorder = s.recorder) AS n_pos
FROM public.sales s WHERE s.recorder = 'b9127f17-76bd-11f1-bc87-2c4d545a0775'::uuid

-- [ims_db@99] Retail: текущее состояние 10 кандидатов
-- 5.8 Retail сейчас: сигналил ли retail по этим uid ПОСЛЕ последнего касания ETL
SELECT document_uid, doc_type, document_status, date AS doc_date,
       created_at AS created_utc, updated_at AS updated_utc
FROM public.sales WHERE lower(document_uid) IN ('277169c0-6bc0-11f1-81c0-04d4c4d2bb6f', 'd622cb25-7458-11f1-8274-a8a1594c5fd8', '18898783-7458-11f1-866c-7085c2428d7f', 'd776e52f-7457-11f1-8e28-00e04c684b14', 'c3d7e5e2-7457-11f1-866c-7085c2428d7f', '40f435b1-7458-11f1-81c3-04d4c4d2bb6f', '50b5b93f-7458-11f1-8db9-74563c11b8bb', '7d2c5953-7458-11f1-8274-a8a1594c5fd8', 'fbba0a68-6595-11f1-81bf-04d4c4d2bb6f', 'b9127f17-76bd-11f1-bc87-2c4d545a0775')
