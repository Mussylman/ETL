-- ============================================================================
-- АУДИТ SALES ETL — Секция «Свежесть данных (п. 2 ТЗ)»
-- Дата: 2026-07-21, ~12:05–12:20 Asia/Almaty (07:05–07:20 UTC)
-- Режим: READ-ONLY (только SELECT; python-сессии открыты с readonly=True)
-- Подключения:
--   test      = 10.10.1.142:5432/test      (наша витрина public.sales/_positions + etl_meta)
--   ims_db    = 10.10.1.99:5432/ims_db     (настоящий retail-источник, Airflow conn "bd_retail")
--   bd_retail = 10.10.1.142:5432/bd_retail (УСТАРЕВШАЯ копия — см. находку F-1)
-- TZ-семантика: ims_db.updated_at = naive UTC; public.sales.retail_* и etl_* = naive Almaty;
--               etl_meta.load_history.started_at/finished_at = naive UTC (server TZ Etc/UTC),
--               НО содержимое checkpoint_value — в Almaty.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 0. Подготовка / выяснение схемы
-- ---------------------------------------------------------------------------

-- [test] Время и TZ сервера Postgres
SELECT now() AS server_now, current_setting('TimeZone') AS tz;
-- => 2026-07-21 07:06:11.75+00 | Etc/UTC

-- [bd_retail@142] Тип колонок retail-таблицы (проверка TZ-семантики)
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='public' AND table_name='sales'
  AND column_name IN ('updated_at','document_uid','retail_uuid','uid','id')
ORDER BY column_name;
-- => updated_at: timestamp WITHOUT time zone (хранится UTC), document_uid varchar, retail_uuid uuid

-- [test] Список таблиц etl_meta
SELECT table_name FROM information_schema.tables WHERE table_schema='etl_meta' ORDER BY table_name;

-- [test] Все колонки public.sales (какие watermark-поля есть)
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='public' AND table_name='sales' ORDER BY ordinal_position;
-- => recorder uuid — ключ документа; retail_snapshot_at / retail_updated_at / etl_updated_at — naive ts

-- [test] Конфиг регистров: какой uid-column у retail
SELECT id, code, name, retail_table, retail_uid_column FROM etl_meta.registers ORDER BY id;
-- => id=62 code=sales: retail_table=sales, retail_uid_column=document_uid

-- [ims_db] Полная структура retail.sales (типы документов для классификации)
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='public' AND table_name='sales' ORDER BY ordinal_position;
-- => есть doc_type, sale_type, document_status, document (название дока)

-- ---------------------------------------------------------------------------
-- 1. ЧЕТЫРЕ ТОЧКИ (п. 1 секции)
-- ---------------------------------------------------------------------------

-- [bd_retail@142] Точка 1 (ЛОЖНЫЙ след — устаревшая копия!): MAX(updated_at)
SELECT MAX(updated_at)                                                    AS max_updated_utc,
       MAX((updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp) AS max_updated_almaty,
       (now() AT TIME ZONE 'Asia/Almaty')::timestamp(3)                   AS now_almaty,
       count(*)                                                           AS total_rows
FROM public.sales;
-- => 2025-11-11 06:12:44 UTC | 2025-11-11 11:12:44 Almaty | 885 285 строк — копия «стоит» 8.5 мес.
--    Airflow conn "bd_retail" на самом деле указывает на 10.10.1.99/ims_db (см. ниже).

-- [ims_db] Точка 1 (настоящий источник): MAX(updated_at) UTC + Almaty + now
SELECT MAX(updated_at)                                                    AS max_updated_utc,
       MAX((updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp) AS max_updated_almaty,
       (now() AT TIME ZONE 'Asia/Almaty')::timestamp(3)                   AS now_almaty,
       now() AT TIME ZONE 'UTC'                                           AS now_utc,
       count(*)                                                           AS total_rows
FROM public.sales;
-- => 2026-07-21 07:09:58 UTC = 12:09:58 Almaty | now=12:11:16.6 | 1 326 194 строк
--    Повтор в 12:13:13: max=12:12:54 Almaty — источник пишет непрерывно.

-- [ims_db] Перепроверка MAX вторым независимым запросом (ORDER BY DESC LIMIT 1)
SELECT (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp
FROM public.sales ORDER BY updated_at DESC LIMIT 1;
-- => 12:12:54 → в момент повтора 12:13:13 (новые строки успели прийти — источник real-time)

-- [test] Точки 2–4: watermark-поля витрины public.sales
SELECT MAX(retail_updated_at)  AS max_retail_updated_at,
       MAX(retail_snapshot_at) AS max_retail_snapshot_at,
       MAX(etl_updated_at)     AS max_etl_updated_at,
       (now() AT TIME ZONE 'Asia/Almaty')::timestamp(3) AS now_almaty,
       count(*)                AS total_rows
FROM public.sales;
-- => retail_updated_at=2026-07-21 12:02:52 | retail_snapshot_at=12:02:52
--    etl_updated_at=12:10:06.65 | now=12:10:23.8 | 64 435 строк

-- [test] Перепроверка watermark вторым независимым запросом
SELECT retail_updated_at FROM public.sales
WHERE retail_updated_at IS NOT NULL ORDER BY retail_updated_at DESC LIMIT 1;
-- => 2026-07-21 12:02:52 — совпало

-- [test] То же для public.sales_positions («обе таблицы»)
SELECT MAX(retail_updated_at)  AS max_retail_updated_at,
       MAX(retail_snapshot_at) AS max_retail_snapshot_at,
       MAX(etl_updated_at)     AS max_etl_updated_at,
       count(*)                AS total_rows
FROM public.sales_positions;
-- => 12:02:52 | 12:02:52 | etl_updated_at=12:10:09.11 | 87 670 строк

-- [test] Точка 5: последний success-checkpoint инкремента из load_history
SELECT id, register_id, run_mode, status, started_at, finished_at,
       rows_extracted, rows_loaded, checkpoint_value
FROM etl_meta.load_history
WHERE status='success' AND run_mode='incremental'
ORDER BY finished_at DESC
LIMIT 3;
-- => id=7489, started 07:10:03 UTC (=12:10:03 Almaty), extracted=78 loaded=66,
--    checkpoint: watermark_from=2026-07-21 11:35:09; to=2026-07-21 12:09:58.601873;
--                overlap=0:05:00; max_retail_updated_at=2026-07-21 12:09:58; changes=78
--    NB: watermark_from одинаков у 3 подряд прогонов (11:35:09) — watermark стоит,
--    пока не загрузится реальная sales-строка; окно растёт (changes 41→59→78).

-- ---------------------------------------------------------------------------
-- 2. ЛАГИ (п. 2 секции) — каждый лаг считался с now() своего замера
-- ---------------------------------------------------------------------------
-- source lag   = now(Almaty) − MAX(ims_db.updated_at→Almaty) = 12:13:13.4 − 12:12:54  = ~19 c
-- data ETL lag = MAX(ims_db→Almaty) − MAX(sales.retail_updated_at) = 12:12:54 − 12:02:52 = 10 мин 02 c
-- pipeline lag = now(Almaty) − MAX(sales.etl_updated_at) = 12:13:13.3 − 12:10:06.6 = 3 мин 07 c
-- (запросы — те же, что в п.1, выполнены встык одной python-сессией readonly)

-- ---------------------------------------------------------------------------
-- 3. Строки retail НОВЕЕ watermark + split по наличию в public.sales (п. 3)
-- ---------------------------------------------------------------------------

-- [ims_db] Все строки новее watermark (:wm = 2026-07-21 12:02:52)
SELECT document_uid,
       (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp AS upd_alm
FROM public.sales
WHERE (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp > :wm
ORDER BY updated_at;
-- => 36 строк (на 12:13), все валидные uuid, дубликатов uid нет

-- [test] Split (а): сколько из них ЕСТЬ в public.sales (:uids = массив 36 uid'ов)
SELECT recorder::text, retail_updated_at, etl_updated_at
FROM public.sales WHERE recorder = ANY(:uids::uuid[]);
-- => 0 строк — настоящий backlog по продажам ПУСТ

-- [test] Перепроверка (б) обратной логикой NOT EXISTS
SELECT count(*) FROM unnest(:uids::uuid[]) u
WHERE NOT EXISTS (SELECT 1 FROM public.sales s WHERE s.recorder = u);
-- => 36 = 36 — сходится ((а)=0, (б)=36)

-- [ims_db] Классификация категории (б) по типам документов
-- (повтор на 12:15, окно чуть уехало: 40 доков новее watermark)
SELECT lower(document_uid), doc_type, sale_type, document_status, document,
       (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp AS upd
FROM public.sales
WHERE (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp >= :wm - interval '60 minutes';
-- => (б): doc_type/sale_type/status = (8,1,1) ×37 и (6,1,1) ×3 — «Чек ККМ» и «Продажа»,
--    ТЕ ЖЕ типы, что у загруженных. Отсутствие в sales = в MSSQL-регистре не было строк
--    на момент прогона (missing→delete/не проведён) ЛИБО док изменён после to_ts
--    последнего прогона (12:09:58) и ещё вообще не сканировался (4 дока 12:11–12:13).

-- [test+ims_db] SANITY join-ключа (иначе «(а)=0» был бы артефактом несовпадения ключей):
-- доки retail за час ДО watermark должны находиться в sales по recorder
SELECT count(*) FROM unnest(:uids_hour_before::uuid[]) u
WHERE EXISTS (SELECT 1 FROM public.sales s WHERE s.recorder = u);
-- => 92 из 143 присутствуют (ключ работает); 51 отсутствует — те же типы доков,
--    объяснимо missing-обработкой (удалены из регистра 1С) — зона секции полноты.

-- [test→ims_db] Обратная сверка значений: 10 последних загруженных строк sales,
-- их retail_updated_at против ims_db.updated_at(Almaty)
SELECT recorder::text, retail_updated_at FROM public.sales
WHERE retail_updated_at IS NOT NULL ORDER BY retail_updated_at DESC LIMIT 10;
SELECT lower(document_uid),
       (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp
FROM public.sales WHERE lower(document_uid) = ANY(:recorders);
-- => совпало 10/10 секунда-в-секунду — TZ-конверсия +5ч корректна

-- ---------------------------------------------------------------------------
-- 4. Текущее необработанное окно [wm−5мин, now−5с) (п. 4)
-- ---------------------------------------------------------------------------

-- [ims_db] Содержимое окна следующего прогона
SELECT count(*), count(DISTINCT document_uid),
       min((updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp),
       max((updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp),
       (now() AT TIME ZONE 'Asia/Almaty')::timestamp
FROM public.sales
WHERE (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp >= :wm - interval '5 minutes'
  AND (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp <
      (now() AT TIME ZONE 'Asia/Almaty')::timestamp - interval '5 seconds';
-- => окно [11:57:52, 12:13:11): 43 строки / 43 документа, min=11:58:52, max=12:12:54.
--    Из них [11:57:52, 12:09:58) уже сканировалось прогоном 7489 (overlap/пересечение),
--    реально новых (updated_at > to_ts=12:09:58): ~4–5 доков.

-- ---------------------------------------------------------------------------
-- 5. Стабильность за 24 часа (п. 5)
-- ---------------------------------------------------------------------------

-- [test] Интервалы между инкремент-прогонами за 24ч
WITH runs AS (
  SELECT started_at,
         started_at - lag(started_at) OVER (ORDER BY started_at) AS gap
  FROM etl_meta.load_history
  WHERE run_mode='incremental' AND started_at > now() - interval '24 hours'
)
SELECT count(*) AS runs,
       min(gap) AS min_gap, max(gap) AS max_gap,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM gap)) AS median_gap_s,
       count(*) FILTER (WHERE gap > interval '7 minutes') AS gaps_over_7min
FROM runs;
-- => 292 прогона | min 2:08 | max 5:02 | медиана 300.0 c | пропусков >7 мин: 0

-- [test] Failed-прогоны за 24ч
SELECT started_at, substring(error_message for 200) AS err
FROM etl_meta.load_history
WHERE run_mode='incremental' AND status='failed' AND started_at > now() - interval '24 hours'
ORDER BY started_at;
-- => 4 шт (Jul-20 10:20, 12:00, 14:50; Jul-21 05:20 UTC) — ошибки MSSQL-extract;
--    каждый следующий прогон успешен, watermark на fail не двигается → данные не теряются.

-- [test] Длина окна (to − watermark_from) из checkpoint_value за 24ч
WITH parsed AS (
  SELECT started_at,
         (substring(checkpoint_value from 'watermark_from=([0-9:. -]+);'))::timestamp AS wfrom,
         (substring(checkpoint_value from '; to=([0-9:. -]+);'))::timestamp          AS wto,
         (substring(checkpoint_value from 'changes=([0-9]+)'))::int                  AS changes
  FROM etl_meta.load_history
  WHERE run_mode='incremental' AND status='success'
    AND started_at > now() - interval '24 hours'
    AND checkpoint_value LIKE 'watermark_from=%'
)
SELECT count(*) AS runs, max(wto - wfrom) AS max_window,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM (wto-wfrom)))/60 AS median_window_min,
       max(changes) AS max_changes, sum(changes) AS sum_changes
FROM parsed;
-- => 288 прогонов | max окно 4:35:30 | медиана 25.5 мин | max changes 179 | сумма 10 506

-- [test] Длина окна по часам (Almaty) — эффект ночного стазиса watermark
WITH parsed AS (
  SELECT started_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty' AS started_almaty,
         (substring(checkpoint_value from 'watermark_from=([0-9:. -]+);'))::timestamp AS wfrom,
         (substring(checkpoint_value from '; to=([0-9:. -]+);'))::timestamp          AS wto,
         (substring(checkpoint_value from 'changes=([0-9]+)'))::int                  AS changes
  FROM etl_meta.load_history
  WHERE run_mode='incremental' AND status='success'
    AND started_at > now() - interval '24 hours'
    AND checkpoint_value LIKE 'watermark_from=%'
)
SELECT date_trunc('hour', started_almaty) AS hour_almaty, count(*) AS runs,
       round(max(extract(epoch FROM wto-wfrom))/60) AS max_window_min,
       round(avg(extract(epoch FROM wto-wfrom))/60) AS avg_window_min,
       max(changes) AS max_changes
FROM parsed GROUP BY 1 ORDER BY 1;
-- => днём avg 21–28 мин; ночью (23:00–07:00 Almaty) окно растёт до 275 мин,
--    т.к. watermark стоит на последней загруженной продаже; в 08:00 схлопывается до 49 мин.

-- ============================================================================
-- ПОВТОРНЫЙ ЗАМЕР (контрольный, перед фиксацией отчёта): 12:39–12:40 Almaty
-- Все запросы ниже выполнены заново той же readonly-сессией (см. freshness_check.py,
-- checkpoint_recheck.py в scratchpad). Живая БД — числа сдвинулись относительно
-- первого замера 12:10–12:15, оба снапшота согласованы между собой.
-- ============================================================================

-- [test] Точки 2–4 (повтор): watermark/snapshot/etl_updated + now
SELECT MAX(retail_updated_at), MAX(retail_snapshot_at), MAX(etl_updated_at),
       (now() AT TIME ZONE 'Asia/Almaty')::timestamp
FROM public.sales;
-- => 12:23:01 | 12:23:01 | 12:35:06.834 | now=12:39:26.78; positions: MAX(etl_updated_at)=12:35:09.32
--    re-check ORDER BY DESC LIMIT 1 => 12:23:01 (совпало)

-- [ims_db] Точка 1 (повтор): источник + now
-- => MAX(updated_at)=07:38:54 UTC = 12:38:54 Almaty | now=12:39:26.87 | re-check ORDER BY совпал
-- ЛАГИ (повтор): source lag = 32.9 c | data ETL lag = 12:38:54−12:23:01 = 15 мин 53 c
--                pipeline lag = 12:39:26.78−12:35:06.83 = 4 мин 20 c

-- [ims_db] П.3 (повтор): строк новее watermark 12:23:01 => 57 (все uuid валидны, дублей нет)
-- [test]  split: (а) recorder ЕСТЬ в public.sales = 0; (б) НЕТ = 57; NOT EXISTS re-check = 57 (сошлось)

-- [ims_db] НЕЗАВИСИМАЯ перепроверка №2 счёта «новее watermark»: сравнение в UTC без конверсии
SELECT count(*) FROM public.sales WHERE updated_at > :wm_utc;  -- :wm_utc = 12:23:01 − 5ч = 07:23:01
-- => 59 (замер на ~70 c позже Almaty-счёта 57; +2 строки — дрейф живой БД, методики согласованы)

-- [test] Свежий последний success-checkpoint (после прогона 12:40)
SELECT id, register_id, started_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty',
       finished_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty',
       rows_extracted, rows_loaded, checkpoint_value
FROM etl_meta.load_history
WHERE status='success' AND run_mode='incremental'
ORDER BY finished_at DESC LIMIT 3;
-- => id=7496 (12:40:03–12:40:09 Almaty) extracted=30 loaded=30
--    checkpoint: watermark_from=12:18:01; to=2026-07-21 12:39:58.665274; overlap=0:05:00;
--                max_retail_updated_at=2026-07-21 12:39:58; changes=83
--    id=7495 (12:35) loaded=30, to=12:34:58; id=7494 (12:33) loaded=102, to=12:33:19
--    NB: между 7494 и 7495 был failed-прогон 12:30:01 Almaty (watermark не тронут).
-- ВАЖНО: checkpoint-поле max_retail_updated_at (12:39:58) = max по ОТСКАНИРОВАННЫМ retail-строкам
--    (≈ to), а НЕ watermark. Реальный watermark = MAX(sales.retail_updated_at) = 12:23:01
--    даже ПОСЛЕ прогона 7496 (loaded=30): все 30 загруженных — из overlap-зоны ≤ 12:23:01.
--    watermark_from следующего прогона = watermark витрины − 5 мин (12:18:01 = 12:23:01−5:00) — сходится.

-- [ims_db] Split «новее watermark» по to последнего прогона (сканировались ли уже)
SELECT count(*) FILTER (WHERE alm(updated_at) <= :to_ts),   -- уже сканировались, НЕ загружены
       count(*) FILTER (WHERE alm(updated_at) >  :to_ts)    -- ещё не сканировались
FROM public.sales WHERE alm(updated_at) > :wm;              -- alm(x) = x AT TZ 'UTC' AT TZ 'Asia/Almaty'
-- => 57 сканировались и не загружены (в MSSQL-регистре ещё/вообще нет строк) | 2 новые

-- [ims_db] Типы документов «новее watermark» (повтор классификации)
SELECT doc_type, sale_type, document_status, count(*)
FROM public.sales
WHERE (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp > :wm
GROUP BY 1,2,3 ORDER BY 4 DESC;
-- => (8,1,1)=55 «Чек ККМ», (6,1,1)=3 «Продажа», (10,2,1)=1 — те же рабочие типы, что грузятся

-- [ims_db] П.4 (повтор): окно [wm−5мин, now−5с) = [12:18:01, 12:39:24)
-- => 81 строка / 81 документ, min=12:18:20, max=12:38:54
-- Перепроверка №2 в UTC-границах (на 12:40:22): 84 строки; checkpoint прогона 7496: changes=83 —
-- три независимых замера окна согласованы (дрейф — новые продажи за секунды между замерами).

-- [test] Последние 6 стартов инкремента (Almaty) — фактический интервал расписания
SELECT started_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty', status
FROM etl_meta.load_history WHERE run_mode='incremental'
ORDER BY started_at DESC LIMIT 6;
-- => 12:40 s | 12:35 s | 12:33 s (retry) | 12:30 failed | 12:25 s | 12:20 s — сетка 5 мин держится,
--    fail компенсирован ретраем через ~3 мин.
