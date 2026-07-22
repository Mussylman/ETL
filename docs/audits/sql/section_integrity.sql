-- =====================================================================
-- Секция аудита: Целостность DWH (п. 5 ТЗ)
-- БД: PostgreSQL 10.10.1.142:5432/test, только SELECT (read-only сессия)
-- Дата: 2026-07-21
-- Все таймстампы в sales/sales_positions — naive, время Almaty (UTC+5)
-- =====================================================================

-- 0.1 Базовые объёмы: всего строк в sales и sales_positions
SELECT 'sales' AS tbl, count(*) AS cnt FROM public.sales UNION ALL SELECT 'sales_positions', count(*) FROM public.sales_positions;

-- 0.2 Распределение recorder_type в обеих таблицах + TZ сервера
SELECT 'sales' AS tbl, recorder_type, count(*) FROM public.sales GROUP BY 1,2 UNION ALL SELECT 'sales_positions', recorder_type, count(*) FROM public.sales_positions GROUP BY 1,2 ORDER BY 1,2;

-- 0.3 Часовой пояс сервера PostgreSQL (для интерпретации now())
SHOW timezone;

-- 1.1 Проверка ТЗ-1: sales_positions.sales_id IS NULL (должно быть 0)
SELECT count(*) AS null_sales_id FROM public.sales_positions WHERE sales_id IS NULL;

-- 2.1 Проверка ТЗ-2: recorder_type IS NULL в sales и sales_positions, line_no IS NULL в positions
SELECT
  (SELECT count(*) FROM public.sales WHERE recorder_type IS NULL) AS sales_rt_null,
  (SELECT count(*) FROM public.sales_positions WHERE recorder_type IS NULL) AS pos_rt_null,
  (SELECT count(*) FROM public.sales_positions WHERE line_no IS NULL) AS pos_line_no_null;

-- 2.2 Дополнительно: recorder IS NULL в обеих таблицах (recorder nullable по DDL, NULL обходит unique)
SELECT
  (SELECT count(*) FROM public.sales WHERE recorder IS NULL) AS sales_recorder_null,
  (SELECT count(*) FROM public.sales_positions WHERE recorder IS NULL) AS pos_recorder_null;

-- 3.1 Проверка ТЗ-3: дубли sales по (recorder, recorder_type)
SELECT count(*) AS dup_groups, coalesce(sum(cnt-1),0) AS extra_rows FROM (SELECT recorder, recorder_type, count(*) AS cnt FROM public.sales GROUP BY 1,2 HAVING count(*)>1) d;

-- 3.2 Проверка ТЗ-3: дубли sales_positions по (recorder, recorder_type, line_no)
SELECT count(*) AS dup_groups, coalesce(sum(cnt-1),0) AS extra_rows FROM (SELECT recorder, recorder_type, line_no, count(*) AS cnt FROM public.sales_positions GROUP BY 1,2,3 HAVING count(*)>1) d;

-- 3.3 Дополнительно: дубли sales только по recorder (unique(recorder) есть, но проверяем)
SELECT count(*) AS dup_recorder_groups FROM (SELECT recorder FROM public.sales GROUP BY 1 HAVING count(*)>1) d;

-- 4.1 Проверка ТЗ-4: orphan positions по sales_id (LEFT JOIN вместо NOT IN — устойчиво к NULL)
SELECT count(*) AS orphan_by_sales_id FROM public.sales_positions p LEFT JOIN public.sales s ON s.id = p.sales_id WHERE s.id IS NULL;

-- 4.2 Проверка ТЗ-4: positions, у которых (recorder, recorder_type) нет в sales
SELECT count(*) AS orphan_by_recorder FROM public.sales_positions p LEFT JOIN public.sales s ON s.recorder = p.recorder AND s.recorder_type = p.recorder_type WHERE s.id IS NULL;

-- 4.3 Дополнительно: согласованность связки — sales_id указывает на sales с ДРУГИМ (recorder, recorder_type)
SELECT count(*) AS mismatched_link FROM public.sales_positions p JOIN public.sales s ON s.id = p.sales_id WHERE s.recorder IS DISTINCT FROM p.recorder OR s.recorder_type IS DISTINCT FROM p.recorder_type;

-- 5.1 Проверка ТЗ-5: sales без единой позиции — по recorder_type
SELECT s.recorder_type, count(*) AS sales_without_positions FROM public.sales s LEFT JOIN public.sales_positions p ON p.sales_id = s.id WHERE p.id IS NULL GROUP BY 1 ORDER BY 1;

-- 5.2 Перепроверка ТЗ-5 вторым способом (NOT EXISTS)
SELECT count(*) AS total_childless FROM public.sales s WHERE NOT EXISTS (SELECT 1 FROM public.sales_positions p WHERE p.sales_id = s.id);

-- 5.3 Примеры sales без позиций: детали для диагностики
SELECT s.id, s.recorder, s.recorder_type, s.period, s.doc_number, s.is_posted, s.etl_loaded_at, s.etl_updated_at, s.retail_updated_at, s.retail_snapshot_at FROM public.sales s WHERE NOT EXISTS (SELECT 1 FROM public.sales_positions p WHERE p.sales_id = s.id) ORDER BY s.period;

-- 5.4 TZ-гипотеза: etl_loaded_at (DEFAULT now(), сервер UTC) хранится в UTC, а etl_updated_at — в Almaty. Для строк, вставленных и обновлённых одним прогоном, дельта должна быть ~5ч
SELECT min(etl_updated_at - etl_loaded_at) AS min_delta, max(etl_updated_at - etl_loaded_at) AS max_delta, percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM etl_updated_at - etl_loaded_at)) AS median_sec FROM public.sales WHERE etl_updated_at IS NOT NULL;

-- 6.1 Проверка ТЗ-6: NULL в обязательных полях sales (doc_number с разбивкой по recorder_type)
SELECT
  (SELECT count(*) FROM public.sales WHERE period IS NULL) AS period_null,
  (SELECT count(*) FROM public.sales WHERE recorder IS NULL) AS recorder_null;

-- 6.2 Проверка ТЗ-6: doc_number IS NULL по recorder_type (у 352 — норма, у 476/415/254 — аномалия)
SELECT recorder_type, count(*) AS total, count(*) FILTER (WHERE doc_number IS NULL) AS doc_number_null, count(*) FILTER (WHERE doc_number = '') AS doc_number_empty FROM public.sales GROUP BY 1 ORDER BY 1;

-- 6.3 Распределение дельты etl_updated_at - etl_loaded_at (0 = обе Almaty, 5ч = loaded_at в UTC)
SELECT CASE WHEN etl_updated_at - etl_loaded_at < interval '1 hour' THEN '~0 (Almaty)' WHEN etl_updated_at - etl_loaded_at BETWEEN interval '4 hour' AND interval '6 hour' THEN '~5h (UTC)' ELSE 'other' END AS delta_class, count(*), min(etl_loaded_at) AS min_loaded, max(etl_loaded_at) AS max_loaded FROM public.sales WHERE etl_updated_at IS NOT NULL GROUP BY 1;

-- 6.4 Примеры аномальных doc_number IS NULL (типы 476/415): даты и аудит-поля
SELECT id, recorder, recorder_type, period, is_posted, etl_loaded_at, etl_updated_at, retail_updated_at FROM public.sales WHERE doc_number IS NULL AND recorder_type <> 352 ORDER BY period LIMIT 25;

-- 6.5 Связь аномалий: у doc_number IS NULL строк — есть ли позиции, и совпадает ли с is_posted IS NULL
SELECT count(*) AS doc_num_null_rows, count(*) FILTER (WHERE is_posted IS NULL) AS also_is_posted_null, count(*) FILTER (WHERE EXISTS (SELECT 1 FROM public.sales_positions p WHERE p.sales_id = s.id)) AS have_positions FROM public.sales s WHERE doc_number IS NULL AND recorder_type <> 352;

-- 6.6 is_posted IS NULL всего по типам (шире, чем doc_number)
SELECT recorder_type, count(*) FILTER (WHERE is_posted IS NULL) AS is_posted_null FROM public.sales GROUP BY 1 ORDER BY 1;

-- 6.7 Проверка ТЗ-6: NULL в обязательных полях sales_positions
SELECT
  count(*) FILTER (WHERE nomenklatura IS NULL) AS nomenklatura_null,
  count(*) FILTER (WHERE stoimost IS NULL) AS stoimost_null,
  count(*) FILTER (WHERE kolichestvo IS NULL) AS kolichestvo_null
FROM public.sales_positions;

-- 6.8 Проверка ТЗ-6: NaN в numeric-полях sales_positions
SELECT
  count(*) FILTER (WHERE stoimost = 'NaN'::numeric) AS stoimost_nan,
  count(*) FILTER (WHERE summa = 'NaN'::numeric) AS summa_nan,
  count(*) FILTER (WHERE nds = 'NaN'::numeric) AS nds_nan,
  count(*) FILTER (WHERE tsena = 'NaN'::numeric) AS tsena_nan,
  count(*) FILTER (WHERE kolichestvo = 'NaN'::numeric) AS kolichestvo_nan,
  count(*) FILTER (WHERE nomerstroki = 'NaN'::numeric) AS nomerstroki_nan,
  count(*) FILTER (WHERE evrika_bonusy = 'NaN'::numeric) AS evrika_bonusy_nan,
  count(*) FILTER (WHERE evrika_spisannye = 'NaN'::numeric) AS evrika_spisannye_nan,
  count(*) FILTER (WHERE summands = 'NaN'::numeric) AS summands_nan,
  count(*) FILTER (WHERE stoimost_bez_skidok = 'NaN'::numeric) AS stoimost_bez_skidok_nan,
  count(*) FILTER (WHERE akciz = 'NaN'::numeric) AS akciz_nan
FROM public.sales_positions;

-- 6.9 NaN в summa/tsena/nomerstroki: распределение по recorder_type
SELECT recorder_type, count(*) FILTER (WHERE summa='NaN'::numeric) AS summa_nan, count(*) FILTER (WHERE tsena='NaN'::numeric) AS tsena_nan, count(*) FILTER (WHERE nomerstroki='NaN'::numeric) AS nomerstroki_nan, count(*) FILTER (WHERE evrika_bonusy='NaN'::numeric) AS bonusy_nan, count(*) FILTER (WHERE summands='NaN'::numeric) AS summands_nan, count(*) AS total FROM public.sales_positions GROUP BY 1 ORDER BY 1;

-- 6.10 NaN в summa: распределение по дате periods родительского sales (join) — по месяцам
SELECT date_trunc('day', s.period)::date AS d, count(*) AS summa_nan_rows FROM public.sales_positions p JOIN public.sales s ON s.id=p.sales_id WHERE p.summa='NaN'::numeric GROUP BY 1 ORDER BY 1;

-- 6.11 Пересечение: позиции с summa=NaN — их родительские sales имеют doc_number NULL? И примеры строк
SELECT count(*) AS nan_rows, count(*) FILTER (WHERE s.doc_number IS NULL) AS parent_docnum_null, count(DISTINCT p.sales_id) AS distinct_docs FROM public.sales_positions p JOIN public.sales s ON s.id=p.sales_id WHERE p.summa='NaN'::numeric;

-- 6.12 Примеры позиций с summa=NaN
SELECT p.id, p.recorder, p.recorder_type, p.line_no, p.stoimost, p.summa, p.tsena, p.nomerstroki, p.kolichestvo, s.period, s.doc_number FROM public.sales_positions p JOIN public.sales s ON s.id=p.sales_id WHERE p.summa='NaN'::numeric ORDER BY s.period DESC LIMIT 8;

-- 7.1 Проверка ТЗ-7: retail_updated_at > etl_updated_at в sales (обе Almaty; нарушение = аномалия)
SELECT count(*) AS violations, max(retail_updated_at - etl_updated_at) AS max_delta FROM public.sales WHERE retail_updated_at > etl_updated_at;

-- 7.2 Проверка ТЗ-7: то же в sales_positions
SELECT count(*) AS violations, max(retail_updated_at - etl_updated_at) AS max_delta FROM public.sales_positions WHERE retail_updated_at > etl_updated_at;

-- 7.3 Контекст: сколько строк вообще имеют retail_updated_at / retail_snapshot_at / etl_updated_at (NULL-покрытие)
SELECT 'sales' AS tbl, count(*) AS total, count(retail_updated_at) AS has_retail_upd, count(retail_snapshot_at) AS has_snapshot, count(etl_updated_at) AS has_etl_upd FROM public.sales UNION ALL SELECT 'sales_positions', count(*), count(retail_updated_at), count(retail_snapshot_at), count(etl_updated_at) FROM public.sales_positions;

-- 8.1 Проверка ТЗ-8: будущие даты в sales (порог = now() Almaty + 1ч; колонки naive Almaty)
WITH nowa AS (SELECT (now() AT TIME ZONE 'Asia/Almaty') + interval '1 hour' AS lim)
SELECT
  count(*) FILTER (WHERE period > lim) AS period_future,
  count(*) FILTER (WHERE etl_loaded_at > lim) AS etl_loaded_future,
  count(*) FILTER (WHERE etl_updated_at > lim) AS etl_updated_future,
  count(*) FILTER (WHERE retail_updated_at > lim) AS retail_updated_future,
  count(*) FILTER (WHERE retail_snapshot_at > lim) AS retail_snapshot_future
FROM public.sales, nowa;

-- 8.2 Проверка ТЗ-8: будущие даты в sales_positions
WITH nowa AS (SELECT (now() AT TIME ZONE 'Asia/Almaty') + interval '1 hour' AS lim)
SELECT
  count(*) FILTER (WHERE etl_loaded_at > lim) AS etl_loaded_future,
  count(*) FILTER (WHERE etl_updated_at > lim) AS etl_updated_future,
  count(*) FILTER (WHERE retail_updated_at > lim) AS retail_updated_future,
  count(*) FILTER (WHERE retail_snapshot_at > lim) AS retail_snapshot_future
FROM public.sales_positions, nowa;

-- 8.3 Контроль: текущее время Almaty и UTC для протокола + max значений таймстампов
SELECT now() AT TIME ZONE 'Asia/Almaty' AS now_almaty, now() AT TIME ZONE 'UTC' AS now_utc, (SELECT max(period) FROM public.sales) AS max_period, (SELECT max(etl_updated_at) FROM public.sales) AS max_etl_upd, (SELECT max(retail_updated_at) FROM public.sales) AS max_retail_upd, (SELECT max(etl_loaded_at) FROM public.sales) AS max_etl_loaded;

-- 9.1 Проверка ТЗ-9: строк по date(period) в sales + выбросы >3x медианы
WITH daily AS (SELECT period::date AS d, count(*) AS cnt FROM public.sales GROUP BY 1),
med AS (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY cnt) AS m FROM daily)
SELECT d, cnt, round(cnt/m::numeric,2) AS x_median, CASE WHEN cnt > 3*m THEN '<< ВЫБРОС' ELSE '' END AS flag FROM daily, med ORDER BY d;

-- 9.2 Проверка ТЗ-9: строк по date(period родителя) в sales_positions + выбросы >3x медианы
WITH daily AS (SELECT s.period::date AS d, count(*) AS cnt FROM public.sales_positions p JOIN public.sales s ON s.id=p.sales_id GROUP BY 1),
med AS (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY cnt) AS m FROM daily)
SELECT d, cnt, round(cnt/m::numeric,2) AS x_median, CASE WHEN cnt > 3*m THEN '<< ВЫБРОС' ELSE '' END AS flag FROM daily, med ORDER BY d;

-- 9.3 Проверка ТЗ-9: строк по date(etl_loaded_at) в обеих таблицах (ВНИМАНИЕ: колонка TZ-смешанная — full-load строки в Almaty, incremental в UTC)
WITH s AS (SELECT etl_loaded_at::date AS d, count(*) AS sales_cnt FROM public.sales GROUP BY 1),
p AS (SELECT etl_loaded_at::date AS d, count(*) AS pos_cnt FROM public.sales_positions GROUP BY 1)
SELECT coalesce(s.d,p.d) AS d, coalesce(sales_cnt,0) AS sales_cnt, coalesce(pos_cnt,0) AS pos_cnt FROM s FULL JOIN p ON s.d=p.d ORDER BY 1;

-- 10.1 Проверка ТЗ-10: покрытие distinct (recorder, recorder_type) в обе стороны
WITH ps AS (SELECT DISTINCT recorder, recorder_type FROM public.sales_positions),
ss AS (SELECT recorder, recorder_type FROM public.sales)
SELECT
  (SELECT count(*) FROM ps) AS distinct_pairs_positions,
  (SELECT count(*) FROM ss) AS pairs_sales,
  (SELECT count(*) FROM ps LEFT JOIN ss USING (recorder, recorder_type) WHERE ss.recorder IS NULL) AS pos_pairs_not_in_sales,
  (SELECT count(*) FROM ss LEFT JOIN ps USING (recorder, recorder_type) WHERE ps.recorder IS NULL) AS sales_pairs_not_in_pos;

-- 9.4 Медианы дневных объёмов по date(etl_loaded_at) для формальной оценки выброса 2026-06-25
WITH daily AS (SELECT etl_loaded_at::date AS d, count(*) AS cnt FROM public.sales GROUP BY 1)
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY cnt) AS median_cnt, max(cnt) AS max_cnt FROM daily;

-- 11.1 Перепроверка ТЗ-1 иным способом: count(*) - count(sales_id)
SELECT count(*) - count(sales_id) AS null_sales_id, count(*) - count(recorder_type) AS null_rt, count(*) - count(line_no) AS null_line_no FROM public.sales_positions;

-- 11.2 Перепроверка NaN иным способом (::text = 'NaN')
SELECT count(*) FILTER (WHERE summa::text='NaN') AS summa_nan, count(*) FILTER (WHERE tsena::text='NaN') AS tsena_nan, count(*) FILTER (WHERE evrika_bonusy::text='NaN') AS bonusy_nan, count(*) FILTER (WHERE summands::text='NaN') AS summands_nan FROM public.sales_positions;

-- 11.3 Кейс 352 (Отчёт о розничных продажах): его sales-строка и позиции (позиции из AccumRg — должны существовать)
SELECT s.id, s.recorder, s.recorder_type, s.period, s.doc_number, s.is_posted, (SELECT count(*) FROM public.sales_positions p WHERE p.sales_id=s.id) AS positions_cnt FROM public.sales s WHERE s.recorder_type=352;

-- 11.4 Позиции 352: register-поля заполнены, doc-поля NaN (док-таблицы для 352 не грузятся)
SELECT p.id, p.line_no, p.nomenklatura, p.kolichestvo, p.stoimost, p.summa, p.tsena, p.nomerstroki FROM public.sales_positions p WHERE p.recorder_type=352 ORDER BY p.line_no;

-- =====================================================================
-- ПЕРЕПРОВЕРКА (независимый повторный прогон, 2026-07-21 12:39 Almaty)
-- =====================================================================

-- R.1 Повторный базовый объём (живая БД: 64 481 / 87 720; дрейф от первого прогона +46/+50 — норма, incremental каждые ~5 мин)
SELECT (SELECT count(*) FROM public.sales) AS sales_cnt, (SELECT count(*) FROM public.sales_positions) AS pos_cnt;

-- R.2 Повтор ТЗ-1..ТЗ-4 одним блоком: NULL-ключи, дубли, orphan'ы (все = 0, подтверждено)
SELECT
  (SELECT count(*) FROM public.sales_positions WHERE sales_id IS NULL) AS null_sales_id,
  (SELECT count(*) FROM public.sales WHERE recorder_type IS NULL) AS s_rt_null,
  (SELECT count(*) FROM public.sales_positions WHERE recorder_type IS NULL) AS p_rt_null,
  (SELECT count(*) FROM public.sales_positions WHERE line_no IS NULL) AS p_line_null,
  (SELECT count(*) FROM (SELECT recorder, recorder_type FROM public.sales GROUP BY 1,2 HAVING count(*)>1) d) AS s_dup_groups,
  (SELECT count(*) FROM (SELECT recorder, recorder_type, line_no FROM public.sales_positions GROUP BY 1,2,3 HAVING count(*)>1) d) AS p_dup_groups,
  (SELECT count(*) FROM public.sales_positions p LEFT JOIN public.sales s ON s.id=p.sales_id WHERE s.id IS NULL) AS orphan_by_id,
  (SELECT count(*) FROM public.sales_positions p LEFT JOIN public.sales s ON s.recorder=p.recorder AND s.recorder_type=p.recorder_type WHERE s.id IS NULL) AS orphan_by_pair;

-- R.3 Повтор ТЗ-5: childless sales по типам (476 → 6, 415 → 1; итого 7 — стабильно с первого прогона)
SELECT s.recorder_type, count(*) FROM public.sales s WHERE NOT EXISTS (SELECT 1 FROM public.sales_positions p WHERE p.sales_id=s.id) GROUP BY 1 ORDER BY 1;

-- R.4 Повтор ТЗ-6 NULL: doc_number по типам (352 → 1 норма; 476 → 18, 415 → 3, 254 → 0 — 21 аномалия, стабильно)
SELECT recorder_type, count(*) FILTER (WHERE doc_number IS NULL) AS doc_number_null FROM public.sales GROUP BY 1 ORDER BY 1;

-- R.5 Повтор ТЗ-6 NaN: два независимых способа ('NaN'::numeric и ::text='NaN') дают одинаковые числа:
-- summa/tsena/nomerstroki = 216 (стабильно); evrika_bonusy/spisannye = 67 421, summands = 67 689 (выросли на +40 с новыми строками — NaN продолжает поступать)
SELECT count(*) FILTER (WHERE summa='NaN'::numeric) AS summa_nan, count(*) FILTER (WHERE tsena='NaN'::numeric) AS tsena_nan,
       count(*) FILTER (WHERE nomerstroki='NaN'::numeric) AS nomerstroki_nan,
       count(*) FILTER (WHERE evrika_bonusy='NaN'::numeric) AS bonusy_nan,
       count(*) FILTER (WHERE evrika_spisannye='NaN'::numeric) AS spisannye_nan,
       count(*) FILTER (WHERE summands='NaN'::numeric) AS summands_nan,
       count(*) FILTER (WHERE summa::text='NaN') AS summa_nan_v2,
       count(*) FILTER (WHERE evrika_bonusy::text='NaN') AS bonusy_nan_v2
FROM public.sales_positions;

-- R.6 Повтор ТЗ-7/ТЗ-8: инверсии watermark и будущие даты (все = 0, подтверждено)
WITH nowa AS (SELECT (now() AT TIME ZONE 'Asia/Almaty') + interval '1 hour' AS lim)
SELECT
  (SELECT count(*) FROM public.sales WHERE retail_updated_at > etl_updated_at) AS s_inversions,
  (SELECT count(*) FROM public.sales_positions WHERE retail_updated_at > etl_updated_at) AS p_inversions,
  (SELECT count(*) FROM public.sales, nowa WHERE period>lim OR etl_loaded_at>lim OR etl_updated_at>lim OR retail_updated_at>lim OR retail_snapshot_at>lim) AS s_future,
  (SELECT count(*) FROM public.sales_positions, nowa WHERE etl_loaded_at>lim OR etl_updated_at>lim OR retail_updated_at>lim OR retail_snapshot_at>lim) AS p_future;

-- R.7 Повтор ТЗ-10: покрытие пар (64 474 vs 64 481; 0 пар positions вне sales; 7 пар sales вне positions = childless из ТЗ-5)
WITH ps AS (SELECT DISTINCT recorder, recorder_type FROM public.sales_positions),
     ss AS (SELECT recorder, recorder_type FROM public.sales)
SELECT (SELECT count(*) FROM ps) AS pairs_pos, (SELECT count(*) FROM ss) AS pairs_sales,
       (SELECT count(*) FROM ps LEFT JOIN ss USING (recorder, recorder_type) WHERE ss.recorder IS NULL) AS pos_not_in_sales,
       (SELECT count(*) FROM ss LEFT JOIN ps USING (recorder, recorder_type) WHERE ps.recorder IS NULL) AS sales_not_in_pos;

-- R.8 Повтор кейса 352: id=17954, doc_number NULL (норма), 2 позиции из AccumRg — подтверждено
SELECT s.id, s.recorder_type, s.doc_number IS NULL AS docnum_is_null,
       (SELECT count(*) FROM public.sales_positions p WHERE p.sales_id=s.id) AS positions_cnt
FROM public.sales s WHERE s.recorder_type=352;

-- R.9 Контроль свежести на момент перепроверки: now Almaty = 12:39:12, max(etl_updated_at) = 12:35:06 (лаг ~4 мин)
SELECT now() AT TIME ZONE 'Asia/Almaty' AS now_almaty, (SELECT max(etl_updated_at) FROM public.sales) AS max_etl_upd;

-- =====================================================================
-- ФИНАЛЬНАЯ ВЕРИФИКАЦИЯ (третий независимый прогон, 2026-07-21 15:59 Almaty)
-- =====================================================================

-- F.1 Объёмы на момент финальной верификации (64 920 / 88 274 — живой дрейф от 12:39, норма)
SELECT (SELECT count(*) FROM public.sales) AS sales, (SELECT count(*) FROM public.sales_positions) AS positions;

-- F.2 ТЗ-1/ТЗ-2: NULL-ключи (все = 0, стабильно во всех трёх прогонах)
SELECT
  (SELECT count(*) FROM public.sales_positions WHERE sales_id IS NULL) AS null_sales_id,
  (SELECT count(*) FROM public.sales WHERE recorder_type IS NULL) AS s_rt_null,
  (SELECT count(*) FROM public.sales_positions WHERE recorder_type IS NULL) AS p_rt_null,
  (SELECT count(*) FROM public.sales_positions WHERE line_no IS NULL) AS p_line_null;

-- F.3 ТЗ-3/ТЗ-4: дубли и orphan'ы (все = 0, стабильно)
SELECT
  (SELECT count(*) FROM (SELECT recorder, recorder_type FROM public.sales GROUP BY 1,2 HAVING count(*)>1) d) AS s_dup,
  (SELECT count(*) FROM (SELECT recorder, recorder_type, line_no FROM public.sales_positions GROUP BY 1,2,3 HAVING count(*)>1) d) AS p_dup,
  (SELECT count(*) FROM public.sales_positions p LEFT JOIN public.sales s ON s.id=p.sales_id WHERE s.id IS NULL) AS orphan_id,
  (SELECT count(*) FROM public.sales_positions p LEFT JOIN public.sales s ON s.recorder=p.recorder AND s.recorder_type=p.recorder_type WHERE s.id IS NULL) AS orphan_pair;

-- F.4 ТЗ-5: childless sales (476 → 6, 415 → 1; итого 7 — не изменилось за 4 часа, дыра застыла)
SELECT s.recorder_type, count(*) FROM public.sales s
WHERE NOT EXISTS (SELECT 1 FROM public.sales_positions p WHERE p.sales_id=s.id) GROUP BY 1 ORDER BY 1;

-- F.5 ТЗ-6 NULL: doc_number по типам (352 → 1 норма; 476 → 18, 415 → 3, 254 → 0 — 21 аномалия, стабильно)
SELECT recorder_type, count(*) FILTER (WHERE doc_number IS NULL) FROM public.sales GROUP BY 1 ORDER BY 1;

-- F.6 ТЗ-6 NaN: summa/tsena/nomerstroki = 217 (было 216 в 12:39 — дефект АКТИВЕН, +1 строка за ~3.5ч);
--     evrika_bonusy/spisannye = 67 822, summands = 68 090 (растут с каждым инкрементом); v2-метод даёт то же
SELECT
  count(*) FILTER (WHERE summa='NaN'::numeric) AS summa_nan,
  count(*) FILTER (WHERE tsena='NaN'::numeric) AS tsena_nan,
  count(*) FILTER (WHERE nomerstroki='NaN'::numeric) AS nomerstroki_nan,
  count(*) FILTER (WHERE stoimost='NaN'::numeric) AS stoimost_nan,
  count(*) FILTER (WHERE kolichestvo='NaN'::numeric) AS kolich_nan,
  count(*) FILTER (WHERE nds='NaN'::numeric) AS nds_nan,
  count(*) FILTER (WHERE evrika_bonusy='NaN'::numeric) AS bonusy_nan,
  count(*) FILTER (WHERE evrika_spisannye='NaN'::numeric) AS spis_nan,
  count(*) FILTER (WHERE summands='NaN'::numeric) AS summands_nan,
  count(*) FILTER (WHERE summa::text='NaN') AS summa_nan_v2
FROM public.sales_positions;

-- F.7 ТЗ-7/ТЗ-8: инверсии watermark и будущие даты (все = 0, стабильно)
WITH nowa AS (SELECT (now() AT TIME ZONE 'Asia/Almaty') + interval '1 hour' AS lim)
SELECT
  (SELECT count(*) FROM public.sales WHERE retail_updated_at > etl_updated_at) AS s_inv,
  (SELECT count(*) FROM public.sales_positions WHERE retail_updated_at > etl_updated_at) AS p_inv,
  (SELECT count(*) FROM public.sales, nowa WHERE period>lim OR etl_loaded_at>lim OR etl_updated_at>lim OR retail_updated_at>lim OR retail_snapshot_at>lim) AS s_future,
  (SELECT count(*) FROM public.sales_positions, nowa WHERE etl_loaded_at>lim OR etl_updated_at>lim OR retail_updated_at>lim OR retail_snapshot_at>lim) AS p_future;

-- F.8 ТЗ-10: покрытие пар (64 913 vs 64 920; 0 / 7 — стабильно)
WITH ps AS (SELECT DISTINCT recorder, recorder_type FROM public.sales_positions),
     ss AS (SELECT recorder, recorder_type FROM public.sales)
SELECT (SELECT count(*) FROM ps) AS pairs_pos, (SELECT count(*) FROM ss) AS pairs_sales,
       (SELECT count(*) FROM ps LEFT JOIN ss USING (recorder, recorder_type) WHERE ss.recorder IS NULL) AS pos_not_in_sales,
       (SELECT count(*) FROM ss LEFT JOIN ps USING (recorder, recorder_type) WHERE ps.recorder IS NULL) AS sales_not_in_pos;

-- F.9 Кейс 352: id=17954, doc_number NULL (норма), 2 позиции из AccumRg — подтверждено в третий раз
SELECT s.id, s.recorder_type, s.doc_number IS NULL AS docnum_null,
       (SELECT count(*) FROM public.sales_positions p WHERE p.sales_id=s.id) AS pos_cnt
FROM public.sales s WHERE s.recorder_type=352;

-- F.10 Контроль свежести: now Almaty = 15:59:17, max(etl_updated_at) = 15:55:07 (лаг ~4 мин)
SELECT now() AT TIME ZONE 'Asia/Almaty' AS now_almaty, (SELECT max(etl_updated_at) FROM public.sales) AS max_etl_upd;
