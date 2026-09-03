-- PROD etl_prod, 2026-09-03: связь sales → orders через zakaz_id BIGINT (стандарт ссылок: existing register → *_id).
-- raw_refs.zakaz {type:271, uid} остаётся source-of-truth; hash GUID→BIGINT не используется; физический FK не ставим —
-- как и у sales_id / doc_sale_id / order_id / dim-FK (nullable-ссылки, резолв в post_load).
BEGIN;

ALTER TABLE public.sales ADD COLUMN IF NOT EXISTS zakaz_id bigint;
CREATE INDEX IF NOT EXISTS idx_sales_zakaz_id ON public.sales (zakaz_id);

-- backfill: только type = 271 (ЗаказПокупателя), только через natural key заказа
UPDATE public.sales s SET zakaz_id = o.id
FROM public.orders o
WHERE s.zakaz_id IS NULL AND s.raw_refs ? 'zakaz'
  AND (s.raw_refs->'zakaz'->>'type')::int = 271
  AND o.recorder_type = 271
  AND o.recorder = (s.raw_refs->'zakaz'->>'uid')::uuid;

COMMIT;

-- Штатный резолв (без ручного backfill). Предикат намеренно узкий: только zakaz_id IS NULL AND raw_refs ? 'zakaz'
-- (после backfill это ~700 строк вне истории orders + свежие продажи; idx_sales_zakaz_id закрывает IS NULL).
BEGIN;
-- (a) sales post_load: новая продажа с заказом, который уже в orders → zakaz_id сразу
UPDATE etl_meta.register_targets
SET post_load_sql = replace(post_load_sql,
  '-- zakaz (ЗаказПокупателя, _Document271): регистр order в DWH ещё нет — только raw_refs.zakaz {type, uid}, zakaz_id не выдумываем.',
  $PL$-- zakaz_id: ссылка на регистр orders (ЗаказПокупателя, 271) по natural key. Заказ вне истории orders / непроведён → NULL,
-- {type, uid} остаются в raw_refs. Late-resolve (заказ пришёл позже продажи) закрывает post_load orders.
UPDATE public.sales s SET zakaz_id = o.id FROM public.orders o
WHERE s.zakaz_id IS NULL AND s.raw_refs ? 'zakaz'
  AND o.recorder_type = (s.raw_refs->'zakaz'->>'type')::int AND o.recorder = (s.raw_refs->'zakaz'->>'uid')::uuid;$PL$)
WHERE register_id = 62 AND target_table = 'sales' AND post_load_sql NOT LIKE '%SET zakaz_id%';

-- (b) orders post_load: заказ появился позже продажи → дозаполнить существующие sales.zakaz_id IS NULL
UPDATE etl_meta.register_targets
SET post_load_sql = replace(post_load_sql,
  $OLD$-- Этап 12 (включается после ALTER TABLE public.sales ADD COLUMN zakaz_id bigint):
-- UPDATE public.sales s SET zakaz_id = o.id FROM public.orders o
-- WHERE s.zakaz_id IS NULL AND o.etl_updated_at >= timezone('Asia/Almaty', now()) - interval '60 minutes'
--   AND s.raw_refs @> jsonb_build_object('zakaz', jsonb_build_object('type', 271, 'uid', o.recorder::text));$OLD$,
  $NEW$-- Late-resolve sales.zakaz_id: продажа могла прийти раньше заказа. Узкий предикат (IS NULL + raw_refs ? 'zakaz'),
-- индекс idx_sales_zakaz_id; orders ищется по natural key (recorder, recorder_type).
UPDATE public.sales s SET zakaz_id = o.id FROM public.orders o
WHERE s.zakaz_id IS NULL AND s.raw_refs ? 'zakaz'
  AND o.recorder_type = (s.raw_refs->'zakaz'->>'type')::int AND o.recorder = (s.raw_refs->'zakaz'->>'uid')::uuid;$NEW$)
WHERE target_table = 'orders' AND post_load_sql NOT LIKE '%Late-resolve sales.zakaz_id%';
COMMIT;
