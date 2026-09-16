-- PROD etl_prod, 2026-09-16. Превентивный hardening orders → order_positions по образцу sales (023).
--
-- Данных дефекта нет (read-only проверка 16.09: order_id IS NULL = 0, dangling = 0), но схема
-- загрузки идентична sales: шапки и позиции пишутся разными транзакциями, post_load позиций
-- ставит order_id последним шагом. Откат движка при аварии удаляет документ целиком
-- (etl_engine._cleanup_incomplete_headers, фикс 2026-09-15), однако если шапка пересоздаётся
-- с новым id, старый order_id остаётся «висячим», а прежний резолв чинил только `order_id IS NULL`.
--
-- Два точечных UPDATE, полного UPDATE таблицы нет:
--   1) order_id IS NULL — где угодно (индекс idx_order_positions_order_id);
--   2) dangling (order_id <> o.id) — только документы последних загрузок (индекс idx_orders_etl_updated_at).
BEGIN;
UPDATE etl_meta.register_targets SET post_load_sql = replace(post_load_sql,
$OLD$-- FK на шапку по natural key
UPDATE public.order_positions p SET order_id = o.id FROM public.orders o
WHERE p.recorder = o.recorder AND p.recorder_type = o.recorder_type AND p.order_id IS NULL;$OLD$,
$NEW$-- FK на шапку по natural key (recorder, recorder_type).
-- 1) не привязанные строки — где угодно (индекс idx_order_positions_order_id)
UPDATE public.order_positions p SET order_id = o.id FROM public.orders o
WHERE p.order_id IS NULL AND p.recorder = o.recorder AND p.recorder_type = o.recorder_type;
-- 2) «висячие» ссылки: шапку пересоздали с новым id после отката упавшего прогона.
--    Скоуп — документы последних загрузок (индекс idx_orders_etl_updated_at).
UPDATE public.order_positions p SET order_id = o.id FROM public.orders o
WHERE o.etl_updated_at >= timezone('Asia/Almaty', now()) - interval '60 minutes'
  AND p.recorder = o.recorder AND p.recorder_type = o.recorder_type
  AND p.order_id IS NOT NULL AND p.order_id <> o.id;$NEW$)
WHERE target_table = 'order_positions'
  AND post_load_sql LIKE '%AND p.order_id IS NULL;%'
  AND post_load_sql NOT LIKE '%p.order_id <> o.id%';
COMMIT;
