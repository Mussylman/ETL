-- PROD etl_prod, 2026-09-15. Резолв FK позиций → шапка чинит и «висячие» ссылки, не только NULL.
--
-- Дефект (аудит 2026-09-15): при аварии прогона позиции уже вставлены с sales_id IS NULL,
-- откат удалял только шапку, retry пересоздавал её с НОВЫМ id — и sales_id старых позиций
-- указывал на удалённую строку. post_load чинил лишь `sales_id IS NULL`, поэтому dangling
-- оставались навсегда (33 строки, 31 чек от 14.09). Откат в движке теперь удаляет и позиции,
-- а этот резолв закрывает уже существующие и любые будущие расхождения.
--
-- Два точечных UPDATE вместо одного: первый ловит NULL где угодно (индекс
-- idx_sales_positions_sales_id), второй — dangling только у документов последних загрузок
-- (индекс idx_sales_etl_updated_at). Полного UPDATE таблицы нет.
BEGIN;
UPDATE etl_meta.register_targets SET post_load_sql = replace(post_load_sql,
$OLD$-- FK на документ: по natural key (recorder, recorder_type)
UPDATE public.sales_positions AS f SET sales_id = d.id FROM public.sales AS d
WHERE f.recorder = d.recorder AND f.recorder_type = d.recorder_type AND f.sales_id IS NULL;$OLD$,
$NEW$-- FK на документ по natural key (recorder, recorder_type).
-- 1) не привязанные строки — где угодно (индекс idx_sales_positions_sales_id)
UPDATE public.sales_positions AS f SET sales_id = d.id FROM public.sales AS d
WHERE f.sales_id IS NULL AND f.recorder = d.recorder AND f.recorder_type = d.recorder_type;
-- 2) «висячие» ссылки: шапку пересоздали с новым id после отката упавшего прогона.
--    Скоуп — документы, тронутые последними загрузками (индекс idx_sales_etl_updated_at).
UPDATE public.sales_positions AS f SET sales_id = d.id FROM public.sales AS d
WHERE d.etl_updated_at >= timezone('Asia/Almaty', now()) - interval '60 minutes'
  AND f.recorder = d.recorder AND f.recorder_type = d.recorder_type
  AND f.sales_id IS NOT NULL AND f.sales_id <> d.id;$NEW$)
WHERE id = 81 AND post_load_sql LIKE '%AND f.sales_id IS NULL;%' AND post_load_sql NOT LIKE '%f.sales_id <> d.id%';
COMMIT;
