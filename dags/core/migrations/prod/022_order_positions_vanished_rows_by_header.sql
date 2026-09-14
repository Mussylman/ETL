-- PROD etl_prod, 2026-09-14 (применено). Удаление исчезнувших строк позиций заказа — по шапке.
-- Старое правило (MAX(etl_updated_at) по строкам того же документа) не видело заказы, у которых в 1С
-- удалили ВСЕ строки товаров: ни одна строка не обновлялась → документ не попадал в окно. Аудит: 55 строк в 26 заказах.
-- Шапка перезаписывается при каждой загрузке документа, поэтому строка старше своей шапки на 30+ минут — удалённая в 1С.
UPDATE etl_meta.register_targets SET post_load_sql = regexp_replace(post_load_sql,
  E'-- Строки, исчезнувшие из документа при перепроведении.*?interval \'30 minutes\';',
  $NEW$-- Строки, исчезнувшие из документа при перепроведении: ориентир — шапка (она перезаписывается при каждой
-- загрузке документа), строки старше шапки на 30+ минут — удалённые в 1С. Правило по строкам самого
-- документа не видело заказы, у которых удалили ВСЕ строки товаров (аудит 2026-09-14: 55 строк в 26 заказах).
DELETE FROM public.order_positions p
USING public.orders o
WHERE o.id = p.order_id
  AND o.etl_updated_at >= timezone('Asia/Almaty', now()) - interval '60 minutes'
  AND p.etl_updated_at < o.etl_updated_at - interval '30 minutes';$NEW$, 's')
WHERE target_table = 'order_positions' AND post_load_sql ~ 'Строки, исчезнувшие из документа' AND post_load_sql !~ 'USING public.orders o';
-- Тот же шаблон генерирует конфигуратор (dao.generate_post_load_sql) для новых регистров с parent_target_id.
