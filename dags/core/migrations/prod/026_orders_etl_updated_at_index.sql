-- PROD etl_prod, 2026-09-16. Индекс под окно последних загрузок в post_load orders/order_positions.
--
-- Его требуют сразу два шага: удаление исчезнувших строк позиций (миграция 022) и резолв
-- «висячих» FK (миграция 025) — оба фильтруют orders.etl_updated_at >= now() - 60 минут.
-- Без индекса это Seq Scan по 445 тыс. шапок на каждом тике инкремента.
--
-- order_positions(order_id) уже существует (idx_order_positions_order_id) — дубль не создаём.
-- CONCURRENTLY нельзя внутри транзакции: файл выполняется без BEGIN/COMMIT.
-- Сборка ждёт завершения всех более старых транзакций: зависший долгий SELECT держит
-- фазу «waiting for old snapshots» (см. лог сессии 2026-09-15).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_etl_updated_at
    ON public.orders (etl_updated_at);
