-- PROD etl_prod, 2026-09-15 (применено через CREATE INDEX CONCURRENTLY, без блокировки записи).
-- Два индекса под резолв FK позиций → шапка и под проверки целостности.
--
-- До них (замер EXPLAIN ANALYZE на 10.4 млн позиций / 6.0 млн шапок):
--   post_load «sales_id IS NULL»            Seq Scan 10 424 222 строк, 3.3 с каждый тик
--   post_load «dangling в окне 60 мин»      Seq Scan 6 026 326 шапок,  7.6 с
-- После: оба шага идут Index Scan (см. планы в логе сессии 2026-09-15).
--
-- CONCURRENTLY нельзя внутри транзакции — файл выполняется без BEGIN/COMMIT.
-- Внимание: сборка ждёт завершения всех более старых транзакций; зависший долгий SELECT
-- держит фазу «waiting for old snapshots» (в этой сессии пришлось отменить такой запрос).

-- 1) Резолв FK и проверка «позиции без шапки»: WHERE sales_id IS NULL, а также anti-join по sales_id.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_positions_sales_id
    ON public.sales_positions (sales_id);

-- 2) Окно последних загрузок в post_load (починка «висячих» ссылок) и диагностика прогонов.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_etl_updated_at
    ON public.sales (etl_updated_at);
