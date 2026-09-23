-- Конфигурация синхронизации fact_sales_positions.
-- Это ДАННЫЕ control plane, а не код: движок их читает, а не содержит.
-- Чтобы подключить новый объект, добавляется такая же пара INSERT'ов — нового
-- загрузчика писать не нужно.
--
-- checksum_columns намеренно включает все ссылочные id: именно контрольная сумма
-- по ним ловит перепроведение документа, при котором меняется измерение, а все
-- суммы остаются прежними. Меры в контрольную сумму не входят — они проверяются
-- через sum и sum квадратов (см. комментарий в 010_ch_sync_control.sql).

INSERT INTO etl_meta.ch_sync
   (code, description, source_conn_id, source_type, source_query,
    target_database, target_table, partition_expr, order_by, load_mode,
    partition_column, partition_granularity, watermark_column, business_key,
    batch_size, empty_partition_policy, hot_window, sweep_interval_min,
    checksum_columns, measure_columns, is_active, priority)
VALUES (
    'fact_sales_positions', 'Строки продаж: документ x строка. Дата денормализована из шапки public.sales.',
    'etl_prod', 'postgres', $SRC$
SELECT s.period, s.podrazdelenie_id, s.sklad_id, s.kontragent_id, s.otvetstvennyy_id,
       s.dogovor_id, s.zakaz_id, p.nomenklatura_id, p.sklad_id AS line_sklad_id,
       p.kachestvo_id AS line_kachestvo_id, p.recorder, p.recorder_type, p.line_no,
       p.kolichestvo, p.stoimost, p.stoimost_bez_skidok, p.nds, p.akciz,
       p.summa, p.tsena, p.evrika_bonusy, p.evrika_spisannye, p.summands,
       p.etl_updated_at
  FROM public.sales_positions p
  JOIN public.sales s ON s.id = p.sales_id
 WHERE p.recorder IS NOT NULL AND p.recorder_type IS NOT NULL
   AND p.line_no IS NOT NULL AND p.etl_updated_at IS NOT NULL
$SRC$,
    'analytics_poc', 'fact_sales_positions', 'toYYYYMM(period)', ARRAY['period', 'podrazdelenie_id', 'nomenklatura_id', 'recorder_type', 'recorder', 'line_no'], 'partitioned',
    'period', 'month', 'etl_updated_at', ARRAY['recorder', 'recorder_type', 'line_no'],
    100000, 'fail', 3, 60,
    ARRAY['recorder', 'recorder_type', 'line_no', 'nomenklatura_id', 'podrazdelenie_id', 'sklad_id', 'kontragent_id', 'otvetstvennyy_id', 'dogovor_id', 'zakaz_id', 'line_sklad_id', 'line_kachestvo_id'],
    ARRAY['kolichestvo', 'stoimost', 'stoimost_bez_skidok', 'nds', 'akciz'], true, 10)
ON CONFLICT (code) DO NOTHING;

INSERT INTO etl_meta.ch_sync_columns (sync_id, ordinal, source_expr, target_column, target_type, codec)
SELECT s.id, v.ordinal, v.source_expr, v.target_column, v.target_type, v.codec
  FROM etl_meta.ch_sync s, (VALUES
    (1, 'period', 'period', 'DateTime', 'Delta, ZSTD(1)'),
    (2, 'coalesce(podrazdelenie_id, 0)', 'podrazdelenie_id', 'UInt16', 'T64, ZSTD(1)'),
    (3, 'coalesce(sklad_id, 0)', 'sklad_id', 'UInt16', 'T64, ZSTD(1)'),
    (4, 'coalesce(kontragent_id, 0)', 'kontragent_id', 'UInt32', 'T64, ZSTD(1)'),
    (5, 'coalesce(otvetstvennyy_id, 0)', 'otvetstvennyy_id', 'UInt32', 'T64, ZSTD(1)'),
    (6, 'coalesce(dogovor_id, 0)', 'dogovor_id', 'UInt32', 'T64, ZSTD(1)'),
    (7, 'coalesce(zakaz_id, 0)', 'zakaz_id', 'UInt32', 'T64, ZSTD(1)'),
    (8, 'coalesce(nomenklatura_id, 0)', 'nomenklatura_id', 'UInt32', 'T64, ZSTD(1)'),
    (9, 'coalesce(line_sklad_id, 0)', 'line_sklad_id', 'UInt16', 'T64, ZSTD(1)'),
    (10, 'coalesce(line_kachestvo_id, 0)', 'line_kachestvo_id', 'UInt8', 'T64, ZSTD(1)'),
    (11, 'recorder', 'recorder', 'UUID', 'ZSTD(1)'),
    (12, 'recorder_type', 'recorder_type', 'UInt16', 'T64, ZSTD(1)'),
    (13, 'line_no', 'line_no', 'UInt16', 'T64, ZSTD(1)'),
    (14, 'coalesce(kolichestvo, 0)', 'kolichestvo', 'Decimal(18,4)', 'ZSTD(1)'),
    (15, 'coalesce(stoimost, 0)', 'stoimost', 'Decimal(18,4)', 'ZSTD(1)'),
    (16, 'coalesce(stoimost_bez_skidok, 0)', 'stoimost_bez_skidok', 'Decimal(18,4)', 'ZSTD(1)'),
    (17, 'coalesce(nds, 0)', 'nds', 'Decimal(18,4)', 'ZSTD(1)'),
    (18, 'coalesce(akciz, 0)', 'akciz', 'Decimal(18,4)', 'ZSTD(1)'),
    (19, 'coalesce(summa, 0)', 'summa', 'Decimal(18,4)', 'ZSTD(1)'),
    (20, 'coalesce(tsena, 0)', 'tsena', 'Decimal(18,4)', 'ZSTD(1)'),
    (21, 'coalesce(evrika_bonusy, 0)', 'evrika_bonusy', 'Decimal(18,4)', 'ZSTD(1)'),
    (22, 'coalesce(evrika_spisannye, 0)', 'evrika_spisannye', 'Decimal(18,4)', 'ZSTD(1)'),
    (23, 'coalesce(summands, 0)', 'summands', 'Decimal(18,4)', 'ZSTD(1)'),
    (24, 'etl_updated_at', 'etl_updated_at', 'DateTime', 'Delta, ZSTD(1)')
  ) AS v(ordinal, source_expr, target_column, target_type, codec)
 WHERE s.code = 'fact_sales_positions'
ON CONFLICT (sync_id, target_column) DO NOTHING;
