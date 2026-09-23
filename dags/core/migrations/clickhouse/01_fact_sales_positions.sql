-- Первый бизнес-факт POC: строки продаж.
-- Grain: документ × тип документа × номер строки. Business key (recorder, recorder_type, line_no).
--
-- ENGINE = MergeTree, НЕ ReplacingMergeTree: period / podrazdelenie_id / nomenklatura_id
-- потенциально изменяемы, а Replacing схлопывает по полному ORDER BY — при смене любого
-- из них в таблице остались бы обе версии строки. Корректность и идемпотентность даёт
-- полная замена месячной партиции.
--
-- NULL не используем: id справочников начинаются с 1, поэтому 0 = «не указано».
-- Разрядность подобрана по фактическим максимумам в PostgreSQL, а не на глаз.

CREATE TABLE analytics_poc.fact_sales_positions
(
    -- дата: в sales_positions её нет, денормализована из шапки public.sales.
    -- PostgreSQL timestamp without time zone, ClickHouse timezone Etc/UTC —
    -- перенос без сдвига проверен на 10 строках около полуночи.
    period              DateTime      CODEC(Delta, ZSTD(1)),

    -- измерения из ШАПКИ документа
    podrazdelenie_id    UInt16        CODEC(T64, ZSTD(1)),   -- max 267
    sklad_id            UInt16        CODEC(T64, ZSTD(1)),   -- max 416
    kontragent_id       UInt32        CODEC(T64, ZSTD(1)),   -- max 2 758 914
    otvetstvennyy_id    UInt32        CODEC(T64, ZSTD(1)),   -- max 112 793 — в UInt16 НЕ влезает
    dogovor_id          UInt32        CODEC(T64, ZSTD(1)),   -- max 1 001 240
    zakaz_id            UInt32        CODEC(T64, ZSTD(1)),   -- max 675 734

    -- измерения СТРОКИ; у ЧекККМ (тип 476) пусты — это свойство источника, не потеря
    nomenklatura_id     UInt32        CODEC(T64, ZSTD(1)),   -- max 1 404 135
    line_sklad_id       UInt16        CODEC(T64, ZSTD(1)),   -- max 416
    line_kachestvo_id   UInt8         CODEC(T64, ZSTD(1)),   -- max 14

    -- business key и drill-down до документа
    recorder            UUID          CODEC(ZSTD(1)),
    recorder_type       UInt16        CODEC(T64, ZSTD(1)),   -- max 476
    line_no             UInt16        CODEC(T64, ZSTD(1)),   -- max 801

    -- меры из регистра _AccumRg17844: заполнены 100%, на них считаем итоги.
    -- Максимум |stoimost| = 640 000 000 — Decimal(18,4) с запасом, NaN в источнике нет.
    kolichestvo         Decimal(18,4) CODEC(ZSTD(1)),
    stoimost            Decimal(18,4) CODEC(ZSTD(1)),
    stoimost_bez_skidok Decimal(18,4) CODEC(ZSTD(1)),
    nds                 Decimal(18,4) CODEC(ZSTD(1)),
    akciz               Decimal(18,4) CODEC(ZSTD(1)),

    -- меры из табличных частей: summa/tsena 97.6%, остальные 51% — для детализации
    summa               Decimal(18,4) CODEC(ZSTD(1)),
    tsena               Decimal(18,4) CODEC(ZSTD(1)),
    evrika_bonusy       Decimal(18,4) CODEC(ZSTD(1)),
    evrika_spisannye    Decimal(18,4) CODEC(ZSTD(1)),
    summands            Decimal(18,4) CODEC(ZSTD(1)),

    -- момент последней записи строки в PostgreSQL: аудит загрузки
    etl_updated_at      DateTime      CODEC(Delta, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(period)
ORDER BY (period, podrazdelenie_id, nomenklatura_id, recorder_type, recorder, line_no);

-- Staging для загрузки одного месяца. CREATE ... AS копирует структуру, движок,
-- PARTITION BY и ORDER BY — полное совпадение является условием REPLACE PARTITION.
CREATE TABLE analytics_poc.fact_sales_positions_stage
AS analytics_poc.fact_sales_positions;
