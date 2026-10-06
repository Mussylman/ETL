-- 018 (ClickHouse, ch_admin): реплики справочников и их staging — те же имена, что в PostgreSQL после 018.
-- Одним RENAME: данные и id не трогаются (метаданные Atomic). Права etl_writer привязаны к имени таблицы —
-- выдаются заново под новыми и отзываются со старых. Выполнять сразу после 018_dim_english_names.sql,
-- при приостановленном analytics_sync. Откат — обратный RENAME (018_dim_english_names.down.sql, раздел ClickHouse).
RENAME TABLE
    analytics_poc.dim_nomenklatura        TO analytics_poc.dim_product,
    analytics_poc.dim_nomenklatura_stage  TO analytics_poc.dim_product_stage,
    analytics_poc.dim_sklad               TO analytics_poc.dim_warehouse,
    analytics_poc.dim_sklad_stage         TO analytics_poc.dim_warehouse_stage,
    analytics_poc.dim_podrazdelenie       TO analytics_poc.dim_department,
    analytics_poc.dim_podrazdelenie_stage TO analytics_poc.dim_department_stage,
    analytics_poc.dim_kachestvo           TO analytics_poc.dim_quality,
    analytics_poc.dim_kachestvo_stage     TO analytics_poc.dim_quality_stage,
    analytics_poc.dim_kontragent          TO analytics_poc.dim_counterparty,
    analytics_poc.dim_kontragent_stage    TO analytics_poc.dim_counterparty_stage,
    analytics_poc.dim_dogovor             TO analytics_poc.dim_contract,
    analytics_poc.dim_dogovor_stage       TO analytics_poc.dim_contract_stage,
    analytics_poc.dim_organizatsiya       TO analytics_poc.dim_organization,
    analytics_poc.dim_organizatsiya_stage TO analytics_poc.dim_organization_stage,
    analytics_poc.dim_otvetstvennyy       TO analytics_poc.dim_responsible_person,
    analytics_poc.dim_otvetstvennyy_stage TO analytics_poc.dim_responsible_person_stage;

-- TEMP compatibility: Power BI (10.10.1.136, analytics_reader) читает analytics_poc.dim_nomenklatura по имени.
-- Только для чтения BI, ETL его не использует (справочники ищутся в PostgreSQL и пишутся в dim_product).
-- Удалить, когда модель Power BI переведена на dim_product и query_log подтверждает: обращений к view нет.
CREATE VIEW analytics_poc.dim_nomenklatura
COMMENT 'TEMP compatibility for Power BI: dim_nomenklatura → dim_product (2026-10-06). Drop after BI switched to dim_product.'
AS SELECT * FROM analytics_poc.dim_product;
