---
tags: [audit, postgresql, clickhouse, cleanup, inventory]
date: 2026-10-06
---

# Инвентаризация таблиц PostgreSQL и ClickHouse — кандидатов на безусловное удаление нет

Read-only аудит 2026-10-06 (до переименования справочников: имена `dim_*` ниже — старые, соответствие —
[[Справочники переименованы в английские имена — Power BI читает dim_nomenklatura через временный VIEW]]).
Ничего не удалялось. Итог: SAFE_CANDIDATES — 0 GB; ROLLBACK_CANDIDATES — ~17.1 GB (замороженные PG-факты 15.8,
`fact_*_direct` 0.4, shadow склада 0.9) — только после решения об окне отката.

## Backlog очистки (решения ещё не приняты — ничего не выполнять без подтверждения)
1. ~~Выполнено 2026-10-07~~ (sales — OWNED BY NONE; orders — identity-sequence не отвязывается, переход на `etl_meta.doc_key_orders_seq`; таблицы удалены, см. [[Identity-sequence удаляется вместе с таблицей — orders_id_seq нельзя отвязать OWNED BY NONE]]). Было: **Перед удалением замороженных `public.sales` / `public.orders`** — `ALTER SEQUENCE public.sales_id_seq OWNED BY NONE`
   и `ALTER SEQUENCE public.orders_id_seq OWNED BY NONE`: эти последовательности выдают id документов в `etl_meta.doc_key`
   (doc_key_scope), а DROP TABLE удалил бы их вместе с таблицей — выдача id прямого пути сломалась бы.
2. Отозвать у `etl_writer` права на несуществующие `cost_daily_v2*` (гигиена прав, места не даёт).
3. ~~Разобрано 2026-10-07~~: `is_active = true` — верно, это конфиг прямого пути (Sync PG отключён). Было: `register_targets` замороженных PG-фактов — `is_active = true`:
   разобраться (ETLEngine их не пишет — `pg_fact_write = false`, — но флаг вводит в заблуждение).
4. Rollback/shadow (`fact_*_direct` + их stage, `fact_stock*_shadow` + stage/raw, записи `*:shadow` в
   `ch_source_state`) — не удалять до решения об окончании окна отката.
5. ~~TEMP VIEW `analytics_poc.dim_nomenklatura`~~ — удалён 2026-10-06 11:33 UTC после перевода Power BI на `dim_product` (проверка
   `system.query_log`: обращений к `dim_nomenklatura` больше нет.
6. Вне классификации, решение владельца: БД `bd_retail` на 10.10.1.142 (6.6 GB, обращений нет; Airflow conn
   `bd_retail` указывает на 10.10.1.99) и архивная `test` (1.1 GB).

---
tags: [audit, inventory, postgres, clickhouse]
date: 2026-10-06
---
# Инвентаризация объектов БД — 2026-10-06 (read-only)

Метод: только SELECT/SHOW (PG read-only сессия; CH — system.tables/parts/query_log/grants). Ничего не изменялось.
Источники доказательств: `pg_stat_user_tables` (статистика сброшена **2026-10-02 10:05 UTC** — счётчики = 4 дня), `pg_class.reltuples`,
`system.query_log` CH (окно **2026-09-30 00:00 — 2026-10-06 10:42 UTC**, 386 001 запись), конфиг `etl_meta.*`, grep по `dags/`, `etl_config_app/`, `ops/` (без migrations/tests/docs), метаданные Airflow (DAG'и, dag_run за 7 дней).

## 0. Контекст сервера 10.10.1.142

| БД | размер | статус |
|---|---|---|
| etl_prod | 18 GB (18.94 GB) | PROD control plane + реестры + замороженные факты |
| bd_retail | 6.25 GB | **UNKNOWN**: отдельная БД; Airflow conn `bd_retail` указывает на **10.10.1.99/ims_db**, а не сюда; в `pg_stat_database` нет ни одного обращения (stats_reset = NULL, xact_commit = 0). Нужен владелец/решение |
| test | 1.01 GB | **ARCHIVE_DB** (пассивная, 0 записей с reset 2026-10-05) |
| airflow | 339 MB | метаданные Airflow (не инвентаризировались) |
| postgres/template* | ~25 MB | системные |

ClickHouse: БД `analytics_poc` (51 таблица, все MergeTree, ни одного VIEW/словаря), `default` — пусто. Активных частей 3.22 GB (≈3.0 GiB). **Свободно на диске CH: 9.37 GiB из 97.87 GiB.**

Airflow DAG'и (все не на паузе, запускались за 7 дней): `analytics_sync` (1911 прогонов, единственный, кто трогает etl_prod/CH), `check_orders_dag` (читает `public.orders` на **10.10.1.108/inventory** — conn_inventory, не etl_prod), `gfk_update` (MSSQL 10.10.1.61/forecast), `update_users_and_products` (MSSQL PowerBI 10.10.1.136 — вне scope). Ни один не-core DAG не читает/не пишет etl_prod или ClickHouse.

## 1. Сводка по классам

### PostgreSQL etl_prod (51 объект: 29 таблиц + 22 последовательности, view/matview нет)

| класс | объектов | размер |
|---|---|---|
| CONTROL_PLANE (etl_meta.*) | 27 | 2.04 GiB (из них doc_key 2.0 GiB) |
| ACTIVE_PRODUCTION (public.dim_* + их seq, sales_id_seq, orders_id_seq) | 18 | 884 MiB |
| ROLLBACK_KEEP (sales, sales_positions, orders, order_positions + 2 seq позиций) | 6 | **14.72 GiB (15.81 GB)** |
| прочие классы | 0 | — |
| **итого** | **51** | **17.6 GiB** |

### ClickHouse analytics_poc (51 таблица)

| класс | таблиц | размер |
|---|---|---|
| ACTIVE_PRODUCTION | 15 | 1.80 GiB |
| STAGING_ACTIVE | 22 | 0 (пусты между прогонами) |
| SHADOW_KEEP (fact_stock_shadow, fact_stock_positions_shadow + 4 staging) | 6 | **857 MiB (0.90 GB)** |
| ROLLBACK_KEEP (fact_*_direct) | 4 | **415 MiB (0.43 GB)** |
| STAGING_UNUSED (fact_*_direct_stage) | 4 | 0 |
| LEGACY/ORPHAN/TEMP/POC/UNKNOWN | 0 | — |
| **итого** | **51** | **≈3.0 GiB** |

POC/test-таблиц (`zz_`, `tmp`, `test`, `bak`, `old`) в CH сейчас **нет**: `zz_promote_*` (12 шт., 07:24–07:29) и `zz_gate_test` (09:15–09:17) созданы и удалены ch_admin 2026-10-06 (гейт-тесты ch_promote) — подтверждено query_log.

### PostgreSQL test (ARCHIVE_DB, кратко)
Схемы `etl_meta` (копия конфига, load_history 24 MB), `etl_test`, `etl_test_ref` (пустые наборы control-таблиц — следы тестов конфигуратора), `public`: sales 705 MB / sales_positions 220 MB / dim_nomenklatura 50 MB / прочие dim_* < 1 MB, пустые `order`, `order_positions`, `salesTEST_dim`, `salesTEST_pos`, `stock_positions`, `wt_sales`, `wt_sales_positions` (TEMP/POC, 0 строк). n_tup_ins = 0 по всем. Класс всей БД — ARCHIVE_DB; удаление — решение по архиву, не по коду.

## 2. Полные таблицы

### 2.1 PostgreSQL etl_prod
Код-ссылки: grep по имени (без migrations/tests/docs). Активность — с reset 2026-10-02.

| схема | объект | тип | строк (≈) | размер | активность (seq/idx/ins/upd/del) | vacuum/analyze | конфиг | код | DAG | BI | назначение | КЛАСС |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| etl_meta | ch_source_state | table | 8 | 32.0 KB | seq 1617/idx 542/ins 8/upd 531/del 1 | vac None / an 2026-10-06 10:07 | — | core/tools/ch_cutover.py, core/tools/ch_promote.py, core/clickhouse/onec.py, core/clickhouse/runner.py | analytics_sync | — | watermark/отметки режимов (patch/hot/sweep) | **CONTROL_PLANE** |
| etl_meta | ch_sync | table | 30 (точно) | 160.0 KB | seq 4860/idx 4714/ins 22/upd 26/del 6 | vac None / an None | — | core/tools/ch_cutover.py, dags/analytics_sync_dag.py, core/tools/ch_ddl.py, core/tools/ch_pg_handover.py, core/tools/ch_config.py, core/clickhouse/__init__.py, core/tools/ch_report.py, core/tools/rebuild_sales.py, core/tools/ch_promote.py, core/clickhouse/engine.py, core/clickhouse/runner.py, core/tools/ch_sync.py, core/clickhouse/config.py | analytics_sync | — | конфиг публикаций в CH (30 строк) | **CONTROL_PLANE** |
| etl_meta | ch_sync_columns | table | 559 (точно) | 232.0 KB | seq 8/idx 15142/ins 351/upd 0/del 86 | vac 2026-10-06 07:29 / an 2026-10-06 07:29 | — | core/tools/ch_config.py, core/clickhouse/config.py, core/tools/ch_promote.py, core/clickhouse/__init__.py | analytics_sync | — | колонки публикаций | **CONTROL_PLANE** |
| etl_meta | ch_sync_columns_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | analytics_sync | — | serial для ch_sync_columns | **CONTROL_PLANE** |
| etl_meta | ch_sync_group | table | 8 (точно) | 32.0 KB | seq 1108/idx 90/ins 10/upd 2/del 3 | vac None / an None | — | dags/analytics_sync_dag.py, core/tools/ch_pg_handover.py, core/tools/ch_config.py, core/tools/ch_cutover.py, core/clickhouse/runner.py, core/tools/ch_promote.py | analytics_sync | — | группы оркестрации analytics_sync | **CONTROL_PLANE** |
| etl_meta | ch_sync_history | table | 12,525 | 13.6 MB | seq 3/idx 127/ins 3839/upd 0/del 0 | vac 2026-10-06 04:55 / an 2026-10-05 20:24 | — | dags/analytics_sync_dag.py, core/tools/ch_config.py, core/tools/ch_promote.py, core/clickhouse/runner.py, core/clickhouse/engine.py, core/clickhouse/reconcile.py | analytics_sync | — | история прогонов публикации | **CONTROL_PLANE** |
| etl_meta | ch_sync_history_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | analytics_sync | — | serial для ch_sync_history | **CONTROL_PLANE** |
| etl_meta | ch_sync_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | analytics_sync | — | serial для ch_sync | **CONTROL_PLANE** |
| etl_meta | ch_sync_partition_state | table | 962 | 720.0 KB | seq 0/idx 16934/ins 340/upd 15492/del 0 | vac None / an 2026-10-06 10:07 | — | core/clickhouse/runner.py, core/clickhouse/engine.py | analytics_sync | — | состояние/отпечатки партиций | **CONTROL_PLANE** |
| etl_meta | ch_sync_partition_state_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | analytics_sync | — | serial для ch_sync_partition_state | **CONTROL_PLANE** |
| etl_meta | column_mappings | table | 210 (точно) | 128.0 KB | seq 47281/idx 8/ins 4/upd 0/del 4 | vac None / an None | — | core/config/refs.py, core/config/config_loader.py, core/tools/load_dim_from_config.py, core/tools/ch_config.py, core/clickhouse/onec_reconcile.py, core/clickhouse/onec.py, etl_config_app/spec_writer.py, etl_config_app/spec_reader.py, etl_config_app/dao.py, etl_config_app/register_spec.py | analytics_sync (dim_registry/onec_1c/onec_stock через ConfigLoader); конфигуратор :5556 | — | маппинги колонок 1С | **CONTROL_PLANE** |
| etl_meta | column_mappings_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | analytics_sync | — | serial для column_mappings | **CONTROL_PLANE** |
| etl_meta | doc_key | table | 10,715,830 | 2.0 GB | seq 326/idx 29091928/ins 4792905/upd 7774/del 0 | vac 2026-10-05 18:53 / an 2026-10-05 19:49 | doc_key_scope (FK) | core/tools/ch_pg_handover.py, core/etl_engine.py, core/tools/ch_config.py, core/tools/rebuild_sales.py, core/tools/ch_promote.py, core/clickhouse/registry.py, ops/stock_history_load.py, core/clickhouse/patch.py | analytics_sync | — | реестр id документов (guid→id), строки не удаляются | **CONTROL_PLANE** |
| etl_meta | doc_key_scope | table | 3 (точно) | 32.0 KB | seq 3/idx 4796485/ins 2/upd 0/del 1 | vac None / an None | — | core/clickhouse/onec.py, core/clickhouse/registry.py, core/tools/ch_pg_handover.py, core/tools/ch_cutover.py, core/tools/ch_config.py, core/tools/ch_promote.py | analytics_sync | — | области выдачи id (sales/orders/stock) | **CONTROL_PLANE** |
| etl_meta | doc_key_stock_seq | sequence | 1 | 8.0 KB | — | vac None / an None | doc_key_scope stock.seq | — | analytics_sync | — | выдача id stock (активна, last 4 818 053) | **CONTROL_PLANE** |
| etl_meta | load_history | table | 86,950 | 25.4 MB | seq 2/idx 17913/ins 8768/upd 8768/del 0 | vac None / an 2026-10-06 04:25 | — | core/etl_engine.py, core/tools/set_watermark.py, core/extract/data_checker.py, core/tools/rebuild_sales.py, etl_config_app/spec_writer.py, etl_config_app/dao.py, core/tools/load_dim_from_config.py, core/clickhouse/onec.py | analytics_sync | — | история загрузок dim_registry/ETLEngine | **CONTROL_PLANE** |
| etl_meta | load_history_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | analytics_sync | — | serial для load_history | **CONTROL_PLANE** |
| etl_meta | register_sources | table | 40 (точно) | 144.0 KB | seq 3/idx 33902/ins 1/upd 1/del 1 | vac None / an None | — | core/config/config_loader.py, core/config/refs.py, core/tools/load_dim_from_config.py, core/clickhouse/onec.py, etl_config_app/spec_writer.py, etl_config_app/spec_reader.py, etl_config_app/dao.py, etl_config_app/register_spec.py | analytics_sync (dim_registry/onec_1c/onec_stock через ConfigLoader); конфигуратор :5556 | — | источники регистров | **CONTROL_PLANE** |
| etl_meta | register_sources_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | analytics_sync | — | serial для register_sources | **CONTROL_PLANE** |
| etl_meta | register_targets | table | 14 (точно) | 112.0 KB | seq 20926/idx 15246/ins 2/upd 2/del 2 | vac None / an None | — | core/config/refs.py, core/config/config_loader.py, core/tools/ch_config.py, core/tools/rebuild_sales.py, core/load/loaders.py, etl_config_app/spec_reader.py, etl_config_app/dao.py, etl_config_app/register_spec.py, core/tools/load_dim_from_config.py, core/tools/integrity_manager.py, etl_config_app/spec_writer.py, etl_config_app/app.py | analytics_sync (dim_registry/onec_1c/onec_stock через ConfigLoader); конфигуратор :5556 | — | цели регистров (14) | **CONTROL_PLANE** |
| etl_meta | register_targets_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | analytics_sync | — | serial для register_targets | **CONTROL_PLANE** |
| etl_meta | registers | table | 11 (точно) | 48.0 KB | seq 1101/idx 37149/ins 1/upd 0/del 1 | vac None / an None | — | core/config/config_loader.py, core/etl_engine.py, core/tools/ch_pg_handover.py, core/tools/integrity_manager.py, core/tools/set_watermark.py, core/tools/ch_config.py, core/tools/run_full_period.py, core/clickhouse/runner.py, core/tools/rebuild_sales.py, core/tools/load_dim_from_config.py, etl_config_app/spec_writer.py, core/clickhouse/onec.py, etl_config_app/templates/mappings/form.html, etl_config_app/templates/targets/form.html, etl_config_app/templates/registers/wizard.html, etl_config_app/templates/sources/detail.html, etl_config_app/app.py, etl_config_app/templates/base.html, etl_config_app/templates/registers/list.html, core/config/refs.py, etl_config_app/mssql_client.py, etl_config_app/dao.py, etl_config_app/templates/unions/form.html, core/tools/ch_cutover.py, etl_config_app/templates/unions/detail.html, etl_config_app/spec_reader.py, etl_config_app/templates/registers/wizard_index.html, etl_config_app/templates/registers/form.html, etl_config_app/templates/members/form.html, etl_config_app/templates/registers/detail.html, etl_config_app/templates/sources/form.html | analytics_sync (dim_registry/onec_1c/onec_stock через ConfigLoader); конфигуратор :5556 | — | регистры 1С (11) | **CONTROL_PLANE** |
| etl_meta | registers_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | analytics_sync | — | serial для registers | **CONTROL_PLANE** |
| etl_meta | source_union_members | table | 2 (точно) | 32.0 KB | seq 5232/idx 0/ins 0/upd 0/del 0 | vac None / an None | — | core/config/refs.py, etl_config_app/dao.py, etl_config_app/spec_writer.py, etl_config_app/spec_reader.py, etl_config_app/register_spec.py, core/config/config_loader.py | analytics_sync (dim_registry/onec_1c/onec_stock через ConfigLoader); конфигуратор :5556 | — | члены union | **CONTROL_PLANE** |
| etl_meta | source_union_members_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | analytics_sync | — | serial для source_union_members | **CONTROL_PLANE** |
| etl_meta | source_unions | table | 1 (точно) | 32.0 KB | seq 3181/idx 6/ins 0/upd 0/del 0 | vac None / an None | — | core/config/config_loader.py, etl_config_app/spec_writer.py, etl_config_app/dao.py, etl_config_app/spec_reader.py, etl_config_app/register_spec.py | analytics_sync (dim_registry/onec_1c/onec_stock через ConfigLoader); конфигуратор :5556 | — | union-источники | **CONTROL_PLANE** |
| etl_meta | source_unions_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | analytics_sync | — | serial для source_unions | **CONTROL_PLANE** |
| public | dim_dogovor | table | 548,677 | 102.3 MB | seq 3303/idx 1927208/ins 14/upd 14/del 0 | vac None / an None | registers/register_targets dim_dogovor (reference_dim, active); ch_sync dim_dogovor (источник core_pg_to_ch) | core/tools/load_dim_names.py | analytics_sync | — | реестр справочника guid→id + атрибуты | **ACTIVE_PRODUCTION** |
| public | dim_dogovor_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | registers/register_targets dim_dogovor (reference_dim, active); ch_sync dim_dogovor (источник core_pg_to_ch) | — | analytics_sync | — | выдача id dim_dogovor (OWNED BY dim_dogovor) | **ACTIVE_PRODUCTION** |
| public | dim_kachestvo | table | 7 | 64.0 KB | seq 5153/idx 7166/ins 0/upd 0/del 0 | vac None / an None | registers/register_targets dim_kachestvo (reference_dim, active); ch_sync dim_kachestvo (источник core_pg_to_ch) | core/tools/load_dim_names.py, core/tools/set_dim_retail_marks.py, ops/stock_history_load.py | analytics_sync | — | реестр справочника guid→id + атрибуты | **ACTIVE_PRODUCTION** |
| public | dim_kachestvo_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | registers/register_targets dim_kachestvo (reference_dim, active); ch_sync dim_kachestvo (источник core_pg_to_ch) | — | analytics_sync | — | выдача id dim_kachestvo (OWNED BY dim_kachestvo) | **ACTIVE_PRODUCTION** |
| public | dim_kontragent | table | 1,428,518 | 660.9 MB | seq 3307/idx 2098226/ins 12/upd 24/del 0 | vac None / an None | registers/register_targets dim_kontragent (reference_dim, active); ch_sync dim_kontragent (источник core_pg_to_ch); transform_params {"dim":"dim_kontragent"} | core/tools/load_dim_names.py, core/tools/integrity_manager.py, etl_config_app/dao.py, ops/stock_history_load.py | analytics_sync | — | реестр справочника guid→id + атрибуты | **ACTIVE_PRODUCTION** |
| public | dim_kontragent_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | registers/register_targets dim_kontragent (reference_dim, active); ch_sync dim_kontragent (источник core_pg_to_ch); transform_params {"dim":"dim_kontragent"} | — | analytics_sync | — | выдача id dim_kontragent (OWNED BY dim_kontragent) | **ACTIVE_PRODUCTION** |
| public | dim_nomenklatura | table | 153,169 | 116.9 MB | seq 4519/idx 19292081/ins 42/upd 4320/del 0 | vac None / an None | registers/register_targets dim_nomenklatura (reference_dim, active); ch_sync dim_nomenklatura (источник core_pg_to_ch) | core/tools/load_products_from_retail.py, core/tools/set_dim_retail_marks.py, core/tools/load_dim_names.py, ops/stock_history_load.py | analytics_sync | косвенно: реплика в CH (dim_nomenklatura → PowerBI) | реестр справочника guid→id + атрибуты | **ACTIVE_PRODUCTION** |
| public | dim_nomenklatura_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | registers/register_targets dim_nomenklatura (reference_dim, active); ch_sync dim_nomenklatura (источник core_pg_to_ch) | — | analytics_sync | косвенно: реплика в CH (dim_nomenklatura → PowerBI) | выдача id dim_nomenklatura (OWNED BY dim_nomenklatura) | **ACTIVE_PRODUCTION** |
| public | dim_organizatsiya | table | 1 | 56.0 KB | seq 3356/idx 1164/ins 0/upd 0/del 0 | vac None / an None | registers/register_targets dim_organizatsiya (reference_dim, active); ch_sync dim_organizatsiya (источник core_pg_to_ch) | core/tools/load_dim_names.py | analytics_sync | — | реестр справочника guid→id + атрибуты | **ACTIVE_PRODUCTION** |
| public | dim_organizatsiya_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | registers/register_targets dim_organizatsiya (reference_dim, active); ch_sync dim_organizatsiya (источник core_pg_to_ch) | — | analytics_sync | — | выдача id dim_organizatsiya (OWNED BY dim_organizatsiya) | **ACTIVE_PRODUCTION** |
| public | dim_otvetstvennyy | table | 5,962 | 3.6 MB | seq 2288/idx 912482/ins 0/upd 0/del 0 | vac None / an None | registers/register_targets dim_otvetstvennyy (reference_dim, active); ch_sync dim_otvetstvennyy (источник core_pg_to_ch) | core/tools/load_dim_names.py | analytics_sync | — | реестр справочника guid→id + атрибуты | **ACTIVE_PRODUCTION** |
| public | dim_otvetstvennyy_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | registers/register_targets dim_otvetstvennyy (reference_dim, active); ch_sync dim_otvetstvennyy (источник core_pg_to_ch) | — | analytics_sync | — | выдача id dim_otvetstvennyy (OWNED BY dim_otvetstvennyy) | **ACTIVE_PRODUCTION** |
| public | dim_podrazdelenie | table | 267 | 264.0 KB | seq 2229/idx 99606/ins 0/upd 1040/del 0 | vac 2026-10-06 01:05 / an 2026-10-06 01:05 | registers/register_targets dim_podrazdelenie (reference_dim, active); ch_sync dim_podrazdelenie (источник core_pg_to_ch) | core/tools/set_dim_retail_marks.py, core/tools/load_dim_names.py | analytics_sync | — | реестр справочника guid→id + атрибуты | **ACTIVE_PRODUCTION** |
| public | dim_podrazdelenie_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | registers/register_targets dim_podrazdelenie (reference_dim, active); ch_sync dim_podrazdelenie (источник core_pg_to_ch) | — | analytics_sync | — | выдача id dim_podrazdelenie (OWNED BY dim_podrazdelenie) | **ACTIVE_PRODUCTION** |
| public | dim_sklad | table | 208 | 264.0 KB | seq 3655/idx 150003/ins 0/upd 0/del 0 | vac None / an None | registers/register_targets dim_sklad (reference_dim, active); ch_sync dim_sklad (источник core_pg_to_ch) | core/tools/ch_ddl.py, core/tools/set_dim_retail_marks.py, core/tools/load_dim_from_config.py, ops/stock_history_load.py, core/tools/load_dim_names.py | analytics_sync | — | реестр справочника guid→id + атрибуты | **ACTIVE_PRODUCTION** |
| public | dim_sklad_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | registers/register_targets dim_sklad (reference_dim, active); ch_sync dim_sklad (источник core_pg_to_ch) | — | analytics_sync | — | выдача id dim_sklad (OWNED BY dim_sklad) | **ACTIVE_PRODUCTION** |
| public | order_positions | table | 640,492 | 592.7 MB | seq 0/idx 0/ins 0/upd 0/del 0 | vac None / an None | register_targets public.order_positions is_active=true (регистр order), запись запрещена ETLEngine | etl_config_app/app.py, etl_config_app/dao.py | — (analytics_sync не читает/не пишет; pg_fact_write=false) | — | факт PG заморожен 2026-09-24 (ROLLBACK_KEEP); 0 чтений/записей с reset статистики 2026-10-02 | **ROLLBACK_KEEP** |
| public | order_positions_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | — | — | serial замороженной order_positions (OWNED BY) | **ROLLBACK_KEEP** |
| public | orders | table | 461,153 | 926.7 MB | seq 0/idx 0/ins 0/upd 0/del 0 | vac None / an None | register_targets public.orders is_active=true (регистр order), запись запрещена ETLEngine; transform_params ref_target "orders" | dags/orders_check_dag.py, core/etl_engine.py, core/tools/integrity_manager.py, etl_config_app/templates/targets/form.html, core/tools/ch_cutover.py, etl_config_app/dao.py | — (analytics_sync не читает/не пишет; pg_fact_write=false) | — | факт PG заморожен 2026-09-24 (ROLLBACK_KEEP); 0 чтений/записей с reset статистики 2026-10-02 | **ROLLBACK_KEEP** |
| public | orders_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | **doc_key_scope orders.seq = public.orders_id_seq** (issuer registry) | — | analytics_sync | — | **активная выдача id документов orders в doc_key**; OWNED BY public.orders — DROP TABLE orders удалит последовательность | **ACTIVE_PRODUCTION** |
| public | sales | table | 6,026,582 | 8.0 GB | seq 0/idx 0/ins 0/upd 0/del 0 | vac None / an None | register_targets public.sales is_active=true (регистр sales), запись запрещена ETLEngine; transform_params ref_target "sales" | dags/plugins/gfk_client.py, dags/gfk_report.py, core/tools/ch_pg_handover.py, dags/plugins/gfk_reload.py, core/etl_engine.py, core/etl_core.py, core/tools/run_full_period.py, core/sales_etl.py, core/extract/data_checker.py, core/tools/rebuild_sales.py, core/tools/integrity_manager.py, core/tools/set_watermark.py, etl_config_app/templates/targets/form.html, etl_config_app/templates/registers/wizard.html, etl_config_app/templates/registers/form.html, etl_config_app/app.py, core/tools/ch_cutover.py, core/extract/storage_connector.py, etl_config_app/dao.py, etl_config_app/templates/registers/detail.html | — (analytics_sync не читает/не пишет; pg_fact_write=false) | — | факт PG заморожен 2026-09-24 (ROLLBACK_KEEP); 0 чтений/записей с reset статистики 2026-10-02 | **ROLLBACK_KEEP** |
| public | sales_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | **doc_key_scope sales.seq = public.sales_id_seq** (issuer registry) | — | analytics_sync | — | **активная выдача id документов sales в doc_key**; OWNED BY public.sales — DROP TABLE sales удалит последовательность | **ACTIVE_PRODUCTION** |
| public | sales_positions | table | 10,424,222 | 5.2 GB | seq 0/idx 0/ins 0/upd 0/del 0 | vac None / an None | register_targets public.sales_positions is_active=true (регистр sales), запись запрещена ETLEngine | core/sales_etl.py, core/tools/load_products_from_retail.py, core/load/loaders.py, core/tools/rebuild_sales.py, etl_config_app/templates/targets/form.html, etl_config_app/app.py, etl_config_app/dao.py, etl_config_app/templates/registers/form.html, etl_config_app/templates/registers/detail.html | — (analytics_sync не читает/не пишет; pg_fact_write=false) | — | факт PG заморожен 2026-09-24 (ROLLBACK_KEEP); 0 чтений/записей с reset статистики 2026-10-02 | **ROLLBACK_KEEP** |
| public | sales_positions_id_seq | sequence | 1 | 8.0 KB | — | vac None / an None | — | — | — | — | serial замороженной sales_positions (OWNED BY) | **ROLLBACK_KEEP** |

### 2.2 ClickHouse analytics_poc
Имена `_stage`/`_raw` в коде не зашиты — выводятся из `ch_sync.target_table` (`core/clickhouse/config.py: stage_fqn/raw_fqn`; `_raw` — для `document_patch` и lookup). Поэтому «код» для витрин = runner/engine/patch через конфиг. Запись/чтение — query_log (QueryFinish, окно 09-30…10-06).

| таблица | engine | строк | размер | изменения (metadata; max part) | конфиг | код (по имени) | DAG | кто пишет/читает из ETL (query_log) | внешнее/BI | назначение | КЛАСС |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cost_daily | MergeTree | 52,506,042 | 460.1 MB | meta 2026-09-23 12:51:24; parts 2026-10-06 01:04:53 | ch_sync cost_daily (retail, partitioned, lookup→dim_nomenklatura) | core/tools/ch_pg_handover.py, core/clickhouse/runner.py | analytics_sync | etl_writer 12261 | **PowerBI (10.10.1.136) ODBC: 8247** | витрина | **ACTIVE_PRODUCTION** |
| cost_daily_raw | MergeTree | 0 | 0 B | meta 2026-09-23 12:55:07; parts нет частей | ch_sync cost_daily (retail, partitioned, lookup→dim_nomenklatura) → _raw | — | analytics_sync | etl_writer 80 + async flush 19 | — | staging_raw активной конфигурации | **STAGING_ACTIVE** |
| cost_daily_stage | MergeTree | 0 | 0 B | meta 2026-09-23 12:55:07; parts нет частей | ch_sync cost_daily (retail, partitioned, lookup→dim_nomenklatura) → _stage | — | analytics_sync | etl_writer 154 | — | staging_stage активной конфигурации | **STAGING_ACTIVE** |
| dim_dogovor | MergeTree | 548,823 | 8.9 MB | meta 2026-09-23 10:48:14; parts 2026-10-06 10:15:24 | ch_sync dim_dogovor (core_pg_to_ch, full) | core/tools/load_dim_names.py | analytics_sync | etl_writer 7954 | — | реплика справочника PG dim_* | **ACTIVE_PRODUCTION** |
| dim_dogovor_stage | MergeTree | 0 | 0 B | meta 2026-09-23 10:48:15; parts нет частей | ch_sync dim_dogovor (core_pg_to_ch, full) → _stage | — | analytics_sync | etl_writer 209 | — | staging_stage активной конфигурации | **STAGING_ACTIVE** |
| dim_kachestvo | MergeTree | 7 | 1.3 KB | meta 2026-09-23 10:48:02; parts 2026-09-23 10:52:18 | ch_sync dim_kachestvo (core_pg_to_ch, full) | core/tools/load_dim_names.py, core/tools/set_dim_retail_marks.py, ops/stock_history_load.py | analytics_sync | etl_writer 9710 | — | реплика справочника PG dim_* | **ACTIVE_PRODUCTION** |
| dim_kachestvo_stage | MergeTree | 0 | 0 B | meta 2026-09-23 10:48:03; parts нет частей | ch_sync dim_kachestvo (core_pg_to_ch, full) → _stage | — | analytics_sync | — | — | staging_stage активной конфигурации (в окне лога не трогался: full-синк без изменений) | **STAGING_ACTIVE** |
| dim_kontragent | MergeTree | 1,428,618 | 35.0 MB | meta 2026-09-23 10:48:08; parts 2026-10-06 10:16:31 | ch_sync dim_kontragent (core_pg_to_ch, full) | core/tools/load_dim_names.py, core/tools/integrity_manager.py, etl_config_app/dao.py, ops/stock_history_load.py | analytics_sync | etl_writer 9749 | — | реплика справочника PG dim_* | **ACTIVE_PRODUCTION** |
| dim_kontragent_stage | MergeTree | 0 | 0 B | meta 2026-09-23 10:48:08; parts нет частей | ch_sync dim_kontragent (core_pg_to_ch, full) → _stage | — | analytics_sync | etl_writer 171 | — | staging_stage активной конфигурации | **STAGING_ACTIVE** |
| dim_nomenklatura | MergeTree | 132,968 | 3.7 MB | meta 2026-09-23 10:47:45; parts 2026-10-06 10:40:29 | ch_sync dim_nomenklatura (core_pg_to_ch, full) + lookup cost_daily | core/tools/load_products_from_retail.py, core/tools/set_dim_retail_marks.py, core/tools/load_dim_names.py, ops/stock_history_load.py | analytics_sync | etl_writer 10533 | **PowerBI ODBC: 6332** (JOIN с cost_daily) | реплика справочника PG dim_* | **ACTIVE_PRODUCTION** |
| dim_nomenklatura_stage | MergeTree | 0 | 0 B | meta 2026-09-23 10:47:45; parts нет частей | ch_sync dim_nomenklatura (core_pg_to_ch, full) + lookup cost_daily → _stage | — | analytics_sync | etl_writer 419 | — | staging_stage активной конфигурации | **STAGING_ACTIVE** |
| dim_organizatsiya | MergeTree | 1 | 1011 B | meta 2026-09-23 10:48:20; parts 2026-09-23 10:52:26 | ch_sync dim_organizatsiya (core_pg_to_ch, full) | core/tools/load_dim_names.py | analytics_sync | etl_writer 7902 | — | реплика справочника PG dim_* | **ACTIVE_PRODUCTION** |
| dim_organizatsiya_stage | MergeTree | 0 | 0 B | meta 2026-09-23 10:48:20; parts нет частей | ch_sync dim_organizatsiya (core_pg_to_ch, full) → _stage | — | analytics_sync | — | — | staging_stage активной конфигурации (в окне лога не трогался: full-синк без изменений) | **STAGING_ACTIVE** |
| dim_otvetstvennyy | MergeTree | 5,968 | 195.0 KB | meta 2026-09-23 10:48:27; parts 2026-09-25 14:25:20 | ch_sync dim_otvetstvennyy (core_pg_to_ch, full) | core/tools/load_dim_names.py | analytics_sync | etl_writer 14764 | — | реплика справочника PG dim_* | **ACTIVE_PRODUCTION** |
| dim_otvetstvennyy_stage | MergeTree | 0 | 0 B | meta 2026-09-23 10:48:27; parts нет частей | ch_sync dim_otvetstvennyy (core_pg_to_ch, full) → _stage | — | analytics_sync | — | — | staging_stage активной конфигурации (в окне лога не трогался: full-синк без изменений) | **STAGING_ACTIVE** |
| dim_podrazdelenie | MergeTree | 267 | 9.2 KB | meta 2026-09-23 10:47:57; parts 2026-09-23 10:52:44 | ch_sync dim_podrazdelenie (core_pg_to_ch, full) | core/tools/set_dim_retail_marks.py, core/tools/load_dim_names.py | analytics_sync | etl_writer 7902 | — | реплика справочника PG dim_* | **ACTIVE_PRODUCTION** |
| dim_podrazdelenie_stage | MergeTree | 0 | 0 B | meta 2026-09-23 10:47:57; parts нет частей | ch_sync dim_podrazdelenie (core_pg_to_ch, full) → _stage | — | analytics_sync | — | — | staging_stage активной конфигурации (в окне лога не трогался: full-синк без изменений) | **STAGING_ACTIVE** |
| dim_sklad | MergeTree | 208 | 7.8 KB | meta 2026-09-23 10:47:51; parts 2026-09-23 10:52:35 | ch_sync dim_sklad (core_pg_to_ch, full) | core/tools/ch_ddl.py, core/tools/set_dim_retail_marks.py, core/tools/load_dim_from_config.py, ops/stock_history_load.py, core/tools/load_dim_names.py | analytics_sync | etl_writer 15862 | — | реплика справочника PG dim_* | **ACTIVE_PRODUCTION** |
| dim_sklad_stage | MergeTree | 0 | 0 B | meta 2026-09-23 10:47:51; parts нет частей | ch_sync dim_sklad (core_pg_to_ch, full) → _stage | — | analytics_sync | — | — | staging_stage активной конфигурации (в окне лога не трогался: full-синк без изменений) | **STAGING_ACTIVE** |
| fact_order_positions | MergeTree | 684,719 | 13.9 MB | meta 2026-09-24 08:03:00; parts 2026-10-06 10:45:42 | ch_sync code fact_order_positions_direct (onec_1c) | — | analytics_sync | etl_writer 12968 | — | боевая витрина фактов (прямой путь 1С→CH) | **ACTIVE_PRODUCTION** |
| fact_order_positions_direct | MergeTree | 652,174 | 13.2 MB | meta 2026-09-23 10:56:48; parts 2026-09-24 12:08:11 | ch_sync code fact_order_positions → fact_order_positions_direct, is_active=false, группа legacy_frozen (нет в ch_sync_group) | — | — | — | — | копия старого второго hop PG→CH, заморожена cutover 2026-09-24 (EXCHANGE TABLES) | **ROLLBACK_KEEP** |
| fact_order_positions_direct_stage | MergeTree | 0 | 0 B | meta 2026-09-23 10:56:48; parts нет частей | stage неактивной ch_sync fact_order_positions (legacy_frozen) | — | — | — | — | staging замороженной legacy-конфигурации, пуст | **STAGING_UNUSED** |
| fact_order_positions_raw | MergeTree | 0 | 0 B | meta 2026-09-24 12:14:49; parts нет частей | ch_sync code fact_order_positions_direct (onec_1c) → _raw | — | analytics_sync | etl_writer 5574 + async 766 | — | staging_raw активной конфигурации | **STAGING_ACTIVE** |
| fact_order_positions_stage | MergeTree | 0 | 0 B | meta 2026-09-24 08:03:00; parts нет частей | ch_sync code fact_order_positions_direct (onec_1c) → _stage | — | analytics_sync | etl_writer 10353 | — | staging_stage активной конфигурации | **STAGING_ACTIVE** |
| fact_orders | MergeTree | 486,631 | 11.7 MB | meta 2026-09-24 08:02:54; parts 2026-10-06 10:45:35 | ch_sync code fact_orders_direct (onec_1c) | — | analytics_sync | etl_writer 18876 | — | боевая витрина фактов (прямой путь 1С→CH) | **ACTIVE_PRODUCTION** |
| fact_orders_direct | MergeTree | 462,589 | 10.5 MB | meta 2026-09-23 10:56:42; parts 2026-09-24 12:07:50 | ch_sync code fact_orders → fact_orders_direct, is_active=false, группа legacy_frozen (нет в ch_sync_group) | — | — | — | — | копия старого второго hop PG→CH, заморожена cutover 2026-09-24 (EXCHANGE TABLES) | **ROLLBACK_KEEP** |
| fact_orders_direct_stage | MergeTree | 0 | 0 B | meta 2026-09-23 10:56:42; parts нет частей | stage неактивной ch_sync fact_orders (legacy_frozen) | — | — | — | — | staging замороженной legacy-конфигурации, пуст | **STAGING_UNUSED** |
| fact_orders_raw | MergeTree | 0 | 0 B | meta 2026-09-24 12:14:48; parts нет частей | ch_sync code fact_orders_direct (onec_1c) → _raw | — | analytics_sync | etl_writer 12082 + async 917 | — | staging_raw активной конфигурации | **STAGING_ACTIVE** |
| fact_orders_stage | MergeTree | 0 | 0 B | meta 2026-09-24 08:02:54; parts нет частей | ch_sync code fact_orders_direct (onec_1c) → _stage | — | analytics_sync | etl_writer 23575 | — | staging_stage активной конфигурации | **STAGING_ACTIVE** |
| fact_sales | MergeTree | 6,063,471 | 122.3 MB | meta 2026-09-24 07:14:05; parts 2026-10-06 10:46:03 | ch_sync code fact_sales_direct (onec_1c) | — | analytics_sync | etl_writer 4909 | DataGrip ad-hoc 12 (192.168.18.233) | боевая витрина фактов (прямой путь 1С→CH) | **ACTIVE_PRODUCTION** |
| fact_sales_direct | MergeTree | 6,042,653 | 110.3 MB | meta 2026-09-23 10:56:36; parts 2026-09-24 12:07:34 | ch_sync code fact_sales → fact_sales_direct, is_active=false, группа legacy_frozen (нет в ch_sync_group) | — | — | — | DataGrip 2 (SELECT LIMIT 10 + SHOW CREATE) | копия старого второго hop PG→CH, заморожена cutover 2026-09-24 (EXCHANGE TABLES) | **ROLLBACK_KEEP** |
| fact_sales_direct_stage | MergeTree | 0 | 0 B | meta 2026-09-23 10:56:37; parts нет частей | stage неактивной ch_sync fact_sales (legacy_frozen) | — | — | — | — | staging замороженной legacy-конфигурации, пуст | **STAGING_UNUSED** |
| fact_sales_positions | MergeTree | 10,473,790 | 281.8 MB | meta 2026-09-24 07:14:12; parts 2026-10-06 10:46:12 | ch_sync code fact_sales_positions_direct (onec_1c) | core/tools/ch_sync.py, core/tools/ch_shadow_compare.py | analytics_sync | etl_writer 3108 | DataGrip 2 | боевая витрина фактов (прямой путь 1С→CH) | **ACTIVE_PRODUCTION** |
| fact_sales_positions_direct | MergeTree | 10,446,128 | 280.7 MB | meta 2026-09-22 12:13:56; parts 2026-09-24 12:06:04 | ch_sync code fact_sales_positions → fact_sales_positions_direct, is_active=false, группа legacy_frozen (нет в ch_sync_group) | core/tools/ch_shadow_compare.py | — | — | — | копия старого второго hop PG→CH, заморожена cutover 2026-09-24 (EXCHANGE TABLES) | **ROLLBACK_KEEP** |
| fact_sales_positions_direct_stage | MergeTree | 0 | 0 B | meta 2026-09-22 12:13:56; parts нет частей | stage неактивной ch_sync fact_sales_positions (legacy_frozen) | — | — | — | — | staging замороженной legacy-конфигурации, пуст | **STAGING_UNUSED** |
| fact_sales_positions_raw | MergeTree | 0 | 0 B | meta 2026-09-24 12:16:42; parts нет частей | ch_sync code fact_sales_positions_direct (onec_1c) → _raw | — | analytics_sync | etl_writer 2528 + async 352 | — | staging_raw активной конфигурации | **STAGING_ACTIVE** |
| fact_sales_positions_stage | MergeTree | 0 | 0 B | meta 2026-09-24 07:14:12; parts нет частей | ch_sync code fact_sales_positions_direct (onec_1c) → _stage | — | analytics_sync | etl_writer 3588 | — | staging_stage активной конфигурации | **STAGING_ACTIVE** |
| fact_sales_raw | MergeTree | 0 | 0 B | meta 2026-09-24 12:16:42; parts нет частей | ch_sync code fact_sales_direct (onec_1c) → _raw | — | analytics_sync | etl_writer 2528 + async 392 | — | staging_raw активной конфигурации | **STAGING_ACTIVE** |
| fact_sales_stage | MergeTree | 0 | 0 B | meta 2026-09-24 07:14:05; parts нет частей | ch_sync code fact_sales_direct (onec_1c) → _stage | — | analytics_sync | etl_writer 4066 | — | staging_stage активной конфигурации | **STAGING_ACTIVE** |
| fact_stock | MergeTree | 4,818,050 | 75.0 MB | meta 2026-10-06 10:00:23; parts 2026-10-06 10:41:52 | ch_sync fact_stock (onec_stock) | — | analytics_sync | etl_writer 151 (с 10-06 10:01); ch_admin promote | — | боевая витрина фактов (прямой путь 1С→CH) | **ACTIVE_PRODUCTION** |
| fact_stock_positions | MergeTree | 25,471,233 | 782.0 MB | meta 2026-10-06 10:00:29; parts 2026-10-06 10:42:05 | ch_sync fact_stock_positions (onec_stock) | — | analytics_sync | etl_writer 493; ch_admin promote | — | боевая витрина фактов (прямой путь 1С→CH) | **ACTIVE_PRODUCTION** |
| fact_stock_positions_raw | MergeTree | 0 | 0 B | meta 2026-10-06 10:00:30; parts нет частей | ch_sync fact_stock_positions (onec_stock) → _raw | — | analytics_sync | etl_writer 54 | — | staging_raw активной конфигурации | **STAGING_ACTIVE** |
| fact_stock_positions_shadow | MergeTree | 25,470,411 | 781.9 MB | meta 2026-10-01 07:42:44; parts 2026-10-06 09:46:53 | ch_sync fact_stock_positions_shadow (shadow_stock, is_active=false; группа выключена) | ops/stock_history_load.py | — | etl_writer 6012 (до 10-06 10:00); ch_admin 195 | DataGrip 2 (LIMIT 10 10-06) | shadow склада = точка отката после ch_promote 2026-10-06 | **SHADOW_KEEP** |
| fact_stock_positions_shadow_raw | MergeTree | 0 | 0 B | meta 2026-10-01 07:42:44; parts нет частей | ch_sync fact_stock_positions_shadow (shadow_stock, is_active=false; группа выключена) | — | — | etl_writer 2331 (до 10-06 09:46) | — | staging_raw shadow-конфигурации, пуст | **SHADOW_KEEP** |
| fact_stock_positions_shadow_stage | MergeTree | 0 | 0 B | meta 2026-10-01 07:42:44; parts нет частей | ch_sync fact_stock_positions_shadow (shadow_stock, is_active=false; группа выключена) | — | — | etl_writer 4337 (до 10-06 09:46) | — | staging_stage shadow-конфигурации, пуст | **SHADOW_KEEP** |
| fact_stock_positions_stage | MergeTree | 0 | 0 B | meta 2026-10-06 10:00:30; parts нет частей | ch_sync fact_stock_positions (onec_stock) → _stage | — | analytics_sync | etl_writer 128 | — | staging_stage активной конфигурации | **STAGING_ACTIVE** |
| fact_stock_raw | MergeTree | 0 | 0 B | meta 2026-10-06 10:00:24; parts нет частей | ch_sync fact_stock (onec_stock) → _raw | — | analytics_sync | etl_writer 54 | — | staging_raw активной конфигурации | **STAGING_ACTIVE** |
| fact_stock_shadow | MergeTree | 4,817,963 | 75.0 MB | meta 2026-10-01 07:42:37; parts 2026-10-06 09:46:40 | ch_sync fact_stock_shadow (shadow_stock, is_active=false; группа выключена) | ops/stock_history_load.py | — | etl_writer 3790 (до 10-06 09:55); ch_admin promote-источник 10-06 10:00 | — | shadow склада = точка отката после ch_promote 2026-10-06 | **SHADOW_KEEP** |
| fact_stock_shadow_raw | MergeTree | 0 | 0 B | meta 2026-10-01 07:42:38; parts нет частей | ch_sync fact_stock_shadow (shadow_stock, is_active=false; группа выключена) | — | — | etl_writer 2334 (до 10-06 09:46) | — | staging_raw shadow-конфигурации, пуст | **SHADOW_KEEP** |
| fact_stock_shadow_stage | MergeTree | 0 | 0 B | meta 2026-10-01 07:42:38; parts нет частей | ch_sync fact_stock_shadow (shadow_stock, is_active=false; группа выключена) | — | — | etl_writer 3549 (до 10-06 09:46) | — | staging_stage shadow-конфигурации, пуст | **SHADOW_KEEP** |
| fact_stock_stage | MergeTree | 0 | 0 B | meta 2026-10-06 10:00:24; parts нет частей | ch_sync fact_stock (onec_stock) → _stage | — | analytics_sync | etl_writer 106 | — | staging_stage активной конфигурации | **STAGING_ACTIVE** |

Пояснения к CH:
- После cutover 2026-09-24 `ch_cutover` сделал EXCHANGE TABLES `fact_X ↔ fact_X_direct`: боевые `fact_sales/…` пишутся конфигами с code `fact_*_direct` (onec_1c), а таблицы `fact_*_direct` содержат старый второй hop PG→CH и принадлежат неактивным конфигам code `fact_*` (sync_group `legacy_frozen`, такой группы нет в `ch_sync_group`). Последнее изменение частей `fact_*_direct` — 2026-09-24 12:06–12:08 UTC (= момент заморозки). etl_writer в окне лога их не трогал, но **сохраняет `ALTER DELETE` на fact_*_direct и MOVE/TRUNCATE на fact_*_direct_stage** (риск записи в точку отката).
- `fact_stock_shadow*`: писались etl_writer до 2026-10-06 09:55, с 10:00 ch_admin перенёс партиции в боевые `fact_stock*` (ch_promote, REPLACE PARTITION FROM); группа `shadow_stock` выключена, конфиги shadow оставлены как точка отката. Строки shadow ≈ боевым (4 817 963 vs 4 818 050; 25 470 411 vs 25 471 233).
- `dim_*_stage` для kachestvo/organizatsiya/otvetstvennyy/podrazdelenie/sklad в окне лога etl_writer не использовал (full-синк без изменений не грузит stage), но конфиг активен → STAGING_ACTIVE.
- Висячие гранты etl_writer на несуществующие `cost_daily_v2`, `cost_daily_v2_raw`, `cost_daily_v2_stage` — следы удалённого POC (гигиена прав, не объёма).

## 3. Кандидаты: неиспользуемые / legacy

| таблица | БД | размер | класс | почему не используется | safe_to_delete |
|---|---|---|---|---|---|
| public.sales | PG etl_prod | 8.04 GiB | ROLLBACK_KEEP | заморожена 2026-09-24; 0 seq/idx scan и 0 ins/upd/del с 2026-10-02; analytics_sync не читает; check_orders читает другую БД | ROLLBACK_CANDIDATE (**сначала `ALTER SEQUENCE public.sales_id_seq OWNED BY NONE`** — seq активно выдаёт id в doc_key) |
| public.sales_positions | PG etl_prod | 5.20 GiB | ROLLBACK_KEEP | то же | ROLLBACK_CANDIDATE |
| public.orders | PG etl_prod | 927 MiB | ROLLBACK_KEEP | то же | ROLLBACK_CANDIDATE (**сначала `orders_id_seq OWNED BY NONE`**) |
| public.order_positions | PG etl_prod | 593 MiB | ROLLBACK_KEEP | то же | ROLLBACK_CANDIDATE |
| public.sales_positions_id_seq, order_positions_id_seq | PG etl_prod | 16 KB | ROLLBACK_KEEP | serial замороженных таблиц, нигде не выдаются | ROLLBACK_CANDIDATE (уйдут вместе с таблицами) |
| fact_stock_positions_shadow | CH | 782 MiB | SHADOW_KEEP | группа shadow_stock выключена 2026-10-06; читали только DataGrip ad-hoc | ROLLBACK_CANDIDATE (после окна отката склада) |
| fact_stock_shadow | CH | 75 MiB | SHADOW_KEEP | то же | ROLLBACK_CANDIDATE |
| fact_stock_shadow_stage/_raw, fact_stock_positions_shadow_stage/_raw | CH | 0 | SHADOW_KEEP | staging выключенной shadow-конфигурации | ROLLBACK_CANDIDATE (вместе с shadow) |
| fact_sales_positions_direct | CH | 281 MiB | ROLLBACK_KEEP | неактивный legacy_frozen, без записи с 2026-09-24; разово SHOW CREATE в DataGrip | ROLLBACK_CANDIDATE |
| fact_sales_direct | CH | 110 MiB | ROLLBACK_KEEP | то же; DataGrip SELECT LIMIT 10 (2026-10-05) | ROLLBACK_CANDIDATE |
| fact_order_positions_direct | CH | 13 MiB | ROLLBACK_KEEP | то же | ROLLBACK_CANDIDATE |
| fact_orders_direct | CH | 10 MiB | ROLLBACK_KEEP | то же | ROLLBACK_CANDIDATE |
| fact_*_direct_stage (4) | CH | 0 | STAGING_UNUSED | stage неактивных legacy-конфигов, пусты | ROLLBACK_CANDIDATE (освобождают 0 байт; удалять вместе с fact_*_direct и конфигами legacy_frozen) |
| гранты etl_writer на cost_daily_v2* | CH | — | — | таблиц нет | SAFE_CANDIDATE (REVOKE, гигиена) |
| БД bd_retail на 10.10.1.142 | PG | 6.25 GiB | UNKNOWN | нет обращений; conn bd_retail смотрит на 10.10.1.99 | требуется решение владельца (не удалять по этому аудиту) |
| БД test | PG | 1.01 GB | ARCHIVE_DB | пассивный архив, тесты конфигуратора запускаются вручную на ней | требуется решение (CLAUDE.md: тесты конфигуратора — только на test) |

**SAFE_CANDIDATE среди таблиц нет**: каждый неактивный объект либо точка отката, либо архив, либо его назначение не доказано.

## 4. Потенциальное освобождение места

| категория | ГБ |
|---|---|
| SAFE_CANDIDATE (таблицы) | **0 GB** |
| ROLLBACK_CANDIDATE PG (4 факта) | **15.81 GB (14.72 GiB)** |
| ROLLBACK_CANDIDATE CH fact_*_direct | 0.43 GB (0.41 GiB) |
| ROLLBACK_CANDIDATE CH shadow склада | 0.90 GB (0.84 GiB) |
| **ROLLBACK_CANDIDATE итого** | **≈17.14 GB (≈15.97 GiB)** |
| вне классификации (решение владельца): bd_retail 6.56 GB, test 1.06 GB | ≈7.6 GB |

## 5. Списки

### DO_NOT_DELETE
- PG `etl_meta.*` целиком (control plane): ch_sync, ch_sync_columns, ch_sync_group, ch_sync_history, ch_sync_partition_state, ch_source_state, registers, register_sources, register_targets, column_mappings, source_unions, source_union_members, load_history, **doc_key (2 GiB, строки не удаляются никогда)**, doc_key_scope, **doc_key_stock_seq**, все `*_id_seq`.
- PG `public.dim_*` (8) и их `*_id_seq` — реестр справочников, источник реплик CH.
- PG **`public.sales_id_seq`, `public.orders_id_seq`** — активные выдатчики id в doc_key (doc_key_scope). Привязаны OWNED BY к замороженным таблицам: `DROP TABLE public.sales/orders` без предварительного `OWNED BY NONE` сломает выдачу id прямого пути.
- PG `etl_meta.ch_source_state` строки `*:shadow` — отметки shadow (нужны для отката склада/продаж).
- CH: cost_daily, dim_* (8), fact_sales, fact_sales_positions, fact_orders, fact_order_positions, fact_stock, fact_stock_positions + все их `_stage`/`_raw` (22 staging, пусты между прогонами, но используются каждым циклом).
- CH `cost_daily` и `dim_nomenklatura` — **читаются PowerBI** (см. п.6).

### REQUIRES BUSINESS / ROLLBACK DECISION
1. PG `public.sales`, `sales_positions`, `orders`, `order_positions` (+ 2 seq позиций) — 15.81 GB; окно отката продаж/заказов от 2026-09-24. Предусловие: снять OWNED BY с sales_id_seq/orders_id_seq; register_targets этих таблиц всё ещё is_active=true — привести конфиг.
2. CH `fact_*_direct` (4) + `fact_*_direct_stage` (4) + конфиги ch_sync code `fact_sales/fact_sales_positions/fact_orders/fact_order_positions` (legacy_frozen) — 0.43 GB; заодно REVOKE ALTER DELETE etl_writer на них.
3. CH `fact_stock_shadow`, `fact_stock_positions_shadow` + 4 staging + конфиги shadow_stock + ch_source_state `onec_register:stock:shadow` — 0.90 GB; окно отката склада от 2026-10-06 (сегодня).
4. Остатки shadow продаж/заказов в control plane: ch_source_state `onec_register:sales:shadow`, `onec_register:order:shadow` (последний прогон 2026-09-24), группа `shadow_1c` (is_active=false; ch_sync-конфигов в ней нет — после cutover они стали боевыми onec_1c).
5. PG БД `bd_retail` на 10.10.1.142 (6.56 GB, нет обращений) и `test` (1.06 GB, архив) — решение владельца.
6. Группа `out_of_scope_powerbi` + 9 неактивных ch_sync `pbi_*` (таблиц в CH нет) — вне scope, не трогать.

## 6. Внешнее BI-использование (system.query_log, окно 2026-09-30 00:00 — 2026-10-06 10:42 UTC)

Пользователь `analytics_reader` (GRANT SELECT ON analytics_poc.*), 19 184 запроса. Клиенты:
- **PowerBI / clickhouse-odbc с 10.10.1.136** — 18 778 запросов; **PowerBI с 192.168.18.233** — 188;
- DataGrip (192.168.18.233) — 206; jdbc — 10; clickhouse-client localhost — 2.

| таблица | запросов | последний | кто |
|---|---|---|---|
| **cost_daily** | 8 247 | 2026-10-06 06:35 | PowerBI (основная нагрузка; DirectQuery-паттерны, JOIN с dim_nomenklatura) |
| **dim_nomenklatura** | 6 332 | 2026-10-06 06:35 | PowerBI (JOIN cost_daily.nomenklatura_id = id; фильтры по code/name, поля category/subcategory*/brand) |
| fact_sales | 12 | 2026-10-06 06:26 | DataGrip ad-hoc (`select * … limit N`) |
| fact_sales_positions | 2 | 2026-10-05 | DataGrip |
| fact_sales_direct | 2 | 2026-10-05 10:58 | DataGrip (LIMIT 10 + SHOW CREATE) |
| fact_stock_positions_shadow | 2 | 2026-10-06 06:31 | DataGrip (LIMIT 10) |
| все остальные 40+ таблиц (включая dim_*_stage, *_raw, *_direct_stage, shadow) | по 1 | 2026-10-05 10:58:58–59 | DataGrip — массовый `SHOW CREATE TABLE` (интроспекция схемы, не использование) |

Справочники:
- **dim_nomenklatura — реально используется BI (PowerBI)**.
- dim_sklad, dim_podrazdelenie, dim_kachestvo, dim_kontragent, dim_dogovor, dim_organizatsiya, dim_otvetstvennyy — **BI-чтений нет**, только 1 `SHOW CREATE` из DataGrip 2026-10-05. Это не повод удалять: они ACTIVE_PRODUCTION (реплики реестра, активные ch_sync, нужны для расшифровки *_id в фактах), а окно лога всего ~6.5 суток.
- Факты fact_orders/fact_order_positions/fact_stock/fact_stock_positions внешним пользователем не читались (кроме SHOW CREATE) — BI пока на них не построен.
- Пользователь `''` в логе — AsyncInsertFlush (асинхронные вставки etl_writer в `*_raw`), не внешний клиент.
- Ошибки analytics_reader: MEMORY_LIMIT (2 GiB) на агрегатах cost_daily×dim_nomenklatura 2026-10-01, обрывы сокета ODBC — к инвентаризации не относится, но говорит о живой нагрузке PowerBI.

Ограничение: query_log хранит ~6.5 суток; редкие (еженедельные/ежемесячные) отчёты могли не попасть в окно. pg_stat — 4 суток.
