# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Проект
ETL-платформа: Apache Airflow + FastAPI конфигуратор. Аналитический слой — **ClickHouse**
(`analytics_poc`), наполняется напрямую из источников:

```
1С (MSSQL) / Retail  →  analytics_sync  →  ClickHouse
```

**PostgreSQL (`etl_prod`) — control plane и реестр ключей, не хранилище фактов:**
- `etl_meta.*` — конфиги, маппинги, watermark, история и состояние публикаций;
- `etl_meta.doc_key` — реестр id документов (guid → id, строки не удаляются никогда);
- `public.dim_*` — реестр справочников (guid → id) + атрибуты, реплицируются в ClickHouse;
- PostgreSQL-таблиц фактов нет: `public.sales`, `sales_positions`, `orders`, `order_positions`
  (заморожены 2026-09-24) удалены 2026-10-07. Их `register_targets` остаются **активными** — это
  конфигурация извлечения прямого пути (`ch_sync.source_params.target`), не таблицы; Sync конфигуратора
  для регистров с `pg_fact_write = false` отключён. Id документов: `doc_key_scope` — sales →
  `public.sales_id_seq` (отвязана от таблицы), orders → `etl_meta.doc_key_orders_seq`, stock →
  `etl_meta.doc_key_stock_seq`.

Витрины ClickHouse: `fact_sales`, `fact_sales_positions`, `fact_orders`, `fact_order_positions`,
`cost_daily`, `dim_*`. Копии для отката — `fact_*_direct` (ROLLBACK_KEEP), склад — `fact_stock*_shadow`.

## Ключевые пути
- DAG'и: `dags/` — core ETL один: `analytics_sync_dag.py` (остальные — отдельные бизнес-DAG'и: GFK, PowerBI, check_orders)
- Прямой путь и runner: `dags/core/clickhouse/` (runner.py, onec.py, patch.py, changes.py,
  registry.py, onec_reconcile.py, engine.py)
- Извлечение из 1С по метаданным: `dags/core/etl_engine.py` (`extract_frame`, `ready_keys`)
- Справочники: `dags/core/tools/load_dim_from_config.py`
- CLI: `dags/core/tools/` (ch_sync, ch_report, ch_ddl, load_dim_from_config, ch_cutover, ch_pg_handover)
- Миграции: `dags/core/migrations/` (control plane ClickHouse-контура — `migrations/clickhouse/`)
- Конфигуратор: `etl_config_app/` (FastAPI, один экземпляр — PROD :5556, запуск — `etl_config_app/RUNNING.md`)
- Obsidian vault: `docs/`; отчёты аудитов: `reports/`, `docs/audits/`

## Obsidian Knowledge Vault
При старте сессии прочитай `docs/00-home/index.md` для понимания контекста.

### Структура
- `docs/00-home/` — навигация и приоритеты
- `docs/atlas/` — архитектура, стек, БД, подключения
- `docs/knowledge/integrations/` — каждая интеграция (1С, GFK, Sheets, Telegram)
- `docs/knowledge/decisions/` — решения с обоснованиями
- `docs/knowledge/debugging/` — баги и их решения
- `docs/knowledge/patterns/` — паттерны DAGов и загрузки
- `docs/knowledge/business/` — контекст: кто пользователи, зачем платформа
- `docs/sessions/` — логи сессий разработки
- `docs/inbox/` — необработанные заметки

### Правила
- После завершения сессии — сохрани лог в `docs/sessions/YYYY-MM-DD тема.md`
- При решении бага — создай заметку в `docs/knowledge/debugging/`
- При принятии архитектурного решения — `docs/knowledge/decisions/`
- Названия файлов = утверждения, не категории
- Wiki-ссылки `[[имя заметки]]` между связанными
- Frontmatter с tags и date
- Язык: русский

## Подключения
Креды берутся из Airflow connections, в коде их нет.

| Система | Адрес | Airflow conn_id |
|---|---|---|
| PostgreSQL PROD (control plane, реестры) | 10.10.1.142:5432/etl_prod | `etl_prod` |
| PostgreSQL `test` — пассивная архивная БД, автоматически не пишется | 10.10.1.142:5432/test | `postgre_test_base` (только ручной доступ) |
| ClickHouse (аналитический слой) | 10.10.1.142:9000/analytics_poc | `clickhouse_etl` (etl_writer) |
| MSSQL 1С УПП | 10.10.1.61:1433/UPP_JAN | `mssql_1c_conn` |
| retail (сигнал об изменениях) | 10.10.1.99:5432/ims_db | `bd_retail` |
| retail себестоимость | 10.10.1.85:5432/main_db | `retail_cost` |
| 1С meta API | http://192.168.18.224:8090/NikitaBase/hs/meta | — |

Конфигуратор: секреты только из окружения — PROD `~/.config/etl_config/prod.env` (700/600, из Airflow
connections), имена — `etl_config_app/.env.example`; нет обязательной переменной — отказ до подключения.
**Ротация:** пароли `airflow_admin` (PostgreSQL 10.10.1.142) и `musulmon.k` (1С MSSQL) были в git
с первого коммита 2026-04-14 — сменить, затем обновить Airflow connections и prod.env.
Admin ClickHouse (только ручной DDL, в Airflow не заведён): `~/.config/clickhouse/ch_admin.xml`
(600, вне репозитория) — дефолт `--ch-config` в `ch_ddl` / `ch_cutover`.
MSSQL PowerBI 10.10.1.136 — **вне scope**, не трогать.

## Команды
```bash
# Конфигуратор — только PROD :5556 (без ETL_CONFIG_DB_NAME не стартует; полная команда — RUNNING.md)
cd etl_config_app && ETL_CONFIG_DB_NAME=etl_prod ETL_CONFIG_ENV_LABEL="PROD / etl_prod" \
  nohup ../venv/bin/uvicorn app:app --host 0.0.0.0 --port 5556 > ../logs/etl_config_prod.log 2>&1 &

# Группа analytics_sync вручную (то же, что делает DAG; режим patch | rebuild | hot | sweep)
PYTHONPATH=dags python3 -m core.tools.ch_sync --config-conn etl_prod --group onec_1c --mode patch --apply
# Пересборка месяцев регистра 1С в ClickHouse (ремонт / backfill; generic)
PYTHONPATH=dags python3 -m core.tools.ch_sync --config-conn etl_prod --group onec_1c --mode rebuild --partition 202609 --apply

# Справочники 1С → реестр PostgreSQL (то же, что группа dim_registry)
PYTHONPATH=dags python3 -m core.tools.load_dim_from_config --dim dim_product --mode incremental --pg-conn etl_prod
#   --mode register — завести все объекты 1С, которых нет в справочнике (id существующих не меняются)

# Отчёт сверки источник ↔ ClickHouse по обобщённым источникам (справочники, cost_daily)
PYTHONPATH=dags python3 -m core.tools.ch_report --config-conn etl_prod

# Тесты конфигуратора — ТОЛЬКО вручную и ТОЛЬКО на архивной test (создают/удаляют свои схемы);
# на etl_prod не запускать
ETL_CONFIG_DB_NAME=test python3 etl_config_app/tests/golden_sales_test.py   # запускать ПЕРВЫМ
ETL_CONFIG_DB_NAME=test python3 etl_config_app/tests/sync_ddl_test.py
ETL_CONFIG_DB_NAME=test python3 etl_config_app/tests/validator_test.py
```

**Не для PROD:** `core.tools.rebuild_sales`, `run_full_period`, `docs/audits/sql/sales_recon.py` —
инструменты фактов PostgreSQL. На PROD факты заморожены: `rebuild_sales` отказывает до любого шага
(`pg_fact_write=false`, а `--from-scratch` — в любом контуре с `etl_meta.doc_key`), `ETLEngine.run`
запись запрещает.

**Соединение с PostgreSQL — только явно.** У CLI (`--pg-conn` / `--config-conn` / `--conn`) и классов
движка (`ETLEngine`, `ConfigLoader`, `DataChecker`, `TransformUtils`, `Loaders`) значения по умолчанию нет:
пропущенное соединение — немедленный отказ до подключения (`core/conn.py`). Не заменять умолчанием `etl_prod`.

**TEST-контур удалён 2026-09-28**: DAG `incremental` и TEST-конфигуратор :5555. База `test`
(`postgre_test_base`) сохранена как пассивная test/archive DB — ни один DAG, сервис, cron или триггер в неё
не пишет; доступ к ней — только явным `--pg-conn postgre_test_base` / `ETL_CONFIG_DB_NAME=test`.

## Архитектура

**Один core ETL DAG — `analytics_sync`**: одна стабильная задача `sync`, раз в 5 минут. Группы и их
порядок runner читает в момент выполнения из `etl_meta.ch_sync_group` (правка конфига не меняет
структуру DAG):

| position | группа | runner | что делает |
|---|---|---|---|
| 5 | `dim_registry` | reference_dim | справочники 1С → реестр PostgreSQL (`dim_*`) |
| 10 | `core_pg_to_ch` | ch_sync | справочники PostgreSQL → ClickHouse |
| 15 | `onec_1c` | ch_sync | заказы, затем продажи: 1С → ClickHouse напрямую |
| 20 | `retail` | ch_sync | `cost_daily` |

Группы изолированы: сбой одной не останавливает и не перезапускает остальные. Режим прогона —
по отметкам последнего успешного выполнения в `ch_source_state` (не по минуте слота):
**patch** (набор изменений retail + 15-дневный хвост), **hot** (раз в час — пересборка текущего и
прошлого месяца: документы без сигнала retail, правки некассовых документов), **sweep** (раз в сутки
после 03:00 Almaty — сверка всей истории 1С ↔ ClickHouse, несошедшиеся месяцы пересобираются).

**Конфиг живёт в БД, а не в коде.** Извлечение из 1С — `etl_meta.registers` → `register_sources` →
`column_mappings` → `register_targets` (как и раньше, через `ConfigLoader` / `ETLEngine.extract_frame`).
Публикация в ClickHouse — `etl_meta.ch_sync` + `ch_sync_columns`. Снимок конфига первого hop'а —
`dags/core/migrations/etl_meta_dump.sql`.

**Change-provider** (`changes.py`): сигнал retail ≠ готовность в 1С. Кандидаты → точный lookup в 1С →
`ready` (в патч) / `pending` (нет в 1С и в витрине — ждёт в хвосте) / `actually_deleted` (нет в 1С,
есть в витрине — патч удаляет). Watermark — в control plane, не `MAX()` факта.

**Публикация партиций** (`patch.py`): текущая партиция − старые строки изменившихся документов +
свежие строки → сверка отпечатков → атомарный REPLACE/MOVE PARTITION. Плохая партиция не публикуется.

**Реестр ключей.** Документы: `etl_meta.doc_key` (issuer `registry`, последовательности — те же, что у
старых фактов); ссылки на ещё не загруженный документ получают заготовку `is_stub`. Справочники:
`dim_*`, stub по первой ссылке (`ON CONFLICT (guid) DO NOTHING`); номенклатура
(`dim_key_source='source'`) регистрирует все товары 1С сама — `cost_daily` и будущие остатки не
зависят от продаж. `id` выдаётся один раз и не меняется никогда.

**Сверка — критерий корректности**: независимый отпечаток 1С ↔ ClickHouse (`onec_reconcile.py`),
выводится из метаданных; ночной sweep делает её по всей истории.

Решения: `docs/knowledge/decisions/Прямой путь 1С → ClickHouse — переключение без двух выдающих id и
без потери ссылок.md`, лог переключения — `docs/sessions/2026-09-24 …`.

## Ловушки, на которых уже спотыкались
- **Год в 1С хранится с офсетом +2000**: 2026 → `4026-06-15`. Runner и `_period_bounds` конвертируют
  сами; даты в запросах к MSSQL — в формате 1С.
- **Время**: `period` — бизнес-дата (Almaty). `etl_*`/`retail_*`, watermark — Almaty naive.
  `retail.updated_at` — UTC, конвертируется при чтении. `load_history.started_at` — UTC. Сервер — UTC.
- **Запросы в MSSQL идут с `WITH (NOLOCK)`** — иначе SELECT-ы становились жертвой deadlock боевой 1С.
- **MSSQL-хэши для сверки — через UTF-8 collation** `Latin1_General_100_BIN2_UTF8` (nvarchar = UTF-16LE).
- **ClickHouse: AST-лимит и ARG_MAX** — длинные списки ключей строковыми литералами, запросы через stdin.
- **Миграции 002/003 — seed без ON CONFLICT**: на живой базе продублируют конфиг. Восстанавливать
  конфиг нужно из `etl_meta_dump.sql`, а не из них.
- **Sync конфигуратора дропает колонки, которых нет в мэппингах** (факты PostgreSQL — ROLLBACK_KEEP:
  Sync по регистрам sales/order на PROD не запускать).
- **`recorder` + `recorder_type` + `line_no` — технический хребет** (патч, хвост, удаления, сверка).
- **Пустая ссылка 1С** `00000000-0000-0000-0000-000000000000` — семантически NULL: в справочник не
  попадает, `*_id` = 0. Это норма, не дыра.
- **retail сигналит не обо всём**: B2B-реализации и заказы — только при создании (правки невидимы);
  ОтчётКомитенту и возвраты без чека отсутствуют; даже ЧекККМ — не все (сентябрь 2026: 12 из 39 092).
  Это закрывает не патч, а hot-пересборка и ночной sweep.
- **Реплики `dim_*` в ClickHouse** обновляются группой `core_pg_to_ch`: заготовка, созданная фактом в
  том же прогоне, появится в реплике в следующем — сверка по guid на это время покажет расхождение.

## Правила разработки
- UI на русском языке
- Трансформации автоматические по MSSQL типам
- Union/Target/Sync — скрытая механика, не показывать пользователю
- Source of truth для колонок — Column Builder
- Прод-данные меняются только по явному подтверждению; критерий, что ClickHouse сходится с 1С, —
  сверка `onec_reconcile` / ночной sweep
- Не ждать внешние процессы: без sleep / polling / background waiters — одна проверка статуса,
  RUNNING/PENDING и дальше
