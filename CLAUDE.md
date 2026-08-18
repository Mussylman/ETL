# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Проект
ETL-платформа: Apache Airflow + FastAPI конфигуратор для загрузки данных из 1С (MSSQL) в PostgreSQL.
Главная витрина — продажи (`public.sales` + `public.sales_positions`) со слоем справочников `public.dim_*`.

## Ключевые пути
- DAGи: `dags/`
- ETL ядро: `dags/core/` (etl_engine.py, etl_core.py, sales_etl.py)
- Трансформации: `dags/core/transform/` (binary.py, dates.py)
- CLI-инструменты: `dags/core/tools/` (rebuild_sales, run_full_period, load_dim_names)
- Миграции: `dags/core/migrations/` (+ `etl_meta_dump.sql` — снимок конфига)
- Плагины: `plugins/`
- Конфигуратор: `etl_config_app/` (FastAPI, порт 5555)
- Obsidian vault: `docs/`
- Отчёты аудитов: `reports/`, `docs/audits/`

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
| PostgreSQL (витрина + `etl_meta`) | 10.10.1.142:5432/test | `postgre_test_base` |
| MSSQL 1С УПП | 10.10.1.61:1433/UPP_JAN | `mssql_1c_conn` |
| retail (сигнал об изменениях) | 10.10.1.99:5432/ims_db | `bd_retail` |
| 1С meta API | http://192.168.18.224:8090/NikitaBase/hs/meta | — |

## Команды
```bash
# Конфигуратор
cd etl_config_app && uvicorn app:app --host 0.0.0.0 --port 5555 --reload

# Полная пересборка витрины продаж (одна команда; критерий успеха — сверка в ноль)
PYTHONPATH=dags python3 -m core.tools.rebuild_sales --start 2026-06-15 --from-scratch
#   --dry-run план  --verbose полный лог  --from-scratch TRUNCATE только фактов

# Загрузка за период отдельно
PYTHONPATH=dags python3 -m core.tools.run_full_period --register sales --start 4026-06-15 --end 4026-06-26
#   даты в формате 1С (+2000 к году!), --skip-names отключает догрузку имён

# Имена справочников из 1С
PYTHONPATH=dags python3 -m core.tools.load_dim_names            # только stub-строки
PYTHONPATH=dags python3 -m core.tools.load_dim_names --all      # + подхватить переименования

# Сверка витрины с 1С — главный инструмент проверки расхождений
python3 docs/audits/sql/sales_recon.py                          # последние 30 дней
python3 docs/audits/sql/sales_recon.py --start 2026-06-15 --full
python3 docs/audits/sql/sales_recon.py --strict                 # exit 1 при расхождении

# Тесты конфигуратора (каждый — самостоятельный скрипт, без pytest)
python3 etl_config_app/tests/golden_sales_test.py   # round-trip spec + эталон SQL; запускать ПЕРВЫМ
python3 etl_config_app/tests/sync_ddl_test.py       # DDL-инварианты Sync (нужен эталон из golden)
python3 etl_config_app/tests/validator_test.py
```

## Архитектура: как устроена загрузка продаж

**Конфиг живёт в БД, а не в коде.** Схема `etl_meta`: `registers` → `register_sources` (таблицы 1С) →
`column_mappings` (source-колонка → target-колонка + transform) → `register_targets` (целевые таблицы,
`upsert_keys`, `post_load_sql`). Движок читает это через `ConfigLoader` и строит SQL на лету.
Снимок конфига — `dags/core/migrations/etl_meta_dump.sql`.

**Pipeline `accumrg_with_documents`** (`builder/query_builder.py`): один SELECT — регистр
`_AccumRg17844` как основа + LEFT JOIN шапок документов по `_RecorderTRef` + LEFT JOIN табличных
частей по `_LineNo`, значения из нескольких источников через COALESCE.

**Два режима** (`etl_engine.py`): `full_period` (по диапазону дат) и `incremental` (по сигналу retail).
Инкремент: watermark = `MAX(sales.retail_updated_at)`, окно `[watermark−5мин, now−5сек)` + **добор
хвоста** за 7 суток (uid из retail, которых нет в DWH) — без него терялось ~23% документов, потому
что данные появляются в MSSQL позже, чем retail о них сигналит. Было 48ч, расширено 2026-08-18:
магазин, синхронизировавшийся с 1С на третьи сутки, за 48ч не успевал и терялся навсегда.

**Слой справочников (guid→id).** `post_load_sql` таргета после каждой загрузки: создаёт stub-строку
в `dim_*` для незнакомого guid (`ON CONFLICT (guid) DO NOTHING`) и проставляет `*_id` в факте.
Имена приезжают отдельно (`load_dim_names`) из `_Reference*` по guid. `id` справочника выдаётся
один раз и не меняется никогда — на него будет ссылаться BI.

Подробное обоснование: `reports/dwh_int_fk_readiness_2026-07-27.md`, `README_REBUILD.md`,
`docs/sales_load_modes.md`.

## Ловушки, на которых уже спотыкались
- **Год в 1С хранится с офсетом +2000**: 2026 → `4026-06-15`. CLI `run_full_period` принимает даты
  уже в формате 1С, `rebuild_sales` — обычные и конвертирует сам.
- **Время**: `period` — бизнес-дата продажи (Almaty). Все `etl_*`/`retail_*` — Almaty naive.
  `retail.updated_at` — UTC, конвертируется при чтении. `load_history.started_at` — UTC.
- **Запросы в MSSQL идут с `WITH (NOLOCK)`** — иначе SELECT-ы становились жертвой deadlock боевой 1С.
- **DDL таблиц фактов не в миграциях** — их создаёт Sync конфигуратора из мэппингов. Поэтому порядок
  чистой сборки: миграции `etl_meta` → Sync → `007_dim_layer.sql` → загрузка.
- **Миграции 002/003 — seed без ON CONFLICT**: на живой базе продублируют конфиг. Восстанавливать
  конфиг нужно из `etl_meta_dump.sql`, а не из них.
- **Sync дропает колонки, которых нет в мэппингах.** Защищены: `SYSTEM_COLS` в `dao.py`, суффикс `_id`
  (FK dim-слоя) и `is_stub`. Добавляя служебную колонку — проверь, что она под защитой.
- **`recorder` + `recorder_type` + `line_no` — технический хребет** (upsert, добор хвоста,
  missing-delete, сверка). Эти uuid из фактов не удаляются никогда.
- **Пустая ссылка 1С** `00000000-0000-0000-0000-000000000000` — семантически NULL: в справочник не
  попадает, `*_id` остаётся NULL. Это норма, не дыра.
- **retail сигналит не обо всём**: ЧекККМ покрыт полностью; у B2B-реализаций сигнал только при
  создании (правки невидимы); ОтчётКомитенту и возвраты без чека отсутствуют. Некассовые документы
  актуализирует не инкремент, а пересборка + сверка.

## Правила разработки
- UI на русском языке
- Трансформации автоматические по MSSQL типам
- Union/Target/Sync — скрытая механика, не показывать пользователю
- Source of truth для колонок — Column Builder
- Прод-данные меняются только по явному подтверждению; сверка `sales_recon.py` — критерий, что
  витрина сходится с 1С
