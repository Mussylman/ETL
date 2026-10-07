# PROJECT_BRIEFING

> Технический брифинг для передачи AI-агенту, не имеющему доступа к коду.
> Данные собраны из реального состояния репозитория. Источники указаны путями с номерами строк.
> Если в коде чего-то нет — отмечено «не нашёл в коде».

---

## 1. Назначение проекта

ETL-платформа: оркестрирует выгрузку оперативных данных из 1С:Управление производственным предприятием (УПП) в PostgreSQL-витрину для Power BI и собственных дашбордов. Под капотом — Apache Airflow для расписаний и FastAPI-конфигуратор (PROD :5556), в котором аналитики маппят колонки 1С на целевые таблицы и распределяют их по dim/fact через канбан-интерфейс.

**Бизнес-процессы 1С которые обслуживает (по коду / зарегистрированным конфигурациям):**
- Продажи: `Документ.ЧекККМ`, `Документ.РеализацияТоваровУслуг`, `Документ.ВозвратТоваровОтПокупателя` (и ещё 3 типа документов, найденные через Discover в регистре накопления `_AccumRg17844`)
- Заказы (`public.orders` в bd_retail — внутренняя БД retail-сервиса) — дневная сверка
- Склад / остатки (миграция `003_stock_register.sql`, таблицы `stock_movements`, `stock_current`, `stock_daily`)
- GFK: внешний CSV-отчёт продаж/товаров, загружается в MSSQL
- Справочники из Google Sheets: `dim_user_name`, `dim_asp_products`

**Точная версия 1С (УПП v1.3.27.1) в коде не зафиксирована** — есть только имя БД `UPP_JAN` и контракт HTTP API `/hs/meta` на сервере 1С.

**Конечные потребители:**
- Команда BI-аналитики (3 человека) — строят отчёты в Power BI
- Inventory-сервис — `check_orders_dag` шлёт алерты по заказам в Telegram

---

## 2. Архитектура

### Источники данных
| Источник | Адрес | Что отдаёт |
|---|---|---|
| MS SQL (1С backend) | `10.10.1.61:1433`, БД `UPP_JAN` (`mssql_client.py:8`) | Сырые таблицы `_Document*`, `_Document*_VT*` (табличные части), `_AccumRg*` (регистры накопления), `_RecorderTRef`/`_RecorderRRef` |
| 1C HTTP API | `http://192.168.18.224:8090/NikitaBase/hs/meta` (`onec_client.py:8`, `plugins/onec_api.py:26`) | Метаданные: `/search?q=…`, `/db_structure/<table>` — резолв русских имён, поля, типы. Также возвращает VT-таблицы документов |
| Retail Postgres (`bd_retail`) | `10.10.1.142:5432/bd_retail` (`dao.py:RETAIL_DB_CONFIG`) | Триггер инкрементальной загрузки: таблица retail-сервиса с `updated_at` |
| Google Sheets | CSV-export URL (`update_users.py:15,50`) | Внешние справочники сотрудников и продуктов |
| GFK | через `plugins/gfk_client.py` (`GFK.get_reportId`, `read_csv_file`) — детали реализации в файле | Внешний CSV отчётов продаж |

### Промежуточные слои
1. **ETL Engine** (`dags/core/etl_engine.py`) — конфигурируемый движок. Читает `etl_meta` схему, строит SQL c JOIN/UNION ALL, тянет данные, трансформирует, грузит.
2. **SalesETL** (`dags/core/sales_etl.py`) — специализированный оркестратор для пары `sales` + `sales_positions`: UPSERT шапок, DELETE+INSERT позиций.
3. **ETLCore** (`dags/core/etl_core.py`) — legacy движок с hardcoded конфигурацией (помечен deprecated).
4. **ETL Config App** (`etl_config_app/`) — отдельное FastAPI-приложение (PROD :5556; TEST :5555 удалён 2026-09-28). Не процесс Airflow, запускается отдельно через uvicorn. Управляет схемой `etl_meta` (регистры, источники, маппинги колонок, юнионы, таргеты, include_columns, target_role).

### Целевые хранилища
- **PostgreSQL `10.10.1.142:5432/test`**:
  - Схема `etl_meta` — конфигурация (7 таблиц: registers, register_sources, column_mappings, source_unions, source_union_members, register_targets, load_history) — см. `dags/core/migrations/001_create_etl_meta_schema.sql`
  - Схема `public` — сами витрины: `sales`, `sales_positions` (и/или `dim_sales` + `fact_sales_products` в зависимости от регистра), `stock_movements`, `stock_current`, `stock_daily`
- **MSSQL `powerbi_connect`** (вторая MSSQL-инстанция, отличная от 1С) — туда DAG `update_users_and_products` пишет `dim_user_name`, `dim_asp_products`. Используется PowerBI.
- **MSSQL `etl_gfk`** — куда DAG `gfk_update` пишет таблицы `gfk_csv`, `products`.

### Поток данных (текст)
```
[1С MSSQL: _Document*, _AccumRg*, _Document*_VT*]
    │
    ├─(метаданные)─► [1C HTTP API /hs/meta] ──► ETL Config App (discover)
    │
    ▼ (полные строки по период/UID)
StorageConnector — SQL c UNION ALL по типам + JOIN header↔VT
    │
    ▼ DataFrame
TransformUtils:
  binary(16)→UUID,  binary(4)→int,  binary(1)→bool
  fix_year (год > 3000 → −2000)
  recorder_type_lookup (binary→название документа)
  rename source_col→target_col
  custom_python (sales_with_vat, earned_bonuses, ...)
    │
    ▼ split по include_columns (target_role)
Loaders.load(...)
    │
    ├──► dim_sales         (priority=0, upsert по recorder, id=SERIAL)
    └──► fact_sales_products  (priority=1, insert)
              │
              ▼ post_load_sql (resolve FK)
         UPDATE fact f SET sales_id = d.id
            FROM dim_sales d WHERE f.recorder = d.recorder
    │
    ▼
PostgreSQL public.*  →  Power BI / прочие дашборды
```

Параллельно: триггер инкрементальной загрузки — `DataChecker` опрашивает `bd_retail.sales.updated_at`, получает список изменившихся `document_uid`, по ним идёт чтение из 1С (`SalesETL._detect_changes`).

---

## 3. Стек

### Airflow
- **Версия:** `apache-airflow 3.0.6` (`pip list`)
- **Executor:** `LocalExecutor` (`airflow.cfg`)
- **Metadata DB:** `postgresql+psycopg2://airflow_admin:****@10.10.1.142:5432/airflow`
- **DAG folder:** `/home/dev/airflow/dags`
- **load_examples:** False
- Запущен напрямую на хосте `sev` (не Docker)

### Провайдеры Airflow (`pip list`):
- `apache-airflow-providers-microsoft-mssql 4.3.1`
- `apache-airflow-providers-postgres 6.2.0`
- `apache-airflow-providers-celery 3.11.0` (установлен, но executor = Local)
- `apache-airflow-providers-fab 2.3.0`
- `apache-airflow-providers-smtp 2.2.0`
- `apache-airflow-providers-standard 1.6.0`

### Python (3.10.12)
- `pandas 2.3.1`
- `psycopg2-binary 2.9.10`
- `pymssql` (через airflow-провайдер + напрямую в `etl_config_app/mssql_client.py`)
- `fastapi 0.116.1` + `uvicorn` — конфигуратор
- `Jinja2 3.1.6` — шаблоны UI
- `requests` — 1C HTTP API клиент
- `pendulum` — таймзоны
- `telebot` (pyTelegramBotAPI) — `helpers/telegram_loggerr.py`
- `duckdb` — для `pre_load_sql` (DataFrame SQL) в `etl_engine._apply_pre_load_sql`

**В репо нет `requirements.txt` / `pyproject.toml`** — зависимости установлены глобально в venv хоста. Это технический долг (упомянут в `docs/00-home`).

### Внешние сервисы
| Сервис | Назначение |
|---|---|
| Obsidian Vault `docs/` | Документация (decision/patterns/debugging/sessions). Используется параллельно как knowledge base — не как зависимость кода |
| MS SQL Server (10.10.1.61) | Backend 1С УПП |
| MS SQL Server (через `powerbi_connect`, `etl_gfk` conn_id) | Витрины для Power BI и GFK — отдельные инстансы, адреса в Airflow Connections, не в коде |
| PostgreSQL (10.10.1.142) | Метаданные Airflow, конфигурация ETL (`etl_meta`), витрины (`public`), retail-источник (`bd_retail`) |
| Telegram Bot API | Алерты по результатам DAG `check_orders_dag` |
| Google Docs (export CSV) | Источник справочников `dim_user_name`, `dim_asp_products` |
| Claude Design (claude.ai/design) | Подключён к репо `Mussylman/ETL` — генератор UI-макетов (не run-time зависимость, dev-tool) |

### Кастомные модули
| Модуль | Путь | Назначение |
|---|---|---|
| `core` package | `dags/core/` | ETLEngine, SalesETL, ColumnMapper, ConfigLoader, QueryBuilder, StorageConnector, DataChecker, TransformUtils, Loaders |
| `core.transform` | `dags/core/transform/` | `binary.py`, `dates.py`, `cast.py`, `custom.py` (вычисляемые колонки), `transform_utils.py` |
| `core.config` | `dags/core/config/` | dataclasses (`models.py`) + `config_loader.py` |
| `helpers` | `dags/helpers/` | `log_setup.py`, `telegram_loggerr.py` |
| `plugins` (DAG-side) | `dags/plugins/` | `gfk_client.py` (класс `GFK`) |
| `plugins` (Airflow plugin) | `plugins/` | `etl_meta_dao.py`, `etl_meta_plugin.py`, `etl_meta_views.py`, `onec_api.py` — Airflow plugin для UI внутри Airflow (тонкая обёртка над `etl_config_app`) |
| `etl_config_app` | `etl_config_app/` | FastAPI: `app.py`, `dao.py`, `mssql_client.py`, `onec_client.py` + Jinja-шаблоны + статика |

---

## 4. Структура репозитория

```
/home/dev/airflow/
├── airflow.cfg                  # LocalExecutor, dags_folder, metadata DB
├── CLAUDE.md                    # инструкции для Claude Code (контекст, пути, БД)
├── PROJECT_BRIEFING.md          # этот файл
├── README.md                    # (не найдено в коде на root)
│
├── dags/                        # DAG-и + ETL ядро (core/)
│   ├── orders_check_dag.py      # check_orders_dag — заказы + Telegram-алерт
│   ├── update_users.py          # update_users_and_products — Google Sheets → MSSQL
│   ├── gfk_report.py            # gfk_update — внешний GFK → MSSQL
│   ├── core/                    # ETL-ядро (импортируется DAG-ами)
│   │   ├── __init__.py
│   │   ├── README.md            # документация ядра
│   │   ├── etl_engine.py        # ETLEngine (конфигурируемый)
│   │   ├── etl_core.py          # ETLCore (deprecated, hardcoded)
│   │   ├── sales_etl.py         # SalesETL для sales+sales_positions
│   │   ├── config/              # ConfigLoader, models (dataclasses)
│   │   ├── builder/             # QueryBuilder (JOIN + UNION ALL)
│   │   ├── extract/             # StorageConnector, DataChecker
│   │   ├── transform/           # binary, dates, cast, custom, transform_utils
│   │   ├── load/                # Loaders (insert/upsert/replace)
│   │   └── migrations/          # 001_create_etl_meta_schema.sql, 002, 003
│   ├── helpers/                 # log_setup, telegram_loggerr
│   └── plugins/                 # gfk_client.GFK
│
├── etl_config_app/              # FastAPI-конфигуратор (PROD :5556)
│   ├── app.py                   # FastAPI endpoints + Jinja routes
│   ├── dao.py                   # доступ к etl_meta схеме + retail Postgres
│   ├── mssql_client.py          # лёгкий клиент MSSQL для discover
│   ├── onec_client.py           # клиент 1C HTTP API
│   ├── static/                  # css/, js/
│   └── templates/               # registers/, sources/, unions/, targets/, mappings/, members/, base.html
│
├── plugins/                     # Airflow plugin (UI-views внутри Airflow)
│   ├── etl_meta_dao.py
│   ├── etl_meta_plugin.py
│   ├── etl_meta_views.py
│   ├── onec_api.py              # тонкая обёртка над etl_config_app/onec_client.py
│   └── templates/
│
├── docs/                        # Obsidian Vault (Markdown, RU)
│   ├── 00-home/                 # index.md, "Текущие приоритеты.md"
│   ├── atlas/                   # схема БД, подключения, стек
│   ├── knowledge/
│   │   ├── decisions/           # архитектурные решения
│   │   ├── patterns/            # паттерны DAG-ов
│   │   ├── debugging/           # баги и решения
│   │   ├── integrations/        # 1С, GFK, Sheets, Telegram
│   │   └── business/            # бизнес-контекст
│   ├── sessions/                # дневник сессий (2026-04-10, -04-14, -04-22)
│   ├── inbox/                   # необработанные заметки
│   └── infographic.html         # презентационная инфографика
│
├── test_scripts/                # эксперименты вне Airflow
│
└── logs/                        # Airflow task logs (по dag_id)
```

---

## 5. Ключевые DAG-и

### `check_orders_dag` — `dags/orders_check_dag.py`
- **Schedule:** `0 7 * * *` (каждый день 07:00 локально, `start_date` в `Asia/Almaty`)
- **Что делает:** забирает заказы из retail Postgres за текущий `ds`, считает `status=3` (успешные) и прочие, шлёт сводку в Telegram.
- **Источник:** `BaseHook.get_connection("conn_inventory")` → `public.orders`
- **Назначение:** Telegram (через `Variable.get("telegram_bot_token")` и `telegram_chat_id`)
- **Особенности:** SQL подставляется f-string'ом с датой — это формально SQL-injection, но дата приходит из Airflow `ds`, не от пользователя. Использует raw psycopg2 connection через SQLAlchemy engine.

### `update_users_and_products` — `dags/update_users.py`
- **Schedule:** `0 6 * * *`
- **Что делает:** тянет два Google Sheets через CSV-export → стирает `DELETE FROM ...` → построчно `INSERT` в `dim_user_name` и `dim_asp_products` в MSSQL `powerbi_connect`.
- **Источник:** Google Sheets (захардкоженные `sheet_id` и `gid` в коде)
- **Назначение:** MSSQL (через conn_id `powerbi_connect`)
- **Особенности:**
  - Стирание целиком + полная перезагрузка (truncate-and-load), без транзакций — есть окно «пустой таблицы».
  - Построчный INSERT (медленно при росте).
  - Таски параллельные (нет `>>` зависимости).

### `gfk_update` — `dags/gfk_report.py`
- **Schedule:** `0 7 * * *`
- **Что делает:** через `plugins.gfk_client.GFK` забирает `reportId` для periods (по умолчанию из `Variable.get("gfk_period")`), читает CSV, вставляет в MSSQL.
- **Источник:** GFK external service (детали в `dags/plugins/gfk_client.py`)
- **Назначение:** MSSQL `etl_gfk` → таблицы `gfk_csv`, `products`
- **Особенности:** `period` приходит из Airflow Variable; если не задан — `None` и берётся «по умолчанию» в GFK-клиенте.

### Где ETLEngine / SalesETL?
- **DAG-обёрток для них в репо нет** — `core/etl_engine.py` и `core/sales_etl.py` доступны как библиотеки, но ни один `.py` в `dags/` не оборачивает их в Airflow DAG. Старый путь фактов PostgreSQL выключен (таблицы фактов удалены 2026-10-07), ручной скрипт `test_scripts/test_sales.py` удалён.

---

## 6. Конвенции и решения

### Именование таблиц и полей
- **Целевые таблицы PostgreSQL** — snake_case английский (`sales`, `sales_positions`, `dim_sales`, `fact_sales_products`, `stock_current`, `dim_asp_products`).
- **Имена колонок в витрине** — английский snake_case. Стандарт принят в `dags/core/transform/custom.py`:
  - Стоимость → `cost`
  - СтоимостьБезСкидок → `sales_without_discounts`
  - НДС → `vat`
  - Количество → `quantity`
  - Эврика_Бонусы → `bonuses`
  - Эврика_Списанные → `used_bonuses_raw`
- **1С таблицы** в MSSQL имеют префикс `_` (`_Document476`, `_AccumRg17844`, `_Document476_VT13626`). VT — табличные части документа, связь `_Document{N}_VT{M}._Document{N}_IDRRef = _Document{N}._IDRRef`.
- **Source codes** в `etl_meta.register_sources` — `doc_{type_int}` (например `doc_476`), `doc_{type_int}_vt_{vt_num}` (`doc_476_vt_13626`).
- **target_role** в `register_targets` — `'dimension'` или `'fact'`.

### GUID ↔ _IDRRef
1С хранит UUID в `binary(16)` с **нестандартным порядком байтов**. Преобразование — `dags/core/transform/binary.py:binary_to_uuid`:
```python
b = bytes (16 байт)
part1 = b[8:16][::-1]      # реверсируем второй полу-блок
full  = part1 + b[0:8]
UUID(bytes = full[0:4][::-1] + full[4:6][::-1] + full[6:8][::-1] + full[8:])
```
Функции `convert_guid_to_idrref_1c` или подобной для обратного преобразования (UUID → binary для подстановки в MSSQL WHERE) **в коде не найдено**. В `transform_utils.py:78` есть `convert_idrref_to_guid(b)` — обёртка вокруг `binary_to_uuid`. Если нужно ходить обратно (UUID → binary для запросов), эту функцию надо написать.

`binary(4)` → int (для `_RecorderTRef` — тип-документа). `binary(1)` → bool. Авто-роутинг по длине — `process_binary_auto`.

### Даты
1С хранит даты в формате с годом `+2000` (например `4025-10-15` вместо `2025-10-15`). Все даты проходят `dags/core/transform/dates.py:fix_year` (`offset=2000`, применяется когда `year > 3000`). Edge-case `29 февраля → 1 марта` обрабатывается явно.

### recorder_type_lookup
`_RecorderTRef` (binary(4) → int) → название документа («Документ.ЧекККМ»). Маппинг хранится в `etl_meta.registers.recorder_type_map` (JSONB), заполняется автоматически при Discover в ETL Config App. Применяется как `transform_type='recorder_type_lookup'` с `transform_params={"map": {476: "Документ.ЧекККМ", …}}`.

### Custom Python Transforms
Row-level вычисления через `dags/core/transform/custom.py`:
- Реестр `CUSTOM_TRANSFORMS` с функциями: `sales_with_vat`, `sales_without_vat`, `earned_bonuses`, `used_bonuses`, `margin`, `unit_price`, `discount_percent`.
- Каждая функция принимает `row: dict`, возвращает значение.
- Знак по типу документа — `_doc_sign(row)`: «Возврат» → -1, «Реализация» → +1, ЧекККМ — по `operation_type` поля.
- В column_mappings: `transform_type='custom_python'`, `transform_params={"function":"sales_with_vat"}`, `source_column='__computed__<func>'` (NOT NULL обход).
- Валидация: при создании колонки через UI проверяется наличие всех `uses_columns` в маппингах регистра.

### Split dim / fact
- `register_targets` имеет поля `target_role` ('dimension'/'fact'), `priority`, `include_columns` (TEXT[]), `post_load_sql`.
- Один SQL extract → один DataFrame → split по `include_columns` → отдельные load по приоритету (dim первым).
- FK resolve через `post_load_sql`: `UPDATE fact SET sales_id = dim.id FROM dim WHERE fact.recorder = dim.recorder`.
- В Column Builder UI: header-source колонки auto → dim, VT-source → fact, standalone (_AccumRg) — вручную.

### Обработка ошибок, ретраи, алерты
- `default_args` в DAG-ах: `retries=1`, `retry_delay=5min`. Email не настроен.
- **Telegram-алерты только в `check_orders_dag`** — остальные DAG-и не уведомляют о падениях.
- В `etl_config_app/app.py` многие endpoint-ы оборачивают тело в `try/except Exception` и возвращают `JSONResponse({"error": str(e)}, 500)`.
- DataChecker, StorageConnector — печатают `print(...)` вместо логирования.

### Логирование
- DAG-овые таски — стандартный Airflow logger (`logging.info(...)` в `orders_check_dag.py`).
- `helpers/telegram_loggerr.py` — кастомный `TelegramHandler` (через `pyTelegramBotAPI`), используется только в `orders_check_dag`. Имеет особенность: парсит `record.pathname`, если `script_name == 'main.py'` → переименовывает в «Проверка заказов» (легаси).
- Конфигуратор пишет в stdout uvicorn-а (`/tmp/uvicorn.log` при запуске через nohup).
- Структурированного логирования нет.

---

## 7. Текущие проблемы и TODO

### Что не работает или работает плохо
- **DAG-обёрток для ETLEngine/SalesETL нет** — нельзя через Airflow UI триггернуть продакшен-выгрузку sales. Сейчас запускается из ETL Config App вручную.
- **Truncate-and-load в `update_users.py`** — окно «пустой таблицы». Хорошо бы делать в транзакции или через upsert.
- **Построчные INSERT** в `update_users.py` — медленно при росте.
- **f-string SQL** в `orders_check_dag.py` — формальный SQL injection (хотя ds контролируем).
- **Captured-данные в регистре 21:** колонка `kolichestvo` ошибочно привязана к `ДокументОснование` (зафиксировано в session log 2026-04-22). Поле `Количество` лежит только в VT.
- **NOT NULL на `source_column`** в `column_mappings` — обход через placeholder `__computed__<func_name>` для computed-колонок.
- **Pipeline UI** (Column Builder из Claude Design) — последнее изменение; нуждается в дотестировании на регистре 22.
- **`post_load_sql` для resolve FK** `dim_id` → `fact_sales_products` не написан (только заглушка); FK сейчас остаётся NULL.

### Что планируется (`docs/00-home/Текущие приоритеты.md`)
**Критично:**
- Полный flow тест: Register → Discover → Column Builder → Split dim/fact → Sync
- Тестирование custom python transforms на реальных данных
- Написать `post_load_sql` для резолва FK
- Дотестировать Pipeline UI

**Важно:**
- Vector-версия custom transforms (когда дойдёт до миллионов строк) — сейчас `df.apply(axis=1)`.
- Валидация `uses_columns` при сохранении маппинга.
- Перевод UI на русский — остатки tooltip'ов / валидаций.

**Перед продом:**
- Пароли в env (сейчас захардкожены: `dao.py:DB_CONFIG`, `mssql_client.py:MSSQL_CONFIG`, см. §8 ниже).
- Обработка таймаутов MSSQL и недоступности 1С API.
- Backup `etl_meta` схемы.

### Технический долг
- Нет `requirements.txt` / `pyproject.toml`.
- Нет CI / тестов (pytest конфиги отсутствуют).
- Дублирование: `plugins/onec_api.py` — тонкая обёртка над `etl_config_app/onec_client.py`. Помечено deprecated.
- `dags/core/etl_core.py` — помечен deprecated, но всё ещё в репо.
- Print-debugging вместо logger в `core/*`.

---

## 8. Окружение

### Хост
- **Hostname:** `sev`
- **Пользователь:** `dev`
- **AIRFLOW_HOME:** `/home/dev/airflow` (он же текущий рабочий каталог)
- **Python:** 3.10.12 (системный)
- **venv:** `/home/dev/airflow/venv` (по выводу traceback'ов uvicorn-логов)
- **Уровень: bare-metal / VM** (не Docker)

### Пути к ключевым файлам
| Что | Путь |
|---|---|
| Airflow config | `/home/dev/airflow/airflow.cfg` |
| DAGs | `/home/dev/airflow/dags/` |
| ETL core | `/home/dev/airflow/dags/core/` |
| Конфигуратор | `/home/dev/airflow/etl_config_app/` |
| Plugin (Airflow side) | `/home/dev/airflow/plugins/` |
| Логи Airflow | `/home/dev/airflow/logs/dag_id=*` |
| Логи uvicorn | `/tmp/uvicorn.log` |
| Obsidian Vault | `/home/dev/airflow/docs/` |
| CLAUDE.md (project) | `/home/dev/airflow/CLAUDE.md` |
| Миграции etl_meta | `/home/dev/airflow/dags/core/migrations/` |

### Airflow Variables (используются в коде)
- `telegram_bot_token` — токен Telegram-бота
- `telegram_chat_id` — целевой чат для алертов
- `gfk_period` — период отчёта GFK (опционально)

### Airflow Connections (по `conn_id`, упомянутым в коде)
| conn_id | Тип | Назначение |
|---|---|---|
| `mssql_1c_conn` | MSSQL | 1С backend `UPP_JAN` (10.10.1.61:1433) |
| `etl_prod` | Postgres | control plane (`etl_meta`) + реестры (`doc_key`, `dim_*`); архивная БД `test` удалена 2026-10-07 |
| `bd_retail` | Postgres | Retail-БД для инкрементальной загрузки |
| `powerbi_connect` | MSSQL | Power BI витрина (для dim_user_name, dim_asp_products) |
| `etl_gfk` | MSSQL | GFK витрина |
| `conn_inventory` | Postgres | Inventory `public.orders` (используется в `orders_check_dag`) |

### Креды (с 2026-09-28 в tracked-файлах нет; старые значения были в git — см. ротацию в CLAUDE.md)
- `etl_config_app/dao.py` — PostgreSQL и retail конфигуратора: только из окружения (`ETL_CONFIG_DB_*`, `ETL_CONFIG_RETAIL_DB_*`), PROD — `~/.config/etl_config/prod.env` (600); значений в коде нет (с 2026-09-28)
- `etl_config_app/mssql_client.py` — 1С MSSQL: только из окружения (`ETL_CONFIG_MSSQL_*`); значений в коде нет (с 2026-09-28)
- `airflow.cfg:sql_alchemy_conn` — метабаза Airflow `airflow_admin@10.10.1.142:5432/airflow`; файл локальный, не в git

### Имена ENV-переменных (только имена)
- В коде на `os.environ` / `os.getenv` явных обращений **не нашёл** — Airflow Variables и хардкод. Это плохой паттерн, упомянут в TODO.

### Git
- **Origin:** `git@github.com:Mussylman/ETL.git` (SSH)
- **Branch:** `main`
- **Последние коммиты (по `git log` в начале сессии):**
  - `e7bca45` Custom Python Transforms + dim/fact split + Kanban UI
  - `ac636f3` Code cleanup: remove dead code, merge duplicates, deprecate legacy
  - `90458bb` ETL Platform: initial commit

### Доступ к UI
- ETL Config App: `http://10.10.1.142:5556/` (PROD; TEST :5555 удалён)
- Airflow webserver: порт **по умолчанию 8080** на хосте `sev`, **в коде не подтверждено** — проверь `airflow.cfg` секцию webserver / api_server.

---

## 9. Глоссарий

### Бизнес-термины 1С (встречающиеся в коде)
| Термин 1С | Перевод / семантика | Где в коде |
|---|---|---|
| ЧекККМ | Кассовый чек ККМ (фискальный документ) | `_Document476`, `recorder_type_map` |
| РеализацияТоваровУслуг | Документ продажи (накладная) | `_Document316` (примерно) |
| ВозвратТоваровОтПокупателя | Возврат от клиента | `_Document254` |
| Регистратор | Документ-источник движения в регистре накопления | `_RecorderRRef`, `recorder` |
| _RecorderTRef | binary(4), тип регистратора (какой документ) | `binary_to_int`, `recorder_type_lookup` |
| _IDRRef | binary(16), UUID любого объекта 1С | `binary_to_uuid` |
| _Date_Time / _Period | datetime, год +2000 (4025=2025) | `fix_year` |
| Эврика_Бонусы | Поле бонусной программы «Эврика» | `bonuses` (custom_python) |
| Эврика_Списанные | Использованные бонусы | `used_bonuses_raw` |
| ВидОперации | Тип операции внутри документа (Продажа / Возврат для ЧекККМ) | `operation_type` |
| ОтправитьВАБМ | Флаг отправки в ABM (ABM-аналитическая система) | `send_to_abm` |
| ЭВРИКА_СуммаБонусами | Сумма документа в бонусах | `bonus_sum` |
| ДокументОснование | Документ-основание (FK на другой документ) | в коде ошибочно использован для kolichestvo в R21 — баг |
| Номенклатура | Товар / SKU | `product_id` |
| Подразделение | Внутреннее подразделение организации | `division_id` |
| Контрагент | Внешний контрагент (клиент/поставщик) | `partner_id` |
| Организация | Юрлицо | `org_id` |
| Склад | Складская единица | `warehouse_id` |
| ЗаказПокупателя | Заказ от клиента | `orders_id` |
| Ответственный | Сотрудник-ответственный | `document_responsible` |

### Внутренние сокращения
- **VT** — Tabular Part (табличная часть документа в 1С). В MSSQL имя `_Document{N}_VT{M}`. В API 1С — `Document{N}.VT{M}`.
- **dim / fact** — стандартные роли таблиц измерения / факта в звёздной схеме.
- **upsert / insert / replace** — режимы загрузки в `register_targets.load_mode`.
- **header / detail / standalone** — типы источника (`register_sources.source_type`):
  - `header` — шапка документа (`_Document*`)
  - `detail` — VT (табличная часть, `_Document*_VT*`)
  - `standalone` — регистр накопления / самостоятельная таблица (`_AccumRg*`)
- **include_columns** — массив имён колонок, попадающих в конкретный target (для split).
- **target_role** — `'dimension'` или `'fact'`.
- **recorder_type_lookup** — кастомная трансформация: binary(4)→int→название документа через `recorder_type_map`.
- **Discover** — операция в ETL Config App: `SELECT DISTINCT _RecorderTRef FROM _AccumRg*` → резолв имён через 1С API → массовое создание source-ов и VT-source-ов.

---

## 10. Открытые вопросы (для уточнения у Mussylman)

### Что в коде неочевидно
1. **Точная версия 1С (1.3.27.1) — где зафиксирована?** В коде имени БД (`UPP_JAN`) и контракта API хватает, но если будет миграция 1С — не понятно где обновлять.
2. **`gfk_client.GFK` — что внутри?** Внешний клиент, читал по поверхности — не уверен про авторизацию, ретраи, как формируется `reportId`.
3. **Schedule timezone** — все DAG-и используют `pendulum.timezone("Asia/Almaty")`, при этом cron-стринг в `check_orders_dag.py` комментирует «UTC». Несоответствие — `start_date=datetime(..., tzinfo=local_tz)` означает, что cron трактуется в Almaty. Стоит уточнить намеренно или баг.
4. **`recorder_type_map` структура** — JSONB на регистре. Видел только запись через ETL Config App. Как читается из ETLEngine при трансформе — нужно посмотреть в `transform_utils.py` подробнее (бегло — есть `_recorder_type_lookup`, ок).
5. **Кто запускает `etl_config_app` в проде?** systemd unit / supervisor / просто `uvicorn --reload`? В коде нет deployment-конфига.
6. **Airflow webserver/api_server** — порт, бэкенд (FAB?), пользователи? `apache-airflow-providers-fab` установлен, но конфиг RBAC не смотрел.

### Что стоит уточнить у Mussylman
1. Нужно ли DAG-обёртки для ETLEngine/SalesETL (триггерить выгрузки по расписанию через Airflow, не вручную через UI)?
2. План миграции credentials в env / Airflow Connections / Vault?
3. Структура `recorder_type_map` для регистров где несколько типов регистраторов с одинаковым int (теоретически возможно при многоконфигурационной 1С)?
4. Кто отвечает за бэкап `etl_meta` схемы?
5. Сценарий incident response (DAG упал → как узнаём)? Сейчас Telegram только в одном DAG-е.
6. Что с регистром 21 — пересоздавать с нуля или чинить руками?
7. Какие ещё бизнес-домены в планах кроме продаж и склада? (Закупки? Производство? Финансы?)

---
*Файл сгенерирован 2026-04-22 на основе фактического содержимого репозитория и истории разработки в `docs/`.*
