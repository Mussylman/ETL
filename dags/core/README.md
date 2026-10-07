# Core ETL Module

Универсальная ETL-система для загрузки данных из 1С (MSSQL) в PostgreSQL.

## Архитектура

```
core/
├── etl_engine.py           # Главный движок (конфигурируемый)
├── etl_core.py             # Legacy движок (hardcoded)
│
├── config/                 # Конфигурация
│   ├── models.py           # Dataclasses
│   └── config_loader.py    # Загрузка из PostgreSQL
│
├── builder/                # Генерация SQL
│   └── query_builder.py    # JOIN, UNION ALL
│
├── extract/                # Извлечение данных
│   ├── storage_connector.py
│   └── data_checker.py
│
├── transform/              # Трансформация
│   └── transform_utils.py
│
├── load/                   # Загрузка
│   └── loaders.py
│
└── migrations/             # SQL-миграции
    ├── 001_create_etl_meta_schema.sql
    ├── 002_sample_sales_register.sql
    └── 003_stock_register.sql
```

## Быстрый старт

### 1. Применить миграции

```sql
-- PostgreSQL
\i migrations/001_create_etl_meta_schema.sql
\i migrations/002_sample_sales_register.sql  -- пример
```

### 2. Запустить ETL

```python
from core import ETLEngine

# Полная загрузка
etl = ETLEngine(
    register_code="sales_register",
    mode="full_period",
    start_date="4025-10-01",
    end_date="4025-11-01"
)
etl.run()

# Инкрементальная загрузка
etl = ETLEngine(
    register_code="sales_register",
    mode="incremental"
)
etl.run()
```

## Режимы работы

| Режим | Описание | Параметры |
|-------|----------|-----------|
| `full_period` | Полная загрузка по периоду | `start_date`, `end_date` |
| `incremental` | По изменениям в retail | `retail_table`, `retail_uid_column` |
| `consistency` | Проверка консистентности | — |

## Конфигурация в PostgreSQL

### Схема `etl_meta`

| Таблица | Назначение |
|---------|------------|
| `registers` | Регистры (sales, stock) |
| `register_sources` | Источники (таблицы 1С) |
| `column_mappings` | Маппинг колонок |
| `source_unions` | UNION-объединения |
| `source_union_members` | Члены UNION |
| `register_targets` | Целевые таблицы |
| `load_history` | История загрузок |

### Пример: добавление нового регистра

```sql
-- 1. Регистр
INSERT INTO etl_meta.registers (code, name, default_mode)
VALUES ('my_register', 'Мой регистр', 'full_period');

-- 2. Источник
INSERT INTO etl_meta.register_sources
    (register_id, source_code, source_type, mssql_table)
SELECT id, 'main', 'standalone', '_AccumRg12345'
FROM etl_meta.registers WHERE code = 'my_register';

-- 3. Маппинг колонок
INSERT INTO etl_meta.column_mappings
    (source_id, source_column, target_column, transform_type)
SELECT s.id, '_IDRRef', 'uid', 'binary_to_uuid'
FROM etl_meta.register_sources s WHERE s.source_code = 'main';

-- 4. Целевая таблица
INSERT INTO etl_meta.register_targets
    (register_id, target_table, source_id, load_mode, upsert_keys)
SELECT r.id, 'my_table', s.id, 'upsert', ARRAY['uid']
FROM etl_meta.registers r, etl_meta.register_sources s
WHERE r.code = 'my_register' AND s.source_code = 'main';
```

## Компоненты

### ETLEngine

Главный оркестратор. Читает конфигурацию из PostgreSQL.

```python
ETLEngine(
    register_code="sales_register",  # код регистра
    mode="full_period",              # режим
    start_date="4025-10-01",         # начало периода
    end_date="4025-11-01",           # конец периода
    target_tables=["sales_daily"],   # конкретные таблицы (опционально)

    # Connections — только явно (значения по умолчанию нет, см. core/conn.py)
    config_conn_id="etl_prod",
    src_conn_id="mssql_1c_conn",
    dst_conn_id="etl_prod",
    retail_conn_id="bd_retail",
)
```

### QueryBuilder

Генерирует SQL-запросы из конфигурации.

```python
from core import QueryBuilder

qb = QueryBuilder(database="UPP_JAN")

# Простой SELECT
sql = qb.build_source_query(source, period_start="4025-10-01")

# UNION ALL
sql = qb.build_union_query(union_config, register)
```

**Поддержка:**
- JOIN header ↔ details
- UNION ALL нескольких источников
- Фильтрация по периоду и ключам

### StorageConnector

Загрузка данных из MSSQL (1С).

```python
from core import StorageConnector

conn = StorageConnector(src_conn_id="mssql_1c_conn")

# Произвольный SQL
df, binary_cols = conn.execute_query("SELECT * FROM ...")

# Legacy: загрузка таблицы
df, binary_cols = conn.load_table("_AccumRg17844", start_date="4025-10-01")
```

### TransformUtils

Трансформация данных.

```python
from core import TransformUtils

t = TransformUtils()

# Автоматическая трансформация
df = t.transform_dataframe(df, binary_columns)

# По конфигурации
df = t.transform_dataframe_by_config(df, binary_columns, {
    "document_uid": {"type": "binary_to_uuid"},
    "document_date": {"type": "fix_year"},
})
```

**Трансформации:**

| Тип | Описание |
|-----|----------|
| `binary_to_uuid` | binary(16) → UUID |
| `binary_to_int` | binary(4) → int |
| `binary_to_bool` | binary(1) → bool |
| `fix_year` | 4025 → 2025 |
| `cast` | Приведение типа |
| `constant` | Константное значение |

### Loaders

Загрузка в PostgreSQL.

```python
from core import Loaders

loader = Loaders(dst_conn_id="etl_prod")

# INSERT
loader.insert_only(df, "my_table")

# UPSERT (один ключ)
loader.upsert_by_key(df, "my_table", "uid")

# UPSERT (составной ключ)
loader.upsert_by_keys(df, "my_table", ["doc_uid", "line_no"])

# REPLACE (DELETE + INSERT)
loader.replace_all(df, "my_table")

# Универсальный метод
loader.load(df, "my_table", mode="upsert", upsert_keys=["uid"])
```

## Типы источников

### standalone
Одна таблица без JOIN.

```sql
INSERT INTO etl_meta.register_sources
    (register_id, source_code, source_type, mssql_table)
VALUES (1, 'main', 'standalone', '_AccumRg17844');
```

### header + detail
Две таблицы с JOIN.

```sql
-- Header
INSERT INTO etl_meta.register_sources
    (register_id, source_code, source_type, mssql_table)
VALUES (1, 'doc_h', 'header', '_Document394');

-- Detail (ссылается на header)
INSERT INTO etl_meta.register_sources
    (register_id, source_code, source_type, mssql_table,
     parent_source_id, join_type, join_key_source, join_key_parent)
VALUES (1, 'doc_d', 'detail', '_Document394_VT17845',
        <header_id>, 'INNER', '_Document394_IDRRef', '_IDRRef');
```

### UNION ALL
Объединение нескольких источников.

```sql
-- Создаём UNION
INSERT INTO etl_meta.source_unions
    (register_id, union_code, output_columns)
VALUES (1, 'all_docs', ARRAY['uid', 'date', 'qty']);

-- Добавляем источники
INSERT INTO etl_meta.source_union_members (union_id, source_id, priority)
VALUES (1, <source1_id>, 1), (1, <source2_id>, 2);
```

## Pre-load SQL

Агрегация данных перед загрузкой.

```sql
INSERT INTO etl_meta.register_targets
    (register_id, target_table, source_id, load_mode, upsert_keys, pre_load_sql)
VALUES (
    1,
    'sales_daily',
    1,
    'upsert',
    ARRAY['date', 'store_id'],
    '
    SELECT
        DATE(document_date) AS date,
        store_id,
        SUM(quantity) AS total_qty
    FROM __df__
    GROUP BY DATE(document_date), store_id
    '
);
```

`__df__` заменяется на DataFrame.

## Airflow Connections

| Connection ID | Тип | Назначение |
|---------------|-----|------------|
| `mssql_1c_conn` | MSSQL | 1С ERP |
| `etl_prod` | PostgreSQL | control plane (`etl_meta`) + реестры (`doc_key`, `dim_*`) |
| `clickhouse_etl` | ClickHouse | аналитический слой — факты и реплики справочников |
| `bd_retail` | PostgreSQL | retail — сигнал об изменениях |

Отдельной тестовой PostgreSQL-базы нет. Запись фактов в PostgreSQL выключена (`registers.pg_fact_write =
false`), таблиц фактов там нет — примеры `ETLEngine`/`Loaders` выше относятся к старому пути.

## Legacy: ETLCore

Старый движок с hardcoded конфигурацией.

```python
from core import ETLCore

etl = ETLCore(
    mssql_table="_AccumRg17844",
    target_table="sales_register",
    mode="full_period",
    start_date="4025-10-01",
    end_date="4025-11-01",
)
etl.run()
```

Используй `ETLEngine` для новых регистров.
