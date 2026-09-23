-- Права учёток ClickHouse. Выполняется от ch_admin.
-- Файл отражает ФАКТИЧЕСКОЕ состояние сервера (проверяется через SHOW GRANTS ниже).
--
-- Принцип: загрузчик не умеет ничего, кроме того, что ему нужно для замены партиции.
-- CREATE DATABASE / CREATE TABLE / DROP TABLE / ALTER UPDATE ему не выдаются,
-- GRANT POSTGRES не выдаётся никому: ClickHouse в PostgreSQL не ходит, данные
-- приносит внешний загрузчик.

-- ---------------------------------------------------------------- etl_writer
-- рабочая учётка загрузки; ch_admin в ETL не используется
GRANT SELECT, INSERT ON analytics_poc.* TO etl_writer;

-- очистка staging между месяцами — только staging, не целевая таблица
GRANT TRUNCATE             ON analytics_poc.fact_sales_positions_stage TO etl_writer;

-- MOVE PARTITION TO TABLE проверяет привилегию на ИСТОЧНИКЕ, то есть на staging.
-- Этой ветки достаточно, когда целевая партиция пуста (первая загрузка месяца).
GRANT ALTER MOVE PARTITION ON analytics_poc.fact_sales_positions_stage TO etl_writer;

-- REPLACE PARTITION требует INSERT + ALTER DELETE на ЦЕЛЕВОЙ таблице.
-- Нужен для перезаливки уже заполненного месяца — открытый месяц пересобирается
-- каждый запуск. Выдан точечно на одну таблицу, не на базу.
GRANT ALTER DELETE ON analytics_poc.fact_sales_positions TO etl_writer;

-- Намеренно НЕ выдано (проверяется негативными тестами):
--   DROP TABLE, DROP DATABASE, CREATE TABLE, CREATE DATABASE,
--   ALTER UPDATE, ALTER ADD/DROP COLUMN, ACCESS MANAGEMENT, OPTIMIZE
-- ALTER MOVE PARTITION на fact_sales_positions отозван как избыточный:
--   штатный путь перезаливки — REPLACE, а не MOVE в целевую таблицу.

-- ----------------------------------------------------------- analytics_reader
-- учётка BI (Power BI / DataGrip). Только чтение.
-- readonly = 2, а не 1: ODBC-драйверу нужно менять сессионные настройки
-- (иначе падает на cast_keep_nullable). Запись закрыта грантами, не readonly,
-- поэтому послабление readonly её не открывает.
-- Ресурсные потолки закреплены через MAX — учётка может лимиты понизить, но не поднять.
GRANT SELECT ON analytics_poc.* TO analytics_reader;
-- ALTER USER analytics_reader SETTINGS
--     max_memory_usage = 2147483648 MAX 2147483648,
--     max_threads      = 4          MAX 4,
--     readonly         = 2;
-- HOST LOCAL, IP '192.168.18.233' (рабочий ПК), '10.10.1.0/24'

SHOW GRANTS FOR etl_writer;
SHOW GRANTS FOR analytics_reader;
