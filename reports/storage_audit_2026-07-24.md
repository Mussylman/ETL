---
tags: [audit, storage, postgres, sales]
date: 2026-07-24
---
# Storage-аудит: public.sales / public.sales_positions

## Окружение и честные оговорки
- Сервер оказался **PostgreSQL 14.23 (Ubuntu-пакет), НЕ 16 и НЕ Docker**: хост `sev` = 10.10.1.142, порт 5432 слушает системный PG. Docker на машине есть, но БД не в нём; `docker exec` в любом случае недоступен (пользователь не в группе docker, sudo под паролем) → все запросы через `psql` по TCP (эквивалентно для read-only).
- Креды: airflow_admin @ test (из Airflow connection `postgre_test_base`).
- Таблицы: `public.sales`, `public.sales_positions` (в задании было `sales_position` — уточнено через `\dt`).
- Статистика pg_stats свежая: autoanalyze 2026-07-23/24. Ручного ANALYZE не делал (read-only).
- `pgstattuple` не установлен, ставить нельзя (read-only) → bloat — только оценка по pg_stat_user_tables.
- Только SELECT: никаких DDL/DML/VACUUM/ANALYZE/индексов.
- Масштаб: обе таблицы вместе ~81 МБ — экономия измеряется мегабайтами, не ГБ. Все "% от heap" честнее абсолютных цифр.

## 1. Размеры таблиц

| Таблица | total | heap | индексы | toast | reltuples | count(*) точный |
|---|---:|---:|---:|---:|---:|---:|
| sales | 44.6 МБ | 29.1 МБ | 15.5 МБ | 0.0 МБ | 81,338 | 81,539 |
| sales_positions | 36.3 МБ | 25.8 МБ | 10.4 МБ | 0.0 МБ | 112,678 | 114,212 |

`sales_positions` больше `sales` в **0.8×** по total size (36.3 МБ vs 44.6 МБ) — оптимизация в первую очередь про неё.

## 2. Колонки по весу

### sales (строк: 81,539, heap 29.1 МБ)

| колонка | тип | attlen | align | not null | avg_width | n_distinct | null% | ~МБ | % heap |
|---|---|---:|---|---|---:|---:|---:|---:|---:|
| recorder | uuid | 16 | c |  | 16 | -1.0 | 0% | 1.24 | 4.3% |
| kontragent | uuid | 16 | c |  | 16 | 297.0 | 0% | 1.24 | 4.3% |
| podrazdelenie | uuid | 16 | c |  | 16 | 94.0 | 0% | 1.24 | 4.3% |
| otvetstvennyy_uid | uuid | 16 | c |  | 16 | 345.0 | 0% | 1.24 | 4.3% |
| zakaz_uid | uuid | 16 | c |  | 16 | -0.95116675 | 0% | 1.24 | 4.3% |
| doc_sale_uid | uuid | 16 | c |  | 16 | -0.9854066 | 0% | 1.24 | 4.3% |
| organizatsiya_uid | uuid | 16 | c |  | 16 | 1.0 | 0% | 1.24 | 4.3% |
| dogovor_uid | uuid | 16 | c |  | 16 | 315.0 | 0% | 1.24 | 4.3% |
| vidoperatsii | uuid | 16 | c |  | 16 | 5.0 | 0% | 1.24 | 4.3% |
| sklad | uuid | 16 | c |  | 16 | 63.0 | 2% | 1.22 | 4.2% |
| doc_number | character varying(255) | -1 | i |  | 12 | -0.9972461 | 0% | 0.93 | 3.2% |
| id | bigint | 8 | d | ✓ | 8 | -1.0 | 0% | 0.62 | 2.1% |
| period | timestamp without time zone | 8 | d | ✓ | 8 | -0.9685387 | 0% | 0.62 | 2.1% |
| updated_at | timestamp without time zone | 8 | d |  | 8 | 1206.0 | 0% | 0.62 | 2.1% |
| etl_loaded_at | timestamp without time zone | 8 | d |  | 8 | 186.0 | 0% | 0.62 | 2.1% |
| retail_snapshot_at | timestamp without time zone | 8 | d |  | 8 | 183.0 | 0% | 0.62 | 2.1% |
| etl_updated_at | timestamp without time zone | 8 | d |  | 8 | 186.0 | 0% | 0.62 | 2.1% |
| retail_updated_at | timestamp without time zone | 8 | d |  | 8 | -0.3633849 | 47% | 0.33 | 1.1% |
| recorder_type | integer | 4 | i |  | 4 | 4.0 | 0% | 0.31 | 1.1% |
| zakaz_type | integer | 4 | i |  | 4 | 2.0 | 0% | 0.31 | 1.1% |
| doc_sale_type | integer | 4 | i |  | 4 | 5.0 | 0% | 0.31 | 1.1% |
| is_posted | boolean | 1 | c |  | 1 | 1.0 | 0% | 0.08 | 0.3% |
| etl_hash | text | -1 | i |  | 0 | 0.0 | 100% | 0.00 | 0.0% |

### sales_positions (строк: 114,212, heap 25.8 МБ)

| колонка | тип | attlen | align | not null | avg_width | n_distinct | null% | ~МБ | % heap |
|---|---|---:|---|---|---:|---:|---:|---:|---:|
| recorder | uuid | 16 | c |  | 16 | -0.5618666 | 0% | 1.74 | 6.8% |
| nomenklatura | uuid | 16 | c |  | 16 | 5471.0 | 0% | 1.74 | 6.8% |
| etl_loaded_at | timestamp without time zone | 8 | d |  | 8 | 126.0 | 0% | 0.87 | 3.4% |
| sales_id | bigint | 8 | d |  | 8 | -0.5618666 | 0% | 0.87 | 3.4% |
| id | bigint | 8 | d | ✓ | 8 | -1.0 | 0% | 0.87 | 3.4% |
| retail_snapshot_at | timestamp without time zone | 8 | d |  | 8 | 124.0 | 0% | 0.87 | 3.4% |
| etl_updated_at | timestamp without time zone | 8 | d |  | 8 | 126.0 | 0% | 0.87 | 3.4% |
| nds | numeric(18,4) | -1 | i |  | 7 | 9970.0 | 0% | 0.76 | 3.0% |
| stoimost | numeric(18,4) | -1 | i |  | 6 | 10718.0 | 0% | 0.65 | 2.5% |
| summa | numeric(18,4) | -1 | i |  | 6 | 10149.0 | 0% | 0.65 | 2.5% |
| tsena | numeric(18,4) | -1 | i |  | 6 | 6579.0 | 0% | 0.65 | 2.5% |
| stoimost_bez_skidok | numeric(18,4) | -1 | i |  | 6 | 7515.0 | 0% | 0.65 | 2.5% |
| retail_updated_at | timestamp without time zone | 8 | d |  | 8 | -0.23432258 | 50% | 0.44 | 1.7% |
| kolichestvo | numeric(18,4) | -1 | i |  | 4 | 69.0 | 0% | 0.44 | 1.7% |
| nomerstroki | numeric(18,4) | -1 | i |  | 4 | 39.0 | 0% | 0.44 | 1.7% |
| line_no | integer | 4 | i |  | 4 | 38.0 | 0% | 0.44 | 1.7% |
| recorder_type | integer | 4 | i |  | 4 | 4.0 | 0% | 0.44 | 1.7% |
| akciz | numeric(18,4) | -1 | i |  | 3 | 1.0 | 0% | 0.33 | 1.3% |
| summands | numeric(18,4) | -1 | i |  | 3 | 1142.0 | 1% | 0.32 | 1.3% |
| evrika_bonusy | numeric(18,4) | -1 | i |  | 3 | 504.0 | 1% | 0.32 | 1.3% |
| evrika_spisannye | numeric(18,4) | -1 | i |  | 3 | 110.0 | 1% | 0.32 | 1.3% |
| sklad | uuid | 16 | c |  | 16 | 57.0 | 95% | 0.10 | 0.4% |
| kachestvo | uuid | 16 | c |  | 16 | 4.0 | 95% | 0.10 | 0.4% |
| etl_hash | text | -1 | i |  | 0 | 0.0 | 100% | 0.00 | 0.0% |
| otvetstvennyy | text | -1 | i |  | 0 | 0.0 | 100% | 0.00 | 0.0% |

## 3. UUID-колонки

| таблица | колонка | distinct | NULL | пустая ссылка 1С | ~МБ | кандидат |
|---|---|---:|---:|---:|---:|---|
| sales_positions | recorder | 81,539 | 0 | 0 | 1.74 | int4 |
| sales_positions | nomenklatura | 9,453 | 0 | 0 | 1.74 | int4 |
| sales | recorder | 81,539 | 0 | 0 | 1.24 | int4 |
| sales | kontragent | 568 | 0 | 0 | 1.24 | smallint (distinct<1000) |
| sales | podrazdelenie | 103 | 0 | 5 | 1.24 | smallint (distinct<1000) |
| sales | otvetstvennyy_uid | 375 | 0 | 36,633 | 1.24 | smallint (distinct<1000) |
| sales | zakaz_uid | 79,428 | 0 | 132 | 1.24 | int4 |
| sales | doc_sale_uid | 81,017 | 0 | 0 | 1.24 | int4 |
| sales | organizatsiya_uid | 1 | 0 | 0 | 1.24 | smallint (distinct<1000) |
| sales | dogovor_uid | 629 | 0 | 1 | 1.24 | smallint (distinct<1000) |
| sales | vidoperatsii | 5 | 7 | 0 | 1.24 | smallint (distinct<1000) |
| sales | sklad | 65 | 1,351 | 127 | 1.22 | smallint (distinct<1000) |
| sales_positions | sklad | 64 | 108,125 | 0 | 0.10 | smallint (distinct<1000) |
| sales_positions | kachestvo | 5 | 108,125 | 0 | 0.10 | smallint (distinct<1000) |

**sales**: uuid-колонок 10, суммарно ~12.4 МБ (43% heap).

**sales_positions**: uuid-колонок 4, суммарно ~3.7 МБ (14% heap).

## 4. UUID, хранящиеся текстом

Не найдено: все text/varchar-колонки не похожи на uuid (проверен сэмпл 5000 значений каждой).

## 5. Связь sales_positions → sales и дублирование колонок

FK-констрейнты sales_positions: **нет** (связь логическая)

Общие колонки (есть в обеих): etl_hash, etl_loaded_at, etl_updated_at, recorder, recorder_type, retail_snapshot_at, retail_updated_at, sklad

Вес дублей в sales_positions (эти данные уже есть в sales, доступны через join по sales_id):

| колонка | ~МБ в positions | комментарий |
|---|---:|---|
| etl_hash | 0.00 | служебная |
| etl_loaded_at | 0.87 | аудит |
| etl_updated_at | 0.87 | аудит |
| recorder | 1.74 | ключ upsert — нужен |
| recorder_type | 0.44 | ключ upsert — нужен |
| retail_snapshot_at | 0.87 | аудит |
| retail_updated_at | 0.44 | аудит |
| sklad | 0.10 | в sales тоже есть — потенциальный дубль |
| **итого** | **5.33** | из них ключи (recorder+recorder_type) — обоснованный дубль |

## 6. Alignment padding (оценка)

**sales**: данные строки сейчас ≈240.0 Б, при идеальном порядке (8→4→2→1→varlena) ≈241.0 Б → потеря ≈-1.0 Б/строку ≈ **-0.1 МБ** на таблицу. (+23 Б заголовок строки и 24 Б заголовок страницы — не меняются)
**sales_positions**: данные строки сейчас ≈144.0 Б, при идеальном порядке (8→4→2→1→varlena) ≈138.9 Б → потеря ≈5.1 Б/строку ≈ **0.6 МБ** на таблицу. (+23 Б заголовок строки и 24 Б заголовок страницы — не меняются)

⚠️ Оценка: NULL-битмапы, short-varlena-заголовки и fillfactor не моделируются точно.

## 7. Экономия от uuid → int4

| таблица | uuid сейчас, МБ | после int4, МБ | экономия МБ | % от heap |
|---|---:|---:|---:|---:|
| sales | 12.4 | 3.1 | 9.3 | 32% |
| sales_positions | 3.7 | 0.9 | 2.8 | 11% |

Индексы, содержащие uuid-колонки (ключевая часть сожмётся ~в 4 раза, страничный overhead останется — оценка):

- `sales_recorder_key` (sales): 4.0 МБ, колонки ['recorder']
- `sales_recorder_recorder_type_key` (sales): 4.9 МБ, колонки ['recorder', 'recorder_type']
- `sales_positions_recorder_recorder_type_line_no_key` (sales_positions): 6.1 МБ, колонки ['recorder', 'recorder_type', 'line_no']

## 8. Индексы

| индекс | таблица | размер МБ | idx_scan | определение |
|---|---|---:|---:|---|
| sales_recorder_recorder_type_key | sales | 4.9 | 482,528 | `btree (recorder, recorder_type)` |
| sales_recorder_key | sales | 4.0 | 1,217,554 | `btree (recorder)` |
| sales_pkey | sales | 3.5 | 998 | `btree (id)` |
| idx_sales_retail_updated_at | sales | 1.8 | 8,458 | `btree (retail_updated_at) WHERE (retail_updated_at IS NOT NULL)` |
| idx_sales_retail_snapshot_at | sales | 1.2 | 9 | `btree (retail_snapshot_at) WHERE (retail_snapshot_at IS NOT NULL)` |
| sales_positions_recorder_recorder_type_line_no_key | sales_positions | 6.1 | 582,939 | `btree (recorder, recorder_type, line_no)` |
| sales_positions_pkey | sales_positions | 4.3 | 18 | `btree (id)` |

Дубли по префиксу колонок:
- sales: индексы ['sales_recorder_recorder_type_key', 'sales_recorder_key'] начинаются с одной колонки `recorder` — возможен лишний

## 9. Bloat (ОЦЕНКА — pgstattuple не установлен)

| таблица | dead tuples | live tuples | dead % |
|---|---:|---:|---:|
| sales_positions | 17,227 | 114,212 | 13.1% |
| sales | 10,761 | 81,539 | 11.7% |

Точный bloat требует `CREATE EXTENSION pgstattuple` (нужны права суперпользователя) — не делал (read-only).

## Итог

- **sales**: сейчас 44.6 МБ → после uuid→int4 ≈35.3 МБ → после переупорядочивания ≈35.4 МБ (без учёта сжатия индексов)
- **sales_positions**: сейчас 36.3 МБ → после uuid→int4 ≈33.5 МБ → после переупорядочивания ≈33.0 МБ (без учёта сжатия индексов)

## Поправка к разделу 1
Вопреки ожиданию, **sales БОЛЬШЕ sales_positions** (44.6 МБ vs 36.3 МБ при меньшем числе строк: 81.5k vs 114.2k). Причина — 10 uuid-колонок в sales (43% её heap) против 4 в positions (14%). Оптимизация в первую очередь про **sales**.

## Находки по убыванию выигрыша

1. **UUID-колонки в sales — 43% heap (12.4 МБ)**. uuid→int4/smallint экономит 9.3 МБ heap (32%) + сожмёт uuid-индексы (8.9 МБ ключевой массы: sales_recorder_key 4.0 + composite 4.9). 8 из 10 колонок — кандидаты на **smallint** (distinct < 1000: организация=1, вид операции=5, склад=65, подразделение=103, контрагент=568, договор=629, ответственный=375). НО: при нынешнем масштабе главный аргумент за int-ключи — не диск, а **модель Power BI** (лёгкие связи) — это и есть план «гибрид C».
2. **organizatsiya_uid: distinct = 1** — константа занимает 1.24 МБ + место в каждой строке. В однofirмenной базе колонка не нужна в fact вообще (вынести в метаданные/справочник).
3. **Мёртвые колонки (100% NULL)**: `etl_hash` в обеих таблицах, `otvetstvennyy` (text) в positions — не заполняются движком вовсе. Вес ~0, но мусор в схеме и в каждом NULL-битмапе. Кандидаты на DROP + чистку mappings.
4. **otvetstvennyy_uid: 36 633 строки (45%) с пустой ссылкой 1С** `00000000-...` — стоило бы нормализовать в NULL (сейчас «пустышка» весит полные 16 Б и попадает в индексы/статистику как значение).
5. **Дублирование в positions: 5.33 МБ**, из них обоснованные ключи upsert (recorder+recorder_type) 2.18 МБ; аудит-колонки (etl_loaded_at/etl_updated_at/retail_snapshot_at/retail_updated_at) 3.05 МБ — используются валидацией, но per-row в fact избыточны, можно обсудить хранение только в sales. `sklad` дублируется лишь на 5% строк (VT-строки) — это не дубль, а уточнение со строки документа.
6. **Индекс sales_recorder_key (4.0 МБ) — префикс-дубль** composite `(recorder, recorder_type)`. Оба активно используются (1.2M и 0.5M сканов), но composite покрывает запросы по recorder → одиночный можно дропнуть, экономия 4.0 МБ. Проверить планы перед дропом.
7. **idx_sales_retail_snapshot_at: 9 сканов** за всё время (1.2 МБ) — почти не используется, НО это fallback watermark'а (нужен редко, зато критично). Оставить, помечен для понимания.
8. **Alignment padding — копейки**: sales уже почти оптимальна (≈0 Б/строку), positions теряет ≈5 Б/строку ≈ 0.6 МБ. Переупорядочивание не окупает миграцию.
9. **Bloat ~12-13% dead tuples** в обеих — норма для upsert-таблиц с 5-минутным циклом, autovacuum справляется. Точный замер — только после установки pgstattuple (нужен суперпользователь).

## Итог одной строкой
Хранение сейчас не болит (81 МБ суммарно). Реальная ценность uuid→int — производительность и удобство Power BI-модели, и это ровно план «DWH гибрид (вариант C)»: справочники dim_* + int-ключи + витрины bi.*. Побочно почистить: etl_hash/otvetstvennyy (мёртвые), organizatsiya_uid (константа), пустышки 1С → NULL, один префикс-дубль индекса.
