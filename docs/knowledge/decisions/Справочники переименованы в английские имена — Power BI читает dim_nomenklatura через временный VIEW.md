---
tags: [decision, dim, naming, postgresql, clickhouse, powerbi]
date: 2026-10-06
---

# Справочники переименованы в английские имена — Power BI читает dim_nomenklatura через временный VIEW

2026-10-06 (миграция `dags/core/migrations/clickhouse/018_dim_english_names*.sql`):

| было | стало |
|---|---|
| dim_nomenklatura | dim_product |
| dim_sklad | dim_warehouse |
| dim_podrazdelenie | dim_department |
| dim_kachestvo | dim_quality |
| dim_kontragent | dim_counterparty |
| dim_dogovor | dim_contract |
| dim_organizatsiya | dim_organization |
| dim_otvetstvennyy | dim_responsible_person |

Переименованы только таблицы (PostgreSQL — вместе с sequence, ограничениями, индексами; ClickHouse — реплики и
`_stage`). id, guid и данные не менялись; колонки фактов (`nomenklatura_id`, `sklad_id`, …) и ключи ссылок
`raw_refs.<ключ>` — прежние.

## Почему одной транзакцией и при паузе
Загрузчик находит справочник по имени: `refs.dim_links` — `transform_params.dim` или неявно `dim_<ключ>`, и только
если таблица существует. Иначе ссылка молча пропадала — факты получили бы `*_id = 0` без ошибки. Поэтому:
- все 38 ссылок `raw_refs` получили явный `transform_params.dim` (ключ ≠ имя справочника);
- `refs.dim_links` теперь падает, если явно названного справочника нет (раньше — тихо пропускал);
- rename PostgreSQL + перевод control plane (`registers`, `register_targets`, `ch_sync` core_pg_to_ch, lookup
  `cost_daily`, `post_load_sql` замороженного пути) — одна транзакция с пред/постусловиями;
- ClickHouse RENAME, права `etl_writer` — сразу после, при приостановленном `analytics_sync` (~3 мин, пропуск
  догнал patch).

Новая ссылка на справочник в конфигураторе — с `transform_params.dim` (имя таблицы больше не выводится из ключа).

## Power BI
`system.query_log`: Power BI (ODBC с 10.10.1.136 и Desktop с 192.168.18.233, пользователь `analytics_reader`) читает
`analytics_poc.dim_nomenklatura` (≈6 300 запросов за 6.5 суток: name, category, subcategory1/2, brand по
`nomenklatura_id` из `cost_daily`). Остальные справочники BI не читал (окно лога короткое — редкие отчёты могли не
попасть).

Поэтому в ClickHouse оставлен **TEMP VIEW** `analytics_poc.dim_nomenklatura AS SELECT * FROM dim_product`
(COMMENT 'TEMP compatibility…'): те же колонки и типы, `analytics_reader` читает его по праву на базу, ETL его не
использует. Реальный последний запрос Power BI выполнен через VIEW без изменений модели. В PostgreSQL alias нет.

**Удалить VIEW**, когда модель Power BI переведена на `dim_product`:
```sql
SELECT count(), max(event_time) FROM system.query_log
WHERE type = 'QueryFinish' AND has(tables, 'analytics_poc.dim_nomenklatura') AND event_time > <дата перевода>;
-- 0 → DROP VIEW analytics_poc.dim_nomenklatura
```

Откат rename — `018_dim_english_names.down.sql` (PostgreSQL одной транзакцией + обратный RENAME в ClickHouse,
перевыдача прав `ch_ddl --apply`), при приостановленном `analytics_sync`.

Связано: [[Инвентаризация таблиц PostgreSQL и ClickHouse — кандидатов на безусловное удаление нет 2026-10-06]].
