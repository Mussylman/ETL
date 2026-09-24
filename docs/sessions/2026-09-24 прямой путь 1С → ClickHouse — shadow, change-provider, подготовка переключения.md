---
tags: [session, clickhouse, direct-path, cutover]
date: 2026-09-24
---

# 2026-09-24 прямой путь 1С → ClickHouse — shadow, change-provider, подготовка переключения

## Сделано
- **Change-provider** различает ready / pending / deleted
  ([[Сигнал retail не значит, что документ готов в 1С — change-provider различает ready, pending и deleted]]).
  Серия ожидающих циклов остановлена по новому правилу проекта: внешние события не ждём —
  один цикл, pending фиксируется, дальше.
- **Shadow продаж**: реальные циклы, rebuild 202609 подобрал ЧекККМ без сигнала retail.
- **Shadow заказов**: backfill 202602–202609, 10 одиночных документов до начала истории режимом
  `keys`; прямой путь = 1С
  ([[Shadow заказов прямого пути совпадает с 1С — расхождения со старым путём это его отставание 2026-09-24]]).
- **Сверка 1С ↔ ClickHouse** обобщена на любой корень (шапка документа, ТЧ через union).
- **К переключению**: реестр документов (013), `pg_fact_write`, единый DAG `analytics_sync` (на паузе,
  без задач), реестр групп (014), условный триггер `clickhouse_sync`, `core.tools.ch_cutover`
  (план проверен). Решение: [[Прямой путь 1С → ClickHouse — переключение без двух выдающих id и без потери ссылок]].

## Найдено
- Retail сигналит не обо всех ЧекККМ (сентябрь: 12 из 39 092) — закрывает только hot-пересборка.
- Заказы правят в 1С после создания, retail об этом не сигналит — старый путь отстаёт.
- Отпечаток сверки зависит от реплик `dim_*` в ClickHouse — справочники публиковать до фактов.

## Ждёт решения
Переключение останавливает запись `public.sales` / `public.orders` в PostgreSQL. Нужно подтвердить,
что внешних читателей этих таблиц нет (ранее виден клиент 192.168.18.233 под airflow_admin), и
передать `ch_admin`-конфиг для `EXCHANGE TABLES`.

## Коммиты
bf842c2, fdc3f7b, 700fe70, 08475d5, ae48e4b, b063851, e89178c, 2ebd363 (+3ef8197).

## Переключение (17:13 Almaty)
`ch_cutover --register order,sales --skip-history --apply` — PASS. Боевые факты ClickHouse идут
из 1С напрямую (`onec_1c` в `analytics_sync`), копии для отката `fact_*_direct`, второй hop по
4 фактам выключен (`legacy_frozen`). PostgreSQL и `incremental_prod` работают как раньше — они
всё ещё выдают id документов (issuer `pg_facts`); снятие зависимости — следующий шаг.
Прогон `analytics_sync` 12:15 UTC упал «Task not found» — он был создан по версии DAG до смены
группы (`sync__shadow_1c` → `sync__onec_1c`); данных не касался.
Admin-конфиг ClickHouse перенесён в `~/.config/clickhouse/ch_admin.xml` (600).

## Передача id реестру и заморозка PG-фактов (~17:55 Almaty)
- `etl_meta.doc_key` засеян из `public.sales` (6 042 653) и `public.orders` (462 589) с сохранением id;
  guid → id и id → guid однозначны, общих guid у областей нет.
- `ch_pg_handover --register order,sales --apply`: pg_fact_write=false, issuer=registry, справочники
  и cost_daily → analytics_sync, clickhouse_sync без групп (на паузе), триггер из incremental_prod исчез.
- Первый цикл: 42 новых заказа и 49 продаж получили id из реестра, в PG их нет; zakaz_id 49/50.
- `analytics_sync` — одна задача `sync`, группы читаются в момент выполнения.
- Запись фактов переключённого регистра в PG запрещена в `ETLEngine` (run, reload_documents).
- Удалены заменённые: UI-плагин etl_meta (Airflow 2), загрузчики TEST sales/dim_incremental,
  POC ch_load_sales_positions. clickhouse_sync оставлен как путь отката.
- Найдено: `orders_check_dag.py` не импортируется (нет модуля helpers — с initial commit);
  в 1С есть проведённые заказы с датами 2080–2081 (мусор данных).
