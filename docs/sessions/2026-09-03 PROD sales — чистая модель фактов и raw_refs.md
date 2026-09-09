---
tags: [session, prod, sales, raw_refs, dim-layer]
date: 2026-09-03
---

# 2026-09-03 — PROD sales: чистая модель фактов и стандарт raw_refs

## Что сделано
- Зафиксирован стандарт ссылок: [[Ссылки в фактах — BIGINT id для BI и raw_refs JSONB для исходных GUID]].
- Движок: `_pack_raw_refs` (мэппинги `raw_refs.*` → один JSONB), валидатор на `raw_refs ? key`, `_normalize_json` в loader, `raw_refs` в SYSTEM QueryBuilder/Sync, legacy `updated_at` пишется только если он в include_columns.
- PROD `etl_meta` (register 62): 63 мэппинга перенацелены в `raw_refs.*`, 48 мёртвых удалены, include_columns и post_load_sql переписаны на resolve из raw_refs.
- DDL PROD: из `sales` удалены 14 колонок (uuid-дубли, `doc_sale_uid/type`, `zakaz_uid/type`, `vidoperatsii`, `etl_hash`, `updated_at`, `is_posted`), из `sales_positions` — 6. Остались: `sales` 18 колонок, `sales_positions` 23.
- Доказано на одном дне (2026-09-02), затем TRUNCATE фактов (DIM не трогали) и полный reload 2026-03-01..2026-09-04 по месяцам + incremental + DIM incremental (8 справочников).

## Итог проверки после reload
- 315 415 док / 435 650 позиций, raw_refs заполнен в 100 % строк, дублей NK 0, `sales_id` NULL 0.
- 9 FK: unresolved 0, dangling 0; stub во всех DIM 0 после stub-pass.
- Агрегаты по recorder_type (254/352/415/476: docs, positions, stoimost, nds, bez_skidok) = 1С в ноль; по дням — все закрытые дни сходятся.
- `id` существовавших guid не изменились; новых строк DIM 5 (3 kontragent, 2 dogovor) — созданы stub'ом post_load, заполнены stub-pass'ом, каждая используется фактами.
- `doc_sale_id`: 310 597 разрешено (309 824 self, 773 cross); 4 818 только в raw_refs — все возвраты на документы вне DWH (353 — 4 685; реализации до 2026-03-01 — 123; чеки, помеченные на удаление — 4). Аномалий нет.
- Вердикт: **SALES CLEAN FACT MODEL READY**.

## Не сделано / дальше
- Commit не делался по указанию (изменения: `etl_engine.py`, `loaders.py`, `query_builder.py`, `dao.py`, `sales_recon.py --dbname`).
- PROD incremental DAG — только дизайн (фабрика `build_incremental_dag(dag_id, config_conn_id)` → `incremental_prod` на `etl_prod`, paused при создании).
- Следующие регистры (order/`_Document271`) — после включения PROD инкремента.
- Мелочь: `load_history.rows_extracted` для full_period остаётся NULL (движок пишет только rows_loaded).

## Этап 2 (тот же день): commit, PROD incremental, аудит order
- Commit `6b53d4f` `feat(etl): finalize clean fact reference model` — только 5 файлов модели.
- `dags/incremental_dag.py` переписан в фабрику `build_incremental_dag(dag_id, config_conn_id, ...)`: `incremental` (TEST, postgre_test_base, task_id не изменились, unpaused как был) и `incremental_prod` (etl_prod, `*/5`, paused при создании). Runner'ы получают conn_id параметрами; `dst_conn_id = config_conn_id`.
- Discovery PROD: sales + 8 reference_dim; `unsupported_report` в PROD не создаётся — в `etl_prod` нет неподдерживаемых сущностей (`order` есть только в TEST).
- Ручной run (`airflow dags test`, paused): 9/9 success, 45 с; sales 62 док / 80 позиций; все проверки зелёные. Единственный нюанс — stub в dim_dogovor от sales, созданный после stub-pass договоров в том же прогоне (порядок тасок произвольный); заполнился следующим тиком.
- Unpause 11:52 UTC → первый scheduled run (слот 11:50) success за 16 с, load_history 69–77 success. **PROD SALES LIVE.**
- Аудит `order` (`_Document271`): см. отчёт в чате; главный вывод — TEST-конфиг это Union 5 документов над `_AccumRg16915` со старым uuid-стандартом и 19 фиктивными колонками из meta API ([[Meta API 1С отдаёт номера полей другой базы — для новых реквизитов колонки не совпадают с UPP_JAN]]); в PROD регистра нет; retail-привязка возможна через `orders.document_uid` (проверено 200/200 на возрасте 2–40 дней).
- Не закоммичено: `dags/incremental_dag.py` (фабрика) — по указанию пользователя коммитили только sales.

## Этап 3: дизайн PROD register order (до full load, ждёт согласования)
- Модель: `_Document271` (posted, не marked) → `orders`; `_VT4708` Товары ∪ `_VT4746` Услуги → `order_positions` с `vt_kind` ('product'|'service'), NK `(recorder, recorder_type, vt_kind, line_no)`. Имя шапки `orders` — `order` зарезервировано в SQL, движок пишет `public.{table}` без кавычек.
- Пайплайн `document_with_vt` = generic-путь QueryBuilder (source для шапки + union двух detail с INNER JOIN на родителя); ядро не меняется, DataChecker (окно + tail 15 суток) переиспользуется как есть с retail `orders.document_uid`.
- Скрипты: scratchpad `prod_order_ddl.sql`, `prod_order_etl_meta.sql` (27 + 15 + 10 мэппингов, все колонки проверены в UPP_JAN). Сухой прогон SQL за 2026-09-02: 2 128 шапок, 2 927 позиций, Σ позиций = Σ шапок = 516 096 795.
- По данным подтверждены: `_Fld24518RRef` Качество, `_Fld24668RRef`/`_Fld24669RRef` Ответственный, `_Fld24721RRef` СкладОтгрузки, `_Fld26962`/`_Fld26963` Эврика_Бонусы/Списанные (только товары). Исключены как пустые: акциз, Эврика_Предоплата, ДатаОтгрузки, автоскидки, МетодОплаты, УсловиеПродаж, ДокументОснование; PII (телефон, ФИО, ИИН, дата рождения) не берём.
- Неизвестные ТЧ: `_VT25124` — оплаты заказа (Эврика_СпособыОплаты, IdTerminal, сумма, № транзакции); `_VT26994` — акции по строкам (Номенклатура, Эврика_ВидыОперацииАкции). Не позиции, на потом.
- Предпосылки перед загрузкой: фикс NaN→NULL в loader ([[NULL-меры из COALESCE попадают в витрину как numeric NaN и ломают SUM]]), FK-имя в валидаторе (`orders`→`order_id`), runner для `document_with_vt` в DAG.

## Этап 4: реализация order на PROD (LIVE)
- Loader: generic `_normalize_nulls` (NaN/±inf/NaT/Decimal NaN → NULL) + фикс `_normalize_datetimes` (NaT снова становился NaT после apply). Юнит-тест через mogrify и TEMP-таблицу.
- Валидатор: FK позиций ищется как `{dim}_id`, затем без `s` (`orders` → `order_id`).
- LIVE sales: `UPDATE` NaN → NULL в 5 колонках `sales_positions` (408 487 строк); после — NaN 0 по всем 10 numeric, суммы без изменений, сверка с 1С в ноль.
- `orders`/`order_positions` созданы; metadata регистр 77 (52 мэппинга + явный NULL `raw_refs.sklad` у услуг). Контрольный день 2026-09-02: 2 128 / 2 927, Σ позиций = Σ шапок.
- История 2026-02-01..2026-09-04 по месяцам (~55 мин), все PASSED; сверка с 1С в ноль. Runner `document_with_vt` добавлен в `incremental_dag.py`; первые scheduled-инкременты order success. См. [[Регистр order строится от шапки документа с UNION табличных частей, а не от AccumRg]].
- Не применено: `scratchpad/sales_zakaz_id.sql` (этап 12 — `sales.zakaz_id`). Commit не делался.

## Этап 5: связь sales → orders (`zakaz_id BIGINT`) — LIVE
- `ALTER TABLE sales ADD zakaz_id bigint` + `idx_sales_zakaz_id`; backfill из `raw_refs.zakaz {type:271, uid}` через `orders(recorder, recorder_type)` за 53 с. Физического FK нет — как у всех ссылок витрины. `raw_refs.zakaz` сохранён.
- Покрытие 314 563 / 315 263 = 99,78 %. Неразрешённые 700: 693 — заказы, проведённые до 2026-02-01 (вне истории orders; 2025 — 335, 2024 — 10, 2022 — 2), 7 — удалены в 1С. Аномалий (заказ есть в orders, а связь пуста) — 0.
- Штатный резолв в post_load: sales (новая продажа → заказ уже есть) и orders (late-resolve: заказ пришёл позже продажи). Предикат узкий: `zakaz_id IS NULL AND raw_refs ? 'zakaz'`, по индексу ~700 строк, 27 мс.
- Проверки: сценарий А — 5 продаж тика 18:25 с заказом разрешены сразу; сценарий Б — текст post_load orders из etl_meta восстановил 3 обнулённые связи. Естественный late-resolve: 42 заказа, проведённые в 1С после исторической загрузки, подтянулись инкрементом и закрылись тем же запросом.
- Ловушка: `airflow tasks test <dag> <task>` в Airflow 3.0.6 прогоняет весь DAG (все 10 тасок), а не одну; инкременты идемпотентны, вреда нет, но для изолированных проверок он не годится.
- Скрипты в репозитории: `dags/core/migrations/prod/020_order_register.sql`, `021_sales_zakaz_id.sql`.

## Этап 6 (2026-09-04): аудит и починка конфигуратора (UI/Sync) под чистую модель
- Аудит: etl_meta ↔ PostgreSQL ↔ QueryBuilder ↔ ETLEngine ↔ Airflow синхронны; `order_positions` физически есть. Проблемы только в UI/Sync; UI подключён к `test` (dao.py), PROD не видит.
- Исправлено в `etl_config_app`: `update_target` — PATCH (Save не затирает include_columns/target_role/post_load_sql/priority/parent_target_id); FK fact→dim по правилу движка (`{dim}_id`, затем без «s»; берётся существующая колонка, `orders` → `order_id`); legacy `updated_at` больше не добавляется; CREATE/ALTER добавляют контрактные system-колонки (audit + `raw_refs` при мэппингах raw_refs.*; для reference_dim — `etl_updated_at`, `is_stub`, `retail_updated_at`), без `etl_hash`/REFERENCES; union-ветка Sync берёт мэппинги родителя члена; `_sync_union_and_target` не сливает в output_columns колонки чужих источников; `column-targets` предпочитает include_columns; UI показывает union (члены, приоритет, output columns, таргет), связи таргетов (source/union/parent/include/post_load), expression- и raw_refs-мэппинги (read-only бейджи), назначение таргета у detail-источников с `selected` по include_columns; форма таргета показывает служебные поля read-only.
- Проверки: TEST plan — NO ACTION у sales и 8 DIM (у пустых legacy `order`/`order_positions` только audit-колонки); PROD read-only plan — NO ACTION по всем 12; Save на TEST-таргете 82 изменил только pre_load_sql и откатился; PROD-регистр 77 рендерится через код UI in-process корректно.
- Не применено: `scratchpad/sales_orphan_mappings_backfill.sql` (54 из 60 orphan-мэппингов sales → однозначный target_id; 6 неоднозначных остаются NULL). UI на PROD не переключён. Commit не делался.
- Дозакрыто (не закоммичено): `dao.delete_union` не удаляет таргеты, которые питает union — отказ с перечислением таргетов (маршрут отвечает 409 с текстом); `add-target-column` добавляет колонку только в свои таргеты (`_targets_for_new_column`: явные target_ids → include_columns → header→dim / detail→fact; standalone — вручную, таблицы не трогаются) и только в union'ы, членами которых являются источники колонки. Проверено на scratch-объектах TEST и на подменённых данных без БД.
