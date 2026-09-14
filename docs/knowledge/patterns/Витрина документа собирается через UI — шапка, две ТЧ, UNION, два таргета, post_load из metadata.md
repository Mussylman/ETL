---
tags: [pattern, configurator, document_with_vt, raw_refs, wizard]
date: 2026-09-09
---

# Витрина документа собирается через UI: шапка, две ТЧ, UNION, два таргета, post_load из metadata

Проверено контрольным циклом на TEST 2026-09-09 (регистр `wiz_probe`, только HTTP-формы, затем удалён).

## Шаги
1. **Регистр** (`/registers/new`): `pipeline_type = document_with_vt`, `default_mode = full_period`, retail-привязка
   (`retail_table`, `retail_uid_column`) — без неё DAG `incremental` не создаст таску.
2. **Источники** (`/registers/{id}/sources/new`): header `_DocumentN` с `where` на `_Posted = 0x01 AND _Marked = 0x00`;
   `period_column` = авто → `_Date_Time` (у документа нет `_Period`). Две detail `_DocumentN_VTx` с `parent_source_id`,
   `INNER JOIN` по `_DocumentN_IDRRef = _IDRRef`; `period_column` авто → пусто (фильтр на родителе).
3. **Мэппинги** (форма мэппинга или Column Builder): технические ключи `_IDRRef → recorder (binary_to_uuid)`,
   `CAST(N AS int) → recorder_type` (expression), `_Date_Time → period (fix_year)`, `_LineNoM → line_no`,
   `'product' / 'service' → vt_kind` (expression). Ссылки — только `raw_refs.<key>` (`binary_to_uuid`), физических
   GUID нет; у ТЧ без реквизита — явный `CAST(NULL AS binary(16)) → raw_refs.<key>` (иначе union унаследует шапку).
   Колонка обязана существовать в UPP_JAN: статус «MSSQL ✓» или «только MSSQL»; «только API» маппить нельзя.
4. **Union** (`/registers/{id}/unions/new` + members): члены — обе ТЧ, `output_columns` = колонки позиций
   (включая `raw_refs.*`); `recorder`/`recorder_type` приходят из шапки через JOIN.
5. **Таргеты** (`/registers/{id}/targets/new`): шапка — `source_id`, role `dimension`, upsert `(recorder, recorder_type)`;
   позиции — `union_id`, role `fact`, `parent_target_id` = шапка, upsert `(recorder, recorder_type, vt_kind, line_no)`,
   priority 1. `include_columns` перечисляют колонки таргета + `raw_refs`. Save запускает Sync: таблицы создаются
   целиком — `id BIGINT`, мэппинг-колонки, `<key>_id BIGINT` для каждого `raw_refs.<key>` с существующим `dim_<key>`,
   FK на шапку (`{parent}_id`, для множественного числа без «s»), `raw_refs JSONB`, audit-колонки.
6. **post_load** — кнопка «Сгенерировать шаблон из metadata» в форме таргета (`GET /api/targets/{id}/post-load-template`):
   FK на шапку, stub+резолв по каждому `raw_refs.<key>` → `dim.guid → <key>_id`, удаление исчезнувших строк у fact,
   late-resolve для ссылок, объявленных `transform_params {"ref_target": "<таблица>"}` у мэппинга `raw_refs.<key>.uid`.
   Шаблон вставляется в поле и сохраняется Save (PATCH). Существующий post_load автоматически не переписывается.
7. **Загрузка**: контрольный день `run_full_period`, сверка с 1С, история; активный регистр с retail-привязкой
   подхватывается `incremental_prod` автоматически.

## Что остаётся руками (SQL/параметры)
- Справочник, чьё имя не равно `dim_<key>` (Грузополучатель → `dim_kontragent`): `transform_params {"dim": "dim_kontragent"}` у мэппинга.
- Ссылка на другой регистр (заказ у продажи): `transform_params {"ref_target": "orders"}` у мэппинга `raw_refs.<key>.uid`.
- Полиморфная ссылка внутри того же регистра (doc_sale у sales) — вручную.
- Индексы под BI сверх PK/UNIQUE — миграцией.

Связано: [[Регистр order строится от шапки документа с UNION табличных частей, а не от AccumRg]],
[[Meta API 1С отдаёт номера полей другой базы — для новых реквизитов колонки не совпадают с UPP_JAN]],
[[Конфигуратор — два экземпляра по окружению, а не переключатель внутри UI]].
