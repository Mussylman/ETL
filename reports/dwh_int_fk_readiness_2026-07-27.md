---
tags: [audit, dwh, architecture, power-bi, sales]
date: 2026-07-27
---
# READ-ONLY аудит: физические integer FK в фактах + stub-справочники + raw_refs

Оценка целевой архитектуры против реального кода/данных. Ничего не менялось.
Базы: PG 14 @ 10.10.1.142/test (не 16, не Docker — см. reports/storage_audit_2026-07-24.md), MSSQL UPP_JAN.

## Вердикт

**Реализуемо, и ложится на уже работающие механизмы проекта.** Целевая схема (int FK физически в фактах, stub-dim, PBI только по int id) совместима с движком при трёх поправках:

1. **Жёсткие FK-констрейнты NOT NULL на `*_id` вешать нельзя** — резолв id происходит ПОСЛЕ приземления строки (паттерн post_load). `*_id` — nullable, целостность гарантируется post_load + валидацией `NULL=0` (механизм уже существует и уже проверяет `sales_id`).
2. **`raw_refs` не «удваивает», а утраивает-учетверяет вес** справочных ссылок (uuid-строка 36Б + имя ключа против 16Б binary). Рекомендация: в `raw_refs` класть только полиморфные документные ссылки (zakaz, doc_sale), справочные uuid-колонки оставить колонками до последнего этапа.
3. **`isHidden` в Power BI НЕ решает проблему веса** — скрытая колонка всё равно импортируется в модель и ест память. Гарантия должна быть на уровне контракта: PBI подключается ТОЛЬКО к `bi.*` views, в которых `raw_refs` просто не селектится. Так как PBI сегодня не подключён вообще (проверено: нет view, нет грантов), контракт вводится с нуля без миграции.

---

## 1. Существующие transforms — нового изобретать не нужно

Всё уже есть в `dags/core/transform/binary.py` и используется в проде:

| Функция | Направление | Где используется |
|---|---|---|
| `binary_to_uuid(b16)` | 1С RRef → guid | transform_type='binary_to_uuid' у 25+ мэппингов |
| `uuid_to_binary_1c(u)` / `uuid_to_mssql_hex_1c(u)` | guid → RRef (точная инверсия, round-trip доказан) | QueryBuilder: `WHERE _RecorderRRef IN (0x…)` — инкремент живёт на этом |
| `binary_to_int(b4)` | TRef → int (big-endian) | recorder_type, zakaz_type, doc_sale_type |
| `_parse_uuid_lenient` | толерантный парсер | `_delete_missing` |

Целевой поток «RRef → guid → lookup/stub → int id» использует первые две функции как есть. Резолв guid→id — это SQL-join по `dim.uid`, конверсий не требует.

## 2. Классификация всех uuid/RRef в фактах

### public.sales (10 uuid-колонок)
| Колонка | Класс | distinct | Судьба в целевой схеме |
|---|---|---|---|
| `recorder` (+`recorder_type` int) | **технический natural key ETL** (документ) | 81.5k | **ОСТАЁТСЯ КОЛОНКОЙ НАВСЕГДА** — хребет инкремента |
| `kontragent` | справочник Контрагенты | 568 | → `kontragent_id` + dim |
| `podrazdelenie` | справочник Подразделения | 103 | → id + dim |
| `sklad` | справочник Склады | 65 | → id + dim |
| `organizatsiya_uid` | справочник Организации | **1** | → id + dim (или константа — кандидат на удаление из fact вовсе) |
| `dogovor_uid` | справочник Договоры | 629 | → id + dim |
| `vidoperatsii` | **перечисление 1С**, не справочник | 5 | → id + мини-dim; имена не грузятся как справочник — см. «чего не хватает» |
| `otvetstvennyy_uid` | справочник Сотрудники (45% пустышек `0000…`) | 375 | → id + dim; пустышки → NULL до резолва |
| `zakaz_uid` (+`zakaz_type`) | **полиморфная ссылка на документ** | 79.4k, 2 типа | → `raw_refs` |
| `doc_sale_uid` (+`doc_sale_type`) | **полиморфная ссылка на документ** | 81k, 5 типов | → `raw_refs` |

### public.sales_positions (4 uuid-колонки)
| Колонка | Класс | Судьба |
|---|---|---|
| `recorder` (+`recorder_type`, `line_no`) | технический natural key ETL | **ОСТАЁТСЯ** |
| `nomenklatura` | справочник Номенклатура (9.5k distinct) | → `nomenklatura_id` + dim — главный кейс |
| `sklad` | справочник (95% NULL — только VT-строки) | → id, stub только для non-null |
| `kachestvo` | справочник (5 distinct, 95% NULL) | → id + мини-dim |

Итого dim-таблиц на горизонте: **9** (номенклатура, склад, подразделение, контрагент, организация, договор, вид операции, ответственный, качество). Целевой вид `sales_positions` из ТЗ достижим 1-в-1 **плюс** сохранённые `recorder`/`recorder_type`/`line_no`.

## 3. Полиморфные ссылки: подтверждаю, guid без типа недостаточен

- `zakaz_uid` живёт с `zakaz_type` (2 значения), `doc_sale_uid` — с `doc_sale_type` (5 значений), `recorder` — с `recorder_type` (4). В 1С это `_RRRef`+`_RTRef` пары — тип определяет ТАБЛИЦУ (`_Document254` vs `_Document415`…), без него guid не разрезолвить в имя/номер.
- Движок это уже знает: QueryBuilder джойнит шапки по `_RecorderRRef AND _RecorderTRef=0x{N}`, upsert-ключ — `(recorder, recorder_type)`.
- Вывод: ключ полиморфной ссылки = **(type, guid)**, в `raw_refs` хранить парой: `{"zakaz": {"type": 354, "uid": "…"}}`.

## 4. Строки VT: искусственный guid не нужен — подтверждаю

- В 1С строка VT своего guid НЕ ИМЕЕТ (только `_LineNo`) — источника для искусственного guid не существует.
- `(recorder, recorder_type, line_no)` — рабочий upsert-ключ; integrity-аудит 2026-07-21: дублей 0 на 114k строк.
- В целевой схеме внутренний ключ строки — surrogate `id` (уже есть BIGSERIAL) + natural `(sales_id, line_no)`.

## 5. ГЛАВНЫЙ РИСК: инкремент. Где именно возникает зависимость fact→dim

Карта мест, где движок держится на uuid в фактах (всё проверено по коду):

| Механизм | Код | Ломается при удалении uuid? | Ломается при ДОБАВЛЕНИИ *_id? |
|---|---|---|---|
| Upsert-ключи | `register_targets.upsert_keys` = (recorder, recorder_type[, line_no]) | ДА | нет |
| Tail-фикс (потеря 23%) | `data_checker.py::_get_tail_uids` — `WHERE recorder = ANY(uuid[])` | ДА | нет |
| Missing-DELETE | `etl_engine.py::_delete_missing` — DELETE по `upsert_keys[0]`='recorder' | ДА | нет |
| Выборка из MSSQL | QueryBuilder — `_RecorderRRef IN (uuid_to_mssql_hex_1c…)` | ДА (нужен recorder в DWH для сверки) | нет |
| FK-резолв | `post_load_sql` — UPDATE … WHERE f.recorder=d.recorder | ДА | нет |
| Еженедельная сверка | `docs/audits/sql/sales_recon.py` — анти-джойн по recorder | ДА | нет |

**Вывод: добавление int id ничего не ломает; ломает только удаление технических uuid — их не трогаем (совпадает с ТЗ).** Справочные uuid (nomenklatura и т.п.) движком НЕ используются — используются только для резолва id, поэтому их перенос в raw_refs на позднем этапе безопасен для инкремента.

### Порядок «stub → id → строка в факт»: как это реально гарантировать

Ключевой факт: **паттерн «строка приземляется с NULL-id, id дозаполняется post_load» уже работает в проде** — ровно так живёт `sales_positions.sales_id`:
- post_load_sql выполняется на ОБОИХ путях: incremental (`etl_engine.py:624`) и full_period (`:760`), через `hook.run()` — **один транзакционный блок**;
- `_validate_full_period_load` (`:249-260`) уже проверяет `{dim}_id IS NULL = 0` и роняет загрузку при нарушении.

Целевой post_load на каждый тик (одна транзакция):
```
1) INSERT INTO dim_x (uid, is_stub) SELECT DISTINCT <uuid_col> FROM fact WHERE <uuid_col> IS NOT NULL
   AND <uuid_col> <> '0000…' ON CONFLICT (uid) DO NOTHING;   -- stub, гонки решает БД
2) UPDATE fact SET x_id = d.id FROM dim_x d WHERE fact.<uuid_col> = d.uid AND fact.x_id IS NULL;
```
Оба стейтмента в одном `post_load_sql` = одна транзакция. Строка факта НЕ ждёт dim — она встаёт сразу (upsert как сейчас), id появляется в той же секунде post_load'ом.

**Честно про транзакционность всего тика**: цельный тик сегодня НЕ атомарен (loader коммитит батчами по 1000; missing-delete отдельно). Это не дефект для целевой схемы: upsert идемпотентен, окно «строка есть, id NULL» ограничено миллисекундами между коммитом батча и post_load; при падении тика между ними — следующий тик/post_load дозаполнит (`WHERE x_id IS NULL`). Для BI-слоя NULL-id отображается как «(не определено)» через LEFT JOIN во view. Требовать строгую одиночную транзакцию на тик = переписывать loader — не нужно.

### Full_period
Тот же post_load выполняется после каждого target'a + валидация `x_id NULL=0` в конце — расширяется одной строкой в `_validate_full_period_load` по образцу `sales_id`.

### Backfill существующих данных
`UPDATE fact SET x_id = d.id FROM dim_x d WHERE fact.<uuid_col> = d.uid AND fact.x_id IS NULL` — батчами; объёмы смешные (81k+114k строк, минуты).

## 6. ВТОРОЙ РИСК: raw_refs jsonb — честные числа

Замер по факту (storage-аудит 2026-07-24): вес uuid-колонки = 16Б/значение. Вес того же в jsonb ≈ 36Б (uuid строкой) + имя ключа (10–15Б) + структура ≈ **50–56Б на ссылку = ×3.3, не ×2**.

| Сценарий | Прибавка к sales (81.5k строк) | Оценка |
|---|---|---|
| ВСЕ 10 uuid → raw_refs | +540Б/строку ≈ **+44 МБ** (heap sales сейчас 29 МБ) | плохо |
| Только полиморфные (zakaz+type, doc_sale+type) | ~120Б/строку ≈ **+10 МБ** | приемлемо |
| positions: nomenklatura+sklad+kachestvo → raw_refs | +5–8 МБ | приемлемо, но выгоды нет — см. ниже |

Рекомендация: **raw_refs — только для полиморфных документных ссылок** (структура `{"zakaz":{"type":..,"uid":".."},"doc_sale":{...}}`). Справочные uuid держать колонками до финального этапа: они нужны post_load-резолву, и колонка 16Б дешевле jsonb-записи 50Б. Если на финальном этапе захочется их убрать — резолв переключается на `(raw_refs->'x'->>'uid')::uuid`, работает, но медленнее и тяжелее. Убирать — только ради чистоты схемы, не ради места.

### isHidden — НЕ гарантия
В PBI import-mode скрытая (`isHidden`) колонка **всё равно загружается в модель** и занимает память; она лишь не видна в списке полей. Гарантия — структурная:
- PBI-роль получает GRANT только на схему `bi` (сегодня грантов нет вообще — чистый старт, проверено);
- `bi.*` views не селектят ни `raw_refs`, ни uuid-колонки, ни технические поля;
- в модель физически не попадает ничего, кроме int id + атрибутов + мер.

## Что уже есть в проекте и работает на эту архитектуру
- `post_load_sql` — проверенный механизм резолва id (sales_id, в проде с июня);
- `_validate_full_period_load` — готовый хук валидации `*_id NULL=0`;
- `binary_to_uuid`/`uuid_to_binary_1c` — конверсии;
- upsert `ON CONFLICT DO NOTHING/UPDATE` — идемпотентность и гонки;
- право `CREATE SCHEMA` у airflow_admin в test — есть (проверено).

## Чего не хватает (говорю прямо)
1. **Sync конфигуратора может снести новые `*_id`**: `dao.py` при синхронизации удаляет колонки не из include_columns, кроме `SYSTEM_COLS` (sales_id там уже есть). Новые `nomenklatura_id` и т.д. надо защитить: правило «суффикс `_id` не трогать» или включение в SYSTEM_COLS-механику. Без этого первый же Sync после миграции снесёт FK-колонки.
2. **Шаблона «Справочник» в wizard'е нет** («скоро») — загрузку dim из 1С (`_Reference*` таблицы) первое время делать отдельным лёгким загрузчиком/CLI, не через конфигуратор.
3. **`vidoperatsii` — перечисление 1С**, имена живут не в `_Reference*`, а в метаданных — тянуть через 1С meta API (http://192.168.18.224:8090) или ручным словарём на 5 значений.
4. **Бэкапов нет**: перед ADD COLUMN/backfill обязателен pg_dump facts + etl_meta (её бэкапа нет вообще — известный долг).
5. `pgstattuple`/суперправа отсутствуют — на план не влияет.

## Безопасный план миграции (порядок и точки отката)

Ничего из этого НЕ выполнено — план.

| Шаг | Что | Инкремент затронут? | Откат |
|---|---|---|---|
| 0 | pg_dump: public.sales, sales_positions, etl_meta | нет | — |
| 1 | Защита `*_id` от Sync в dao.py (маленький PR + тест) | нет | revert commit |
| 2 | `dim_nomenklatura` (id BIGSERIAL PK, uid uuid UNIQUE NOT NULL, code, name, article, category_id, is_stub bool DEFAULT true, source_refs jsonb) + загрузчик имён из 1С `_Reference*` | нет | DROP TABLE |
| 3 | Stub-стейтмент №1 в post_load_sql fact-target'а (INSERT … ON CONFLICT DO NOTHING) — dim наполняется, факты не тронуты | нет (post_load уже в контуре) | убрать стейтмент |
| 4 | `ALTER TABLE sales_positions ADD COLUMN nomenklatura_id BIGINT` (nullable, без DEFAULT — метаданные-only, мгновенно) + backfill батчами + индекс | нет | DROP COLUMN |
| 5 | Резолв-стейтмент №2 в post_load_sql + строка валидации в `_validate_full_period_load` | нет | убрать стейтмент |
| 6 | Наблюдение ≥1 неделя: sales_recon.py в ноль, `nomenklatura_id NULL=0`, тики в норме | — | — |
| 7 | Схема `bi` + `bi.dim_nomenklatura`, `bi.sales_positions` (только int id + атрибуты + меры; без uuid/raw_refs/технических) + роль PBI с грантом только на bi | нет | DROP SCHEMA |
| 8 | Повторить шаги 2–7 для остальных dim (порядок: sklad, podrazdelenie → kontragent, dogovor, organizatsiya → мелкие) | нет | по-таблично |
| 9 | raw_refs jsonb в sales только для zakaz/doc_sale (+backfill), после чего zakaz_uid/zakaz_type/doc_sale_uid/doc_sale_type удаляются из include_columns и (после проверки) из таблицы | нет (движок эти колонки не использует) | колонки восстановимы из raw_refs |
| 10 | **Опционально, в самом конце**: удаление справочных uuid-колонок из фактов. Только после: (а) N недель чистого recon, (б) grep по коду на использования, (в) перевод резолва на raw_refs. `recorder`/`recorder_type`/`line_no` — НЕ УДАЛЯЮТСЯ НИКОГДА | проверить full+incremental на копии | восстановление из raw_refs/1С |

Красные линии (совпадают с твоим «что не предлагать», плюс мои):
- никаких NOT NULL / жёстких FK-констрейнтов на `*_id` (сломает порядок загрузки);
- никакого резолва id внутри загрузочного batch-цикла (замедлит тик; post_load делает это set-based за миллисекунды);
- Sync конфигуратора не запускать между шагом 4 и шагом 1-фиксом.

## Ответ одним абзацем
Архитектура подходит проекту: физический int FK в фактах достигается уже существующим в проде паттерном post_load (sales_id — живое доказательство), stub-справочники ложатся на idempotent `ON CONFLICT DO NOTHING` в той же транзакции post_load, инкремент не затрагивается вообще, пока живы технические `recorder`/`recorder_type`/`line_no` (они остаются навсегда). Две поправки к ТЗ: `raw_refs` — только для полиморфных ссылок (иначе ×3.3 к весу, а не ×2), и вместо `isHidden` — структурная изоляция через схему `bi` (isHidden не убирает колонку из памяти модели PBI).

---

## Дополнение (2026-07-27): канонический ключ = guid. Проверка всех путей загрузки

Уточнение архитектуры: во всём DWH хранится ТОЛЬКО guid; binary uid не хранится нигде, получается `uuid_to_binary_1c(guid)` в момент запроса к физической MSSQL.

### Инвентаризация путей — чем приходит ссылка на входе

| Путь | Форма на входе | guid достижим? |
|---|---|---|
| retail `ims_db.sales.document_uid` | текстовый guid | ✅ готовый |
| retail `details` json (product, warehouse, counterparty…) | текстовые guid | ✅ готовый |
| MSSQL 1С: `_RecorderRRef`, `_Fld…RRef` (AccumRg, шапки, VT) | binary(16) | ✅ `binary_to_uuid` (мэппинги) |
| MSSQL 1С: колонки RRef БЕЗ transform_type в мэппингах | binary(16) | ✅ движок применяет `process_binary_auto` (len 16 → uuid) |
| MSSQL 1С: `_RecorderTRef`, `_RTRef` (типы полиморфных ссылок) | binary(4) | ❌ это НЕ guid и не должен им быть → **int** (`binary_to_int`), хранится рядом с guid как часть ключа (type, guid) |
| 1С meta API (onec_client.py) | метаданные конфигурации | ссылки не грузит |
| Google Sheets → dim_user_name, dim_asp_products (update_users.py) | email / целочисленные division_id | guid не существует — **не-1С источник**, канон не применим |
| GFK (etl_gfk) | коды продуктов | 1С-ссылок нет вообще |

Факт: **bytea-колонок в DWH ноль** (проверено по information_schema во всех схемах) — binary uid уже сегодня нигде не хранится, канон «guid-only» де-факто соблюдён.

### Вердикт: пути, где guid получить нельзя, НЕ НАЙДЕНО

Исключение для хранения binary uid не требуется. Обратимость гарантирована: `uuid_to_binary_1c` — точная инверсия `binary_to_uuid` (контракт в docstring: round-trip в обе стороны), и это проверяется прод-нагрузкой с июня — каждый тик инкремента конвертирует guid→binary для `WHERE _RecorderRRef IN (…)`, все сверки сходились в ноль.

### Три сноски (не исключения, а правила)

1. **Типы полиморфных ссылок** (`recorder_type`, `zakaz_type`, `doc_sale_type`) — binary(4) → int. Это не uid, а дискриминатор; хранится как int рядом с guid. В raw_refs: `{"zakaz": {"type": 354, "uid": "guid"}}`.
2. **Не-1С источники** (Sheets, GFK, retail-собственные сущности с int-ключами) — у них guid'а не существует в природе; их справочники, если появятся, живут на собственных natural keys (email, код) + пометка источника. Это другой класс, не ломающий канон 1С-ссылок.
3. **Гигиена мэппингов**: 10 активных RRef-мэппингов без transform_type (4 в register 62 — мёртвые otvetstvennyy/zakazpokupatelya, 6 в заброшенном register 63/orders). Работают через binary_auto-фолбэк, но при реализации плана проставить `binary_to_uuid` явно или деактивировать — фолбэк не должен быть несущей конструкцией.
4. **Пустая ссылка 1С** `00000000-…` — валидный guid, но семантически NULL: нормализуется в NULL до stub-резолва (иначе в dim появится «объект-пустышка»).
