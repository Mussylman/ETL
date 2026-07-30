---
tags: [plan, dwh, dim, nomenklatura, pilot]
date: 2026-07-27
---
# Пилот guid→id: sales_positions → dim_nomenklatura (план, НЕ реализовано)

## READ-ONLY проверки (выполнены 2026-07-27 на живой базе)

**1. Колонка**: `public.sales_positions.nomenklatura`, тип `uuid`.
Свежие цифры: 121 939 строк, **9 904 distinct guid**, **NULL = 0**, **пустышек `0000…` = 0**.
(Пустышек нет, но фильтр в резолвере всё равно ставим — появятся с новыми типами документов.)

**2. Одиночная ссылка — ПОДТВЕРЖДЕНО, пилот на ней корректен.**
В `sales_positions` нет `nomenklatura_type` (единственный *_type — `recorder_type`, он про документ).
Все 6 источников — одиночные `_Fld…RRef` (полиморфные в 1С хранятся ПАРОЙ `_RRRef`+`_RTRef`, как у zakaz/doc_sale/recorder — тут пары нет).

**3. Форма на входе**: binary(16) RRef из MSSQL → `binary_to_uuid` на уровне мэппингов
(6 активных мэппингов: `_AccumRg17844._Fld17845RRef` + 5 VT-таблиц, у всех transform_type='binary_to_uuid';
применяется в transform-стадии движка). В PG guid приезжает готовым `uuid`. Retail номенклатуру не несёт вовсе.

**4. Sync снесёт `nomenklatura_id` — ПОДТВЕРЖДЕНО, фикс = шаг 0.**
`dao.py:1079-1090`: колонка не из мэппингов и не из `SYSTEM_COLS` (`:1071`) попадает в план как
`drop_column`. `sales_id` выживает только потому, что он в SYSTEM_COLS литералом.

---

## Шаг 0 (обязательный, ДО любого ALTER): защита *_id от Sync

`dao.py`, в цикле поиска лишних колонок добавить правило: колонки с суффиксом `_id` не дропать
(FK-колонки dim-слоя; заполняются post_load, в мэппингах их нет по определению).
Плюс кейс в существующий `etl_config_app/tests/sync_ddl_test.py`.
Альтернатива — вносить каждую в SYSTEM_COLS литералом; отвергнута: каждый новый dim = правка кода.
**Откат**: revert коммита.

## Шаг 1: dim_nomenklatura (создаётся ПУСТОЙ — это норма паттерна)

```sql
CREATE TABLE public.dim_nomenklatura (
    id      integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    guid    uuid    NOT NULL UNIQUE,        -- канонический ключ; без UNIQUE stub-ы задублируются
    name    text,                           -- NULL пока stub
    code    text,
    is_stub boolean NOT NULL DEFAULT true,
    etl_updated_at timestamp NOT NULL DEFAULT now()
);
```
- `id` — int (integer, по решению: не smallint), раздаёт ТОЛЬКО эта таблица (IDENTITY).
- guid живёт здесь; binary uid не хранится нигде (канон; при запросах в MSSQL — `uuid_to_binary_1c`).
**Откат**: `DROP TABLE public.dim_nomenklatura;`

## Шаг 2: колонка в факте

```sql
ALTER TABLE public.sales_positions ADD COLUMN nomenklatura_id integer;
```
- nullable, без DEFAULT → metadata-only, мгновенно; без NOT NULL и без FK-констрейнта —
  строка факта не ждёт справочник (решение аудита 2026-07-27).
**Откат**: `DROP COLUMN nomenklatura_id;`

## Шаг 3: backfill = первый прогон резолвера (вручную, под присмотром)

Резолвер — set-based get-or-create, два стейтмента (никаких функций/циклов):
```sql
BEGIN;
-- 3a. stub для всех незнакомых guid (гонки решает ON CONFLICT по UNIQUE guid)
INSERT INTO public.dim_nomenklatura (guid)
SELECT DISTINCT p.nomenklatura
FROM   public.sales_positions p
WHERE  p.nomenklatura_id IS NULL
  AND  p.nomenklatura IS NOT NULL
  AND  p.nomenklatura <> '00000000-0000-0000-0000-000000000000'::uuid   -- пустышка → NULL, stub НЕ заводим
ON CONFLICT (guid) DO NOTHING;

-- 3b. резолв id в факт
UPDATE public.sales_positions p
SET    nomenklatura_id = d.id
FROM   public.dim_nomenklatura d
WHERE  p.nomenklatura = d.guid
  AND  p.nomenklatura_id IS NULL;
COMMIT;
```
Ожидаемо: ~9 904 stub-строки (100% is_stub=true — НОРМА), 121 939 фактов получают id, секунды.
Контроль после: `nomenklatura_id IS NULL` = 0; `count(dim)` = distinct guid факта.
**Откат**: `UPDATE sales_positions SET nomenklatura_id=NULL; TRUNCATE dim_nomenklatura;` (id пересоздадутся — до подключения BI это безопасно; после BI отката этого шага уже нет, только вперёд).

## Шаг 4: индекс

```sql
CREATE INDEX idx_sales_positions_nomenklatura_id ON public.sales_positions (nomenklatura_id);
```
**Откат**: DROP INDEX.

## Шаг 5: врезка в ETL — те же два стейтмента в post_load_sql

Конфиг, не код: `UPDATE etl_meta.register_targets SET post_load_sql = <текущий (sales_id) + 3a + 3b> WHERE id = 81;`
- Выполняется движком на ОБОИХ путях: incremental (`etl_engine.py:624`) и full_period (`:760`), через
  `hook.run()` — **одна транзакция**: stub → id, порядок гарантирован внутри тика.
- Оба стейтмента фильтруют `nomenklatura_id IS NULL` → стоимость тика пропорциональна новым строкам.
- Упавший между батчем и post_load тик самозалечивается следующим тиком (идемпотентно).
**Откат**: вернуть старый post_load_sql (текст сохранить в файл перед заменой).

## Шаг 6: наблюдение (≥2-3 дня)
- каждый тик: `changes`/`rows_loaded` в норме, длительность не выросла;
- `SELECT count(*) FROM sales_positions WHERE nomenklatura_id IS NULL` → 0 (допустимы мгновенные ненули между батчем и post_load);
- `sales_recon.py` — в ноль, как обычно;
- один full_period на 1 день — прогнать и убедиться, что валидация зелёная.

## Почему 100% stub НЕ ломает full_period и incremental (проверено по коду)
- `_validate_full_period_load` (`etl_engine.py:249-260`) проверяет только `{dim_target}_id` = `sales_id`
  (dim-target РЕГИСТРА). `dim_nomenklatura` target-ом регистра не является, `nomenklatura_id` валидатор
  не знает → 100% stub / переходные NULL уронить загрузку не могут.
- Инкремент (upsert/tail/missing-delete) колонку `nomenklatura` не использует — только `recorder`.
- Позже (опционально, после стабилизации) валидацию можно РАСШИРИТЬ проверкой `nomenklatura_id NULL=0` —
  только ПОСЛЕ шага 5, иначе первый же full_period упадёт.

## Шаг 7 (после пилота, отдельное «ок»): наполнение справочника
Источник: `_ReferenceNNN` Номенклатуры в MSSQL (номер таблицы получить через meta API / onec_client).
Загрузчик upsert-ит по guid:
```sql
INSERT INTO dim_nomenklatura (guid, name, code, is_stub)
VALUES (...иz 1С..., false)
ON CONFLICT (guid) DO UPDATE
SET name = EXCLUDED.name, code = EXCLUDED.code, is_stub = false, etl_updated_at = now();
```
**id не участвует в SET — не меняется никогда.** Stub, которого нет в справочнике 1С
(удалён/архив), остаётся stub'ом — это сигнал качества данных, не ошибка.

## Чего не хватает (прямо)
1. **Физическое имя таблицы справочника Номенклатуры** (`_ReferenceNNN`) — неизвестно; получить через
   onec_client (meta API 192.168.18.224:8090) на шаге 7. Для пилота не нужно.
2. **Шаблона «Справочник» в wizard'е нет** — шаг 7 первое время делается лёгким загрузчиком/CLI.
3. `name`/`code` до шага 7 — NULL у всех: если пилотную bi-view делать сразу, подпись временно
   `coalesce(name, guid::text)`.
4. Права: не нужны новые — airflow_admin владеет таблицами public и умеет CREATE.

## Что осталось честно неидеальным (в рамках пилота)
guid номенклатуры пока живёт в двух местах: в dim (канонический дом) И колонкой факта — потому что
(а) её льют 6 активных мэппингов, (б) post_load-резолв читает её. Полный уход guid из факта — отдельное
решение после пилота: потребует резолва на этапе загрузки внутри движка, а не post_load. В пилоте не трогаем.

## Порядок и точки невозврата
0 → 1 → 2 → 3 → 4 → 5 → 6 (→ 7 позже). Шаги 0-5 полностью откатываемы. Точка невозврата одна:
когда Power BI начнёт ссылаться на выданные id — после этого TRUNCATE dim запрещён навсегда
(id стабильны). До подключения BI можно откатить всё.

---

# ЖУРНАЛ ВНЕДРЕНИЯ (2026-07-28, по «ок» пользователя, с шагом 5.1)

| Шаг | Результат |
|---|---|
| 0 | dao.py: правило «*_id не дропать» + 2 кейса в sync_ddl_test.py — ВСЕ ТЕСТЫ ЗЕЛЁНЫЕ (заодно починена устаревшая фикстура теста дублей: line_no → актуальные колонки эталона) |
| 1 | dim_nomenklatura создана |
| 2 | sales_positions.nomenklatura_id добавлена (nullable, без FK) |
| 3 | Backfill: 9 905 stub'ов, 121 977 строк разрезолвлено, unresolved=0, dim_rows=fact_distinct точно |
| 4 | idx_sales_positions_nomenklatura_id создан |
| 5 | post_load_sql target 81 расширен (stub→resolve); старый текст: reports/backup_post_load_sql_target81_2026-07-27.txt |
| 5.1 | _validate_full_period_load: generic-проверка пар (<x> uuid, <x>_id) — NULL при валидном guid = ошибка; пустышки 0000… легально NULL |
| 6 | Инкремент: тик 9530 success, 23 строки, unresolved=0. Full_period 1 день (id 9534): success, POST-LOAD VALIDATION PASSED, 3 новых stub'а на лету (9905→9908), unresolved=0 |

Паттерн guid→id ОБКАТАН на обоих путях загрузки. dim: 9 908 строк, все is_stub=true (имена ждут шаг 7 — источник справочника 1С).

## Шаг 7 выполнен (2026-07-29, по «ок»)
- Физическая таблица найдена через meta API: Справочник.Номенклатура = **_Reference123**
  (_Code, _Description, артикул=_Fld1897 — на будущее).
- Обогащение SCOPED: только существующие stub'ы (полный каталог — задача будущего
  register'а справочника, не пилота). Батчи по 500 uuid → uuid_to_mssql_hex_1c.
- Результат: **9 910 из 9 910 stub'ов найдены и обогащены** (100%); групп (_Folder) — 0,
  помеченных на удаление — 0; still_stub = 0, пустых имён = 0. id не менялись (UPDATE без id).
- Контроль: BI-запрос топ-10 по int-связке даёт осмысленные имена (iPhone 17 Pro Max,
  DualSense, Epson…); join по id vs join по guid — mismatch = 0.

ПИЛОТ ЗАВЕРШЁН ПОЛНОСТЬЮ. Паттерн guid→id→имя обкатан от 1С до BI-запроса.

## Тираж на остальные справочники (2026-07-29, по «гоу» с боевым тестом)
Порядок: сначала otvetstvennyy (45% пустышек — боевой тест защиты), после чистого прохода — остальные 5.
vidoperatsii исключён по решению пользователя (перечисление на 5 значений — не справочник).

| dim | _Reference | строк | обогащено | FK-колонки |
|---|---|---:|---|---|
| dim_otvetstvennyy | _Reference145 (Пользователи) | 379 | 100% | sales.otvetstvennyy_id |
| dim_kontragent | _Reference108 | 598 | 100% | sales.kontragent_id |
| dim_podrazdelenie | _Reference141 | 102 | 100% | sales.podrazdelenie_id |
| dim_sklad | _Reference169 | 67 | 100% | sales.sklad_id + sales_positions.sklad_id (один справочник) |
| dim_organizatsiya | _Reference131 | 1 | 100% | sales.organizatsiya_id |
| dim_dogovor | _Reference75 | 663 | 100% | sales.dogovor_id |
| dim_kachestvo | _Reference97 | 5 | 100% | sales_positions.kachestvo_id |

- Боевой тест пустышек: 39 291 строка `0000…` → id NULL, в dim не попала ни одна; unresolved среди валидных = 0 везде.
- Валидация расширена: правило имён `_uid`-суффикса (otvetstvennyy_uid → otvetstvennyy_id) + проверка пар на ВСЕХ target-ах.
- post_load: target 80 — 6 резолверов (sales), target 81 — 3 (positions; sklad кормится stub-ами из обеих таблиц).
- Контроль: тик 9574 success; full_period 9575 — POST-LOAD VALIDATION PASSED (9 пар guid/_id).

ТИРАЖ ЗАВЕРШЁН: 8 справочников (incl. пилот), 9 FK-колонок, все обогащены на 100%.
Осталось вне тиража: vidoperatsii (перечисление — мини-словарь через meta API), полиморфные zakaz/doc_sale (raw_refs-этап), bi.* витрины.
