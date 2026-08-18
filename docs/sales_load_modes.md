---
name: sales_load_modes
description: Режимы загрузки sales ETL — full_period, reprocess_period, incremental_fresh. Семантика дат и watermark.
date: 2026-06-23
tags:
  - knowledge
  - patterns
  - sales
  - etl
---

# Режимы загрузки sales ETL

> **TL;DR:** есть только **два** боевых режима — `full_period`/`reprocess_period` и `incremental_fresh`. `TRUNCATE` запрещён. У дат есть **строгая** семантика — никаких `updated_at` без префикса в новой логике.

## Семантика дат

| Поле | Откуда | Когда заполняется | Смысл |
|---|---|---|---|
| `period` | `_AccumRg17844._Period` (с offset −2000) | в каждой строке | Дата самой продажи (бизнес-таймстамп). По ней фильтруют отчёты и DELETE при reprocess. |
| `retail_snapshot_at` | `MAX(updated_at)` из `bd_retail.public.sales` | на старте `full_period`/`reprocess_period` — **один** на весь прогон | «Витрина пересобрана на состоянии retail до этого момента». Двигается **раз** за прогон, не per-row. |
| `retail_updated_at` | `retail.updated_at` конкретной записи | в **incremental** — per-row через map `recorder → updated_at` | Это **основной watermark** после того как incremental запустился. `MAX(retail_updated_at)` = до какой точки retail-лог мы прочитали. |
| `etl_loaded_at` | `DEFAULT now()` в DDL | при первом INSERT | Когда строка появилась в нашей БД. Не обновляется при upsert. |
| `etl_updated_at` | `now()` в коде ETL | при **каждом** upsert | Когда наш ETL последний раз тронул строку. |
| `updated_at` (legacy) | копия `retail_snapshot_at` у dim | full_period для dim — для совместимости со старым BI | **deprecated** — новый код читает `retail_snapshot_at` или `retail_updated_at`, не это. |

### Дополнения по `sales_positions`

Все три ETL-аудит-поля (`retail_snapshot_at`, `retail_updated_at`, `etl_updated_at`) **дублируются в `sales_positions`**. Причина: Power BI и аналитики фильтруют позиции по этим полям без join к `sales`. Альтернатива (только в `sales` + join) — формально нормализованнее, но создаёт издержки на каждом запросе.

## Режим A: `full_period` / `reprocess_period`

Используется для:
- первой инициализации витрины
- ручной пересборки конкретного периода (`reprocess_period_manual`)
- еженедельной пересборки последнего месяца (`reprocess_last_month_weekly`)

### Контракт

1. Если `start_date`/`end_date` **не переданы** → дефолт = последний месяц (`today − 30d`).
2. `end_date` **exclusive** — последний нужный день + 1. (Узнали на собственной шкуре: `BETWEEN '...AND '...'` в MSSQL включает оба, поэтому правило простое — день+1.)
3. Если `(end − start) > 45 дней` → требуется явный флаг `allow_large_period=true`.
4. Перед загрузкой — `DELETE` строго в пределах периода (НЕ `TRUNCATE`). Сначала из `sales_positions` через JOIN к `sales`, потом из `sales`. Один транзакционный блок.
5. На старте — `SELECT MAX(updated_at) FROM bd_retail.public.sales` → `retail_snapshot_at`.
6. Всем строкам этого прогона записывается **одинаковый** `retail_snapshot_at`.
7. `retail_updated_at` **остаётся NULL** (это full_period, не per-row).
8. `etl_updated_at = now()`.
9. После загрузки — `_validate_full_period_load()`: counts/NULL/duplicates/`sales_id` FK + `retail_snapshot_at IS NOT NULL`.
10. `load_history.checkpoint_value`:
    `<start>..<end>; retail_snapshot_at=<value>`

### SQL DELETE (внутри одной транзакции)

```sql
BEGIN;
DELETE FROM public.sales_positions p
USING public.sales s
WHERE  p.sales_id = s.id
  AND  s.period >= :start AND s.period < :end;

DELETE FROM public.sales
WHERE period >= :start AND period < :end;
COMMIT;
```

После DELETE — запуск `ETLEngine(mode='full_period', start_date, end_date)`.

## Режим B: `incremental_fresh`

Используется для:
- автоматического 24/7 потока изменений из 1С через `bd_retail`

### Контракт

1. Watermark:
   - сначала `MAX(retail_updated_at)` из нашей `public.sales`
   - если NULL → `MAX(retail_snapshot_at)`
   - если и его нет (старая БД до миграции 006) → `MAX(updated_at)` (legacy)
   - default `1970-01-01`
2. **Overlap −5 минут** (`DataChecker.WATERMARK_OVERLAP`): `from_ts = watermark − 5min`. Это компенсирует read-skew между retail и нашей БД. Upsert по `(recorder, recorder_type[, line_no])` идемпотентен — пересчёт безопасен.
3. Окно: `[from_ts, to_ts)`, где `to_ts = now() − 5 секунд` (`READ_SKEW_GUARD`).
4. Запрос к retail:
   ```sql
   SELECT retail_uuid AS uid, updated_at
   FROM   bd_retail.public.sales
   WHERE  updated_at >= :from_ts AND updated_at < :to_ts
   ```
5. Для каждого `retail_uuid` — перегрузка соответствующего документа из MSSQL.
6. В `public.sales.retail_updated_at` пишется **именно retail.updated_at этой конкретной записи** (per-row).
7. В `public.sales_positions.retail_updated_at` — то же значение, что у родительского `sales` (через map `recorder → updated_at`).
8. `retail_snapshot_at` запуска можно проставить `MAX(retail_updated_at)` batch'а (опционально).
9. `etl_updated_at = now()`.
10. `pg_advisory_lock(register_id)` защищает от параллельных incremental.
11. Missing recorders (есть в retail, нет в MSSQL → удалён в 1С) → DELETE из dim/fact.
12. `load_history.checkpoint_value`:
    `watermark_from=<old>; to=<now-skew>; overlap=5min; max_retail_updated_at=<new>; changes=<n>`
13. **Добор хвоста** (`DataChecker.TAIL_LOOKBACK`, с 2026-07-22; **15 суток** с 2026-08-18):
    помимо окна, каждый тик перепроверяются uid из retail за последние TAIL_LOOKBACK,
    отсутствующие в DWH (анти-джойн по `recorder`). Зачем: данные появляются в MSSQL
    позже сигнала retail — overlap 5 мин не спасал, документы терялись навсегда
    (~23% оборота, см. [[Инкремент терял документы из-за лага проведения 1С больше overlap]]
    и `docs/audits/sales_aggregate_recon_2026-07-21.md`). Хвостовые документы пишутся со
    своей старой retail-датой и watermark не двигают; не-продажи сами выходят из окна.

    Почему 48ч заменены на 15 суток: 2026-08-13 потеряно 30 чеков склада Шиели — магазин
    синхронизировался с 1С на третьи сутки, окно уже закрылось; соседние дни того же
    склада догнались с лагом 39ч и 16ч. Цена расширения: потолок кандидатов ~23 тыс. вместо
    ~3.7 тыс., это один индексный анти-джойн (тик с пустым хвостом ~1.5-3 с).
    **Хвост не лечит** ретро-перепроведение (у документа сменился `period`) и правку
    сумм в существующих строках — retail о них не сигналит, их закрывает только
    `sales_recon.py` + пересборка.

## Что запрещено

- ❌ `TRUNCATE` в любом DAG (только CLI с явным флагом `--truncate` для инициализации/аварии).
- ❌ Любой `DELETE` без ограничения `period >= :start AND period < :end`.
- ❌ Загрузка >45 дней одним прогоном без флага `allow_large_period=true`.
- ❌ Использование `updated_at` (без префикса) в новом коде. Это legacy-поле.
- ❌ Полная пересборка всей истории как DAG по расписанию. Только CLI `run_full_period.py`, разово.

## Глоссарий watermark'ов

| Где | Имя | Как двигается |
|---|---|---|
| dim таблица (`public.sales`) | `retail_updated_at` | per-row при incremental, NULL при full_period |
| dim таблица | `retail_snapshot_at` | один на прогон, при full_period и (опционально) incremental |
| `load_history.checkpoint_value` | строка | аудит-запись, не источник правды |
| `DataChecker.get_last_update()` | строка | `MAX(retail_updated_at) → retail_snapshot_at → updated_at → 1970` |

## Связанные файлы

- [[etl_engine]] — `_run_full_period`, `_run_incremental`, `_process_target*`, `_validate_full_period_load`
- [[data_checker]] — `get_last_update`, `get_changed_uids`, `_get_tail_uids`, константы `WATERMARK_OVERLAP`, `READ_SKEW_GUARD`, `TAIL_LOOKBACK`
- `docs/audits/sql/sales_recon.py` — еженедельная проверка дельты витрины с 1С
- [[006_etl_audit_columns]] — миграция, создавшая три новых поля
- [[dao]] — `SYSTEM_COLS` (Sync игнорирует все 4 audit-колонки)
- `dags/core/tools/run_full_period.py` — CLI для ручного запуска
