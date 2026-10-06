> **УСТАРЕЛО (2026-09-28), ROLLBACK_KEEP / только TEST-контур.** Описывает пересборку фактов PostgreSQL.
> На PROD факты `public.sales*` / `orders*` заморожены с 2026-09-24 как копия для отката; аналитика — ClickHouse
> через `analytics_sync`. `ops/rebuild_full_truncate.sql` удалён (обнулял реестр `dim_*` c RESTART IDENTITY);
> `rebuild_sales` на PROD отказывает до любого шага. Пересборка ClickHouse:
> `ch_sync --config-conn etl_prod --group onec_1c --mode rebuild --partition YYYYMM --apply`. См. CLAUDE.md.

# Полный пересбор витрины продаж с нуля (тестовая база)

Порядок жёсткий. Каждый шаг запускается вручную, следующий — только после проверки предыдущего.
Все команды из корня `/home/dev/airflow` с активированным venv:

```bash
source venv/bin/activate
```

---

## ШАГ 0 (ОБЯЗАТЕЛЬНЫЙ). Пауза инкрементального DAG

```bash
airflow dags pause sales_incremental_5min
airflow dags list-runs -d sales_incremental_5min --state running    # убедиться, что активных нет
```

**Почему без этого нельзя.** После TRUNCATE все три watermark-колонки `sales`
(`retail_updated_at`, `retail_snapshot_at`, `updated_at`) пусты, и `get_last_update()`
падает на `DEFAULT_SINCE = 1970-01-01` (`dags/core/extract/data_checker.py:53`).
Тик, попавший в окно между шагом 1 и шагом 3, возьмёт окно с 1970 года и потянет
всю retail-таблицу (~1.3 млн строк) при `execution_timeout=10 мин`.

Снимаем паузу только на ШАГЕ 6.

---

## ШАГ 1. Схема и очистка

### 1a. Миграция 009 (retail_updated_at + индекс)
```bash
psql -h 10.10.1.142 -U airflow_admin -d test -f dags/core/migrations/009_dim_retail_updated_at.sql
```
Идемпотентна (`ADD COLUMN IF NOT EXISTS`), повторный запуск безопасен.

### 1b. TRUNCATE витрины
```bash
# (скрипт ops/rebuild_full_truncate.sql удалён 2026-09-28 — обнулял реестр dim_* с RESTART IDENTITY)
```

**Что труним (10 таблиц):**

| Категория | Таблицы |
|---|---|
| Факты | `sales_positions`, `sales` |
| Справочники (8) | `dim_product`, `dim_warehouse`, `dim_counterparty`, `dim_department`, `dim_organization`, `dim_contract`, `dim_responsible_person`, `dim_quality` |

**Что НЕ труним:** `order`, `order_positions` (другой регистр, пусты, вне scope);
`salesTEST_dim`, `salesTEST_pos`, `wt_sales`, `wt_sales_positions`, `stock_positions` (legacy).

`RESTART IDENTITY` сбрасывает счётчики id — допустимо только потому, что пересобирается
всё согласованно. FK на эти таблицы отсутствуют (проверено в `pg_constraint`), `CASCADE` не нужен.

**Контроль:** скрипт сам печатает счётчики — все должны быть 0.

---

## ШАГ 2. Заливка справочника номенклатуры из retail

```bash
PYTHONPATH=dags python3 -m core.tools.load_products_from_retail --dry-run     # сначала так
PYTHONPATH=dags python3 -m core.tools.load_products_from_retail --skip-migration
```
(`--skip-migration`, потому что 009 уже применена на шаге 1a.)

Заливает ~124 тыс. строк: `guid`, `name`, `code`, `is_stub=false`, `retail_updated_at`.
Таблица пуста, поэтому все строки идут по ветке INSERT; проверка «id не изменились»
пройдёт тривиально (сравнивать не с чем).

**Контроль:**
```sql
SELECT count(*), min(retail_updated_at), max(retail_updated_at),
       count(*) FILTER (WHERE is_stub) AS stub
FROM public.dim_product;
```
Ожидаем: ~124 тыс. строк, `stub = 0`, `max(retail_updated_at)` ≈ текущее время —
**это стартовая метка справочника**, от неё поедет будущий инкремент справочников.

---

## ШАГ 3. Пересбор фактов из 1С

```bash
PYTHONPATH=dags python3 -m core.tools.rebuild_sales --start 2026-03-01 --from-scratch
```

**Про период.** В прежней витрине было 124 021 документ с 2026-06-15 и **всего 3 документа
раньше** (2026-03-03 … 2026-05-31). `--start 2026-03-01` захватывает всё, что было.
Если эти 3 документа не нужны — ставьте `--start 2026-06-15`, будет быстрее.

`--from-scratch` здесь безопасен и уместен: он труним **только факты** (справочники
защищены в `truncate_facts()` списком из `register_targets` + запретом префикса `dim_`),
а витрина после шага 1 и так пуста. Флаг оставляет поведение идемпотентным.

Оркестратор выполнит: миграции → Sync → 007 → truncate фактов → full_period под
advisory-локом → `load_dim_names` → `sales_recon --strict`. Ориентир: 6–10 минут.

**Важно:** факты сами создадут stub-строки в остальных семи справочниках по guid из 1С
(`post_load_sql`), и им же проставят `*_id`. Для `dim_product` stub'ы почти не появятся —
guid уже залиты из retail (покрытие 100%).

Критерий успеха — шаг 8 оркестратора: `sales_recon --strict` в ноль. Иначе сборка падает
с кодом 1 и баннером «СБОРКА ПРОВАЛЕНА».

---

## ШАГ 4. Имена в остальные справочники

Оркестратор уже вызвал `load_dim_names` на шаге 7. Отдельный прогон нужен, только если
шаг 3 запускался чем-то другим (`run_full_period.py`) либо остались stub'ы:

```bash
PYTHONPATH=dags python3 -m core.tools.load_dim_names
```

`dim_product` пропустится сам («нечего обогащать») — там `is_stub=false` из retail.
Остальные семь получат имена из `_Reference*` по guid.

---

## ШАГ 5. End-to-end проверка

```bash
# (скрипт ops/rebuild_full_verify.sql удалён 2026-09-28 вместе с truncate)
```

Пять блоков:
1. **наполнение** — все таблицы > 0, stub'ы в норме;
2. **orphan** — 10 проверок «факт ссылается на несуществующий id», все = 0;
3. **резолв FK** — «guid есть, id нет», все = 0;
4. **метки** — `MAX(retail_updated_at)` у products (из retail) и `MAX(retail_snapshot_at)`
   у sales (момент прогона). У `sales.retail_updated_at` ожидается NULL — это норма
   после full_period, per-row метки появятся с первыми тиками;
5. **дубли** по natural-ключам — все = 0.

Дополнительно — сверка с 1С (её же гоняет шаг 3, но можно отдельно):
```bash
python3 docs/audits/sql/sales_recon.py --start 2026-06-15 --strict; echo "exit=$?"
```

---

## ШАГ 6. Снять паузу с DAG

```bash
airflow dags unpause sales_incremental_5min
```

Через 5 минут проверить первый тик:
```sql
SELECT id, status, rows_loaded, left(checkpoint_value, 120)
FROM etl_meta.load_history
WHERE register_id = 62 AND run_mode = 'incremental'
ORDER BY id DESC LIMIT 3;
```

**От какой метки поедет инкремент.** `retail_updated_at` в `sales` после full_period пуст,
поэтому `get_last_update()` возьмёт второй приоритет — `MAX(retail_snapshot_at)`, то есть
момент старта пересбора. Записи, изменённые в retail до этого момента и не попавшие в
загруженный период, основное окно не увидит; их подбирает добор хвоста (`TAIL_LOOKBACK`,
15 суток). Первый тик после снятия паузы будет крупнее обычного — это ожидаемо.

---

## Откат

Отдельного отката нет: TRUNCATE необратим, восстановление = повторный прогон шагов 1–5.
Данные при этом не теряются — оба источника (1С и retail) остаются нетронутыми, витрина
собирается из них полностью.

Если пересбор прервётся между шагами — витрина остаётся неконсистентной, но DAG на паузе
(шаг 0), поэтому ничего не усугубляется: просто продолжите с прерванного шага.
