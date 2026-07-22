---
tags: [решение, архитектура, конфигуратор, wizard]
date: 2026-06-10
---
# Wizard'ы по типам 1С строятся над единым RegisterSpec и одним writer

Дизайн-сессия 2026-06-10 (без кода). Полный план — в логе сессии
[[2026-06-10 редизайн конфигуратора на wizard'ы]]. Здесь — суть решения.

## Решение

1. **Один канонический config-model — `RegisterSpec`** (Pydantic, etl_config_app):
   декларативное описание регистра целиком (register + sources + mappings +
   unions + members + targets). Зеркало `RegisterConfig` движка + UI-поля
   (onec_name, fields_cache).
2. **Один writer — `spec_writer.write(spec)`**: единственная точка записи
   в etl_meta, одна транзакция (сейчас dao коммитит каждый statement —
   упавший на середине wizard оставляет мусор).
3. **Один валидатор контракта** перед записью: upsert_keys ⊆ колонки target,
   include_columns ⊆ target_columns маппингов, load_mode=upsert ⇒ upsert_keys
   непусты, join_type ∈ {INNER, LEFT, RIGHT}, incremental ⇒ retail_table+uid.
4. **Один Sync**, дополненный контрактным DDL: сейчас `compute_sync_plan`
   создаёт таблицу БЕЗ PK/UNIQUE — upsert ON CONFLICT на такой таблице падает
   (эталон sales потому и создавался руками). Sync обязан создавать:
   `id` PK, UNIQUE по upsert_keys, `updated_at` у dim, FK-колонку fact→dim.
5. **Wizard'ы — тонкие фронты**: собирают ответы аналитика, серверный
   генератор (`spec_from_reference`, `spec_from_document`, `spec_from_accumrg`,
   `spec_from_inforg`) порождает RegisterSpec, фронт показывает diff/превью,
   submit одним вызовом `POST /api/registers/spec`.

## Порядок этапов

Этап 0 (фундамент: spec+writer+валидатор+Sync-DDL+минимальный патч движка)
→ A′ (перевод текущего wizard на writer + фиксы) → B (Справочник → dim)
→ C (Документ+VT → dim+fact, UNION как опция) → D (InfoRg). AccRg — пропуск.

## Минимальный патч движка (без него контракт невыполним, ~40 строк)

- `etl_engine.py:223` — `etl_table="public.sales"` захардкожен: incremental
  любого регистра кроме sales берёт watermark из чужой таблицы. Брать из
  dim-target регистра.
- Режим `full` без периода (справочники не имеют `_Period`).
- `register_sources.period_column` (default `_Period`, у документов
  `_Date_Time`, у справочников NULL) — фильтр периода сейчас прибит к
  `_Period` (query_builder.py:189).
- Ключ missing-delete из upsert_keys dim вместо хардкода `"recorder"`.
- Нормализация join_type (баг «INNER JOIN JOIN», см.
  [[Латентные баги конфигуратора класса target_role]]).
- Новый transform `invert_bool` (для `_Folder`: 0x00 = группа).

## Почему не 5 wizard'ов = 5 путей записи

Сегодня в app.py **25 пишущих endpoint'ов** и минимум 5 независимых путей
создания маппингов; `_sync_union_and_target` вызывается не после всех →
дрейф union.output_columns ↔ include_columns. Каждый новый wizard поверх
этого умножал бы рассинхрон. Spec+writer сводит всё в одну точку,
CRUD-формы остаются как «экспертный режим» поверх того же writer.

## Контрактный тест

`write(spec)` → `ConfigLoader.load_register()` → поле-в-поле эквивалентность
+ снапшот SQL QueryBuilder + dry-run LIMIT 10 на MSSQL + загрузка в схему
etl_test. Регрессия: SQL регистра sales до/после патча движка идентичен.
