---
tags: [сессия, transforms, split, custom-python]
date: 2026-04-22
---
# 2026-04-22 — Custom transforms и split dim-fact

## Что сделано (на момент сессии — uncommitted)

### ETL Engine: split dim/fact
- `TargetConfig` расширен: `post_load_sql`, `include_columns`, `priority`, `target_role` (dimension/fact)
- `ETLEngine._get_active_targets` сортирует targets по `priority` — dim грузится первой
- Фильтрация `df` по `include_columns` перед loader.load — в dim попадают только колонки измерения, в fact — только факта
- `_execute_post_load_sql` вызывается после INSERT — место для резолва FK и cleanup'а
- `ConfigLoader` читает новые поля из `register_targets`

### Custom Python Transforms
- Новый модуль `dags/core/transform/custom.py`, 7 функций в `CUSTOM_TRANSFORMS`:
  - `sales_with_vat`, `sales_without_vat` (с корректировкой бонусов после 2022-10-12)
  - `earned_bonuses`, `used_bonuses` (знак по `_doc_sign(row)`)
  - `margin`, `unit_price`, `discount_percent`
- `TransformUtils.apply_transforms` разделяет column-level и row-level трансформации
- Row-level через `df.apply(axis=1)` — функция получает row как dict, возвращает значение

### ETL Config App
- `/api/custom-transforms` — реестр функций для UI
- `/api/registers/{id}/add-computed-column` — добавление вычисляемой колонки через UI
- Авто-sync `include_columns` в `sync_register_after_columns`:
  - `source_type='header'` → dimension target
  - `source_type='detail'` → fact target
  - `custom_python` колонки не трогаются автоматически (ручное назначение)
- Обновлены шаблоны `registers/detail.html` (+254) и `sources/detail.html` (+55) под новый UX

### Документация (vault)
- Новая decision-заметка: [[Custom Python Transform для вычисляемых колонок]]
- Новый pattern: [[Split dim-fact по priority и include_columns]]
- [[Схема базы данных etl_meta]] обновлена — добавлены поля `post_load_sql`, `include_columns`, `priority`, `target_role`
- [[Текущие приоритеты]] переписаны (закрытые перемещены в секцию "Закрыто")
- Debugging-заметка про duplicate key помечена решённой

## Что осталось
- Протестировать полный flow на реальных данных
- Написать конкретный `post_load_sql` для резолва `dim_id` в `fact_sales_products`
- Закоммитить всё (сейчас 9 файлов в working tree)
- При миграции на миллионные объёмы — переписать `df.apply(axis=1)` на векторный вариант

## Красные флаги
- `uses_columns` в реестре custom-трансформаций — декларация, не валидируется. Если аналитик удалит колонку, от которой зависит функция, упадёт в runtime.
- `df.apply(axis=1)` медленный. Для текущих объёмов (по тестам) нормально, для прода нужен бенчмарк.

## Ссылки
- [[Custom Python Transform для вычисляемых колонок]]
- [[Split dim-fact по priority и include_columns]]
- [[2026-04-14 Дизайн и план тестирования]]
