---
tags: [решение, ui, column-builder]
date: 2026-04-22
---
# Column Builder — 3 макета pipeline / kanban / list

## Проблема
Изначальный Column Builder был плоской таблицей с дропдаунами. Аналитику непонятно:
- Откуда приходит колонка (header / VT / accum)
- Куда она попадёт (dim / fact)
- Что хранится в union
- Что с покрытием по источникам

## Решение
Три переключаемых макета, дефолтный — **Pipeline**.

## Pipeline (по умолчанию)
Три рельсы слева направо — логика потока данных:

1. **Слева: Источники** — sticky rail с группами источников (header / VT / accum). Каждое поле draggable. Dot indicator показывает mapped/unmapped.
2. **По центру: Канбан** — 3 колонки (Не распределено / dim target / fact target). Богатые карточки: имя, тип, 1С-имя, transform, badges по source_type, coverage bar/dots, error strip для computed.
3. **Справа: Целевые таблицы** — sticky rail со статистикой, preview union output columns, кнопка SQL превью.

Drag:
- Field → kanban column → создаёт маппинг + assign target
- Card → другая kanban колонка → меняет target

## Kanban (4 колонки вширь)
Без rails — только канбан. Для режима "только перетаскивание".

## List (таблица)
Классический табличный вид с дропдаунами. Для массового обзора.

## Toggle и persistence
`cbView` в localStorage (`cb_view`). `cbCoverageMode` (bar/dots/hidden) и `cbDensity` (roomy/compact) тоже.

## Ключевые файлы
- `etl_config_app/templates/registers/detail.html` — все 3 макета в одной вкладке #columns
- `etl_config_app/static/css/style.css` — CSS классы `.cb-stage`, `.rail`, `.kbn-*`, `.tgt-*`, `.cb-list-table`
- `assets/colors_and_type.css` — дизайн-токены (импорт `colors_and_type.css` в Claude Design system)

## Почему не выбрали Drag-n-drop библиотеку
HTML5 DnD достаточно для простых кейсов, библиотеки (react-dnd, sortable) добавляют bundle без реальной выгоды в этом проекте.

## Ссылки
- [[Split dim-fact по priority и include_columns]]
- [[Custom Python Transform для вычисляемых колонок]]
