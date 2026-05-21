---
tags: [сессия]
date: 2026-04-22
---
# Сессия 2026-04-22

## Изменено
- [[Custom Python Transform для вычисляемых колонок]] — создана (новый паттерн)
- [[Split dim-fact по priority и include_columns]] — создана (новый паттерн)
- [[Column Builder — 3 макета pipeline kanban list]] — создана (новый UI)
- [[Текущие приоритеты]] — обновлены

## Сделано за день
1. **Custom Python Transforms** — реестр функций, UI-модалка, валидация `uses_columns`
2. **Split dim/fact** — `target_role` в БД, `include_columns`, `post_load_sql`, auto-привязка по source_type
3. **Kanban UI v1** — 3 колонки с drag&drop
4. **Claude Design integration** — подключён репо к claude.ai/design, создана DataPipe Design System
5. **Pipeline UI v2 (из Claude Design)** — 3-lane макет с source rail / kanban / targets rail, с drag полей из источников

## Найдено
- В регистре 21 колонка `kolichestvo` привязана к `ДокументОснование`. Поле `Количество` есть только в VT. Нужно пересоздать.
- `NOT NULL` на `source_column` в `column_mappings` — computed mappings используют placeholder `__computed__<func>`.

## Коммиты
- `e7bca45` Custom Python Transforms + dim/fact split + Kanban UI (pushed to main)

## Открытые вопросы
- `post_load_sql` для resolve FK `dim_id` в `fact_sales_products` не написан (оставлено на след. день)
- Drag-поля из source rail пока создают маппинг через `POST /sources/{id}/mappings/batch` — проверить что UI обновляется корректно
- Vector-версия custom transforms — отложено до миллионов строк

## На завтра
- Дотестировать pipeline UI
- Написать `post_load_sql` авто-генератор для dim-fact пар
- Протестировать полный flow на регистре 22 (с чистого листа)
