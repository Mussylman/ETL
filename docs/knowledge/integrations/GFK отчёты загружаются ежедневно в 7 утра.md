---
tags: [GFK, интеграция, DAG]
date: 2026-04-10
---
# GFK отчёты загружаются ежедневно в 7 утра

## DAG: gfk_update
- Расписание: `0 7 * * *` (каждый день в 7:00)
- Файл: `dags/gfk_report.py`

## Источник
GFK API: `https://startrack.mi.gfk.com/api/v1/Reports`
- Два типа: sales (CSV) и products (SSV)
- Период: недельный

## Flow
1. Получить report_id за период
2. Скачать CSV/SSV файл
3. Парсинг и трансформация (переименование колонок, типы)
4. Вставка в MSSQL:
   - `gfk_csv` — продажи (проверка дубликатов по периоду)
   - `products` — товары (полная перезагрузка + дедупликация по ID)
5. Batch insert по 5000 строк

## Подключение
- Connection: `etl_gfk` (MSSQL)

## Клиент
- `dags/plugins/gfk_client.py` — класс `GFK`

## Ссылки
- [[Деплой и конфигурация Airflow]]
