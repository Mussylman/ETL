---
tags: [etl-config, веб-приложение, FastAPI]
date: 2026-04-10
---
> **Обновлено 2026-09-28:** TEST-экземпляр :5555 удалён; конфигуратор работает одним экземпляром PROD :5556
> (`ETL_CONFIG_DB_NAME=etl_prod` обязателен). База `test` — пассивная архивная. См. `etl_config_app/RUNNING.md`.

# ETL Config App управляет конфигурацией на порту 5555

## Назначение
Standalone веб-конфигуратор ETL pipeline. Аналитик настраивает выгрузку данных из 1С в PostgreSQL без написания кода.

## Стек
FastAPI + Jinja2 + psycopg2, порт 5555

## Файлы
- `etl_config_app/app.py` — 30+ endpoints
- `etl_config_app/dao.py` — PostgreSQL etl_meta CRUD
- `etl_config_app/onec_client.py` — 1С HTTP API
- `etl_config_app/mssql_client.py` — MSSQL запросы
- `etl_config_app/templates/` — 7 шаблонов
- `etl_config_app/static/` — CSS + JS

## Три вкладки
1. **Источники** — добавление/discover таблиц 1С + VT
2. **Колонки** — Column Builder: target-схема + маппинг source-полей
3. **Статус** — pipeline overview, SQL preview, история загрузок

## Ключевые фичи
- Auto-discover: `_RecorderTRef` → MSSQL → 1С API → документы + VT
- MSSQL типы через INFORMATION_SCHEMA → авто target_type + transform
- fields_cache JSONB — кэш полей с типами
- Union/Target/Sync — автоматика, скрыта от пользователя

## Запуск
```bash
cd etl_config_app && uvicorn app:app --host 0.0.0.0 --port 5555 --reload
```

## Ссылки
- [[Схема базы данных etl_meta]]
- [[1С загружается через HTTP meta API]]
- [[Binary данные конвертируются по длине байтов]]
