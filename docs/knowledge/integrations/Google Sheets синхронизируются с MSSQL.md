---
tags: [google-sheets, интеграция, DAG]
date: 2026-04-10
---
# Google Sheets синхронизируются с MSSQL

## DAG: update_users_and_products
- Расписание: `0 6 * * *` (каждый день в 6:00)
- Файл: `dags/update_users.py`

## Два потока (параллельно)

### 1. dim_user_name
- Sheet: `1ywf51nKE2T69DN48XI6LGVAYr9egMZZAHDYA7-MsgeE` (GID: 265148252)
- Колонки: email, employee, division, division_id
- Target: MSSQL `dim_user_name` (powerbi_connect)
- Стратегия: DELETE ALL → INSERT

### 2. dim_asp_products
- Sheet: `1PVtxjpDIG332mr4Qo2vlB6XPsRe80StvlYpMDq9s7MY`
- Колонки: product_id, max_asp, start_date, end_date
- Target: MSSQL `dim_asp_products` (powerbi_connect)
- Стратегия: DELETE ALL → INSERT
- Трансформации: даты DD.MM.YYYY, числа с пробелами

## Экспорт
Google Sheets → CSV через URL:
```
https://docs.google.com/spreadsheets/d/{id}/export?format=csv&gid={gid}
```

## Ссылки
- [[Деплой и конфигурация Airflow]]
