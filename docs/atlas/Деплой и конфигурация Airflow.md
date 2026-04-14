---
tags: [деплой, airflow, конфигурация]
date: 2026-04-10
---
# Деплой и конфигурация Airflow

## Executor
**LocalExecutor** — задачи выполняются в процессах на одном сервере.

## Ключевые настройки (airflow.cfg)
- DAGs: `/home/dev/airflow/dags`
- Plugins: `/home/dev/airflow/plugins`
- Logs: `/home/dev/airflow/logs`
- DB: `postgresql+psycopg2://airflow_admin:1234Aa@10.10.1.142:5432/airflow`
- Broker: `redis://redis:6379/0`
- Timezone: Asia/Almaty

## Процессы
```
airflow-scheduler  (PID файл: airflow-scheduler.pid)
airflow-triggerer
airflow-dag-processor
uvicorn app:app --port 5555  (ETL Config App)
obsidian --no-sandbox --port 8090
```

## DAGи и расписание

| DAG ID | Расписание | Назначение |
|---|---|---|
| gfk_update | 0 7 * * * | GFK отчёты → MSSQL |
| check_orders_dag | 0 7 * * * | Заказы → Telegram |
| update_users_and_products | 0 6 * * * | Google Sheets → MSSQL |

## ETL Config App
Запуск: `cd etl_config_app && uvicorn app:app --host 0.0.0.0 --port 5555 --reload`

## Ссылки
- [[Стек технологий и инфраструктура]]
