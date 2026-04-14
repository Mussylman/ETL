---
tags: [telegram, мониторинг, DAG]
date: 2026-04-10
---
# Telegram уведомляет о статусе заказов

## DAG: check_orders_dag
- Расписание: `0 7 * * *` (каждый день в 7:00)
- Файл: `dags/orders_check_dag.py`

## Flow
1. Читает заказы из PostgreSQL (`conn_inventory`) за вчера
2. Фильтрует: status=3 (успешные) vs остальные
3. Формирует сводку и отправляет в Telegram

## Telegram Logger
- Файл: `dags/helpers/telegram_loggerr.py`
- Использует Airflow Variables: `telegram_bot_token`, `telegram_chat_id`
- Разбивает длинные сообщения (>4096 символов)

## Ссылки
- [[Деплой и конфигурация Airflow]]
