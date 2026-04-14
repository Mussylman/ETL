---
tags: [решение, безопасность, TODO]
date: 2026-04-10
---
# Пароли захардкожены в коде — нужно вынести в env

## Проблема
Пароли и подключения прописаны прямо в Python-файлах.

## Где
- `etl_config_app/dao.py`: PostgreSQL пароль `1234Aa`
- `etl_config_app/mssql_client.py`: MSSQL пароль `Zz123456`
- `airflow.cfg`: connection string с паролем

## Решение (TODO)
1. Создать `.env` файл (исключить из git)
2. Использовать `os.environ` или `python-dotenv`
3. Для Airflow — использовать Connections в UI

## Ссылки
- [[Все подключения к внешним системам]]
- [[Текущие приоритеты]]
