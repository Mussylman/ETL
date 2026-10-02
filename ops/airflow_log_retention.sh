#!/usr/bin/env bash
# Хранение логов Airflow — 3 суток. Запускается из crontab пользователя dev раз в час.
# Удаляет файлы логов задач и dag_processor старше 3 суток и опустевшие каталоги.
# etl_config_prod.log (лог PROD-конфигуратора, пишется живым процессом) не трогает.
# Причина: 2026-10-02 логи заполнили корневой раздел (98 ГБ) — встали Airflow и VS Code SSH.
set -euo pipefail
LOGS=/home/dev/airflow/logs
DAYS=3
find "$LOGS" -mindepth 2 -type f -mtime +"$DAYS" ! -name 'etl_config_prod.log' -delete
find "$LOGS" -mindepth 1 -type d -empty -delete
