# Запуск конфигуратора: два экземпляра, один код

| Экземпляр | Порт | База etl_meta/витрины | Метка в UI |
|---|---|---|---|
| TEST | 5555 | `test` (значения по умолчанию) | синяя `TEST / test` |
| PROD | 5556 | `etl_prod` (`ETL_CONFIG_DB_NAME=etl_prod`) | красная `PROD / etl_prod` |

Переключателя TEST/PROD внутри интерфейса нет намеренно: окружение задаётся только при запуске.

## Переменные окружения (`dao.py`)
- `ETL_CONFIG_DB_NAME` — база (`test` | `etl_prod`); `ETL_CONFIG_DB_HOST/PORT/USER/PASSWORD` — при необходимости.
- `ETL_CONFIG_ENV_LABEL` — текст метки (по умолчанию выводится из имени базы).
- `ETL_CONFIG_ALLOW_DESTRUCTIVE=1` — разрешить destructive DDL (DROP COLUMN, сужение типа, recreate) через
  кнопку Sync с подтверждением. По умолчанию выключено в обоих окружениях: такие изменения — миграцией.

## Команды
```bash
cd /home/dev/airflow/etl_config_app

# TEST (как исторически): auto-reload, читает код с диска
nohup /home/dev/airflow/venv/bin/uvicorn app:app --host 0.0.0.0 --port 5555 --reload \
      > /tmp/uvicorn.log 2>&1 &

# PROD: без --reload (правки кода применяются только явным рестартом)
ETL_CONFIG_DB_NAME=etl_prod ETL_CONFIG_ENV_LABEL="PROD / etl_prod" \
nohup /home/dev/airflow/venv/bin/uvicorn app:app --host 0.0.0.0 --port 5556 \
      > /home/dev/airflow/logs/etl_config_prod.log 2>&1 &

# рестарт PROD после изменения кода
pkill -f 'uvicorn app:app --host 0.0.0.0 --port 5556'; # затем команда запуска выше

# read-only план Sync (dry-run) по регистру — ничего не применяет
curl -s http://localhost:5556/api/registers/77/sync-plan | python3 -m json.tool | head -40
```

## Что защищает UI от опасных действий (dao.py / app.py)
- Save таргета — PATCH: поля, отсутствующие в форме, не меняются.
- Переименование таблицы/колонок на таблице с данными, удаление регистра с данными, удаление
  источника/union, которые питают таргет, — отказ 409 с объяснением.
- Destructive DDL применяется только при `ETL_CONFIG_ALLOW_DESTRUCTIVE=1`; иначе план показывается, но не исполняется.
- FK fact→dim именуется по правилу движка (`{dim}_id`, затем без «s»), физического FK нет; CREATE добавляет
  контрактные system-колонки (audit + `raw_refs`).
