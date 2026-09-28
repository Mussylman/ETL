# Запуск конфигуратора

| Экземпляр | Порт | База etl_meta | Метка в UI |
|---|---|---|---|
| PROD | 5556 | `etl_prod` (`ETL_CONFIG_DB_NAME=etl_prod`) | красная `PROD / etl_prod` |

TEST-экземпляр :5555 (база `test`) удалён 2026-09-28: TEST-контур больше не нужен, база `test` сохранена
как пассивная — в неё ничего не пишет автоматически. Без `ETL_CONFIG_DB_NAME` конфигуратор не запускается.

## Переменные окружения (`dao.py`)
- `ETL_CONFIG_DB_NAME`, `ETL_CONFIG_DB_USER`, `ETL_CONFIG_DB_PASSWORD` — **обязательны**; `ETL_CONFIG_DB_HOST/PORT` — при необходимости.
- `ETL_CONFIG_RETAIL_DB_*`, `ETL_CONFIG_MSSQL_*` — retail и 1С; проверяются до подключения. Значений в коде нет.
- Полный список имён — `.env.example`; PROD-значения — `~/.config/etl_config/prod.env` (из Airflow connections).
- `ETL_CONFIG_ENV_LABEL` — текст метки (по умолчанию выводится из имени базы).
- `ETL_CONFIG_ALLOW_DESTRUCTIVE=1` — разрешить destructive DDL (DROP COLUMN, сужение типа, recreate) через
  кнопку Sync с подтверждением. По умолчанию выключено: такие изменения — миграцией.

## Команды
```bash
cd /home/dev/airflow/etl_config_app

# PROD: без --reload (правки кода применяются только явным рестартом).
# Секреты и база — из ~/.config/etl_config/prod.env (700/600, вне репозитория; имена — .env.example)
set -a; . ~/.config/etl_config/prod.env; set +a
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

## Wizard: новая витрина «документ → шапка + ТЧ» целиком через UI
См. `docs/knowledge/patterns/Витрина документа собирается через UI….md`. Ключевые точки: `pipeline_type` и retail-привязка
в форме регистра; `period_column` авто (`_Date_Time` у `_DocumentN`); таргет с role/parent/include/post_load;
`GET /api/targets/{id}/post-load-template` — шаблон резолва ссылок; `GET /api/sources/{id}/fields` — статусы полей по UPP_JAN.
