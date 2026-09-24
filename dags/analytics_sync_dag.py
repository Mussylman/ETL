"""
analytics_sync — единый DAG аналитического слоя: источники → ClickHouse.

DAG = только оркестрация. Он не знает ни таблиц, ни источников: группы, которые он
ведёт, и их порядок — в etl_meta.ch_sync_group (dag_id = 'analytics_sync'), состав
групп — в etl_meta.ch_sync. Одна стабильная задача: группы читаются в момент выполнения
и идут по position (справочники раньше фактов), внутри группы — по priority (заказы
раньше продаж). Правка конфигурации не меняет структуру DAG.

Режим прогона выбирается по времени запуска (Asia/Almaty):
    каждые 5 минут       — patch: набор изменений retail + хвост, патч документов;
    в начале часа        — hot:   пересборка текущего и прошлого месяца из 1С —
                                  слепая зона retail (документы без сигнала, правки
                                  некассовых документов);
    ночью (03:00)        — sweep: сверка всей истории с 1С, несошедшиеся месяцы
                                  пересобираются.
Для обобщённых источников (postgres / mssql) режим не важен.

Прогоны одного источника не пересекаются: блокировка источника в control plane
(pg_advisory_lock(int,int)), max_active_runs=1. Плохая партиция не публикуется —
задача падает, цель остаётся прежней, причина в ch_sync_history.

Создаётся на паузе. Пока группа прямого пути в shadow, DAG пишет только shadow-таблицы.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DAGS_PATH = "/home/dev/airflow/dags"
DAG_ID = "analytics_sync"
CONFIG_CONN = "etl_prod"
CH_CONN = "clickhouse_etl"
SWEEP_HOUR = 3          # первый прогон этого часа — sweep, остальных часов — hot


def pick_mode(logical_date) -> str:
    """Режим по времени запуска в бизнес-часовом поясе."""
    from zoneinfo import ZoneInfo
    t = logical_date.astimezone(ZoneInfo("Asia/Almaty"))
    if t.minute < 5:
        return "sweep" if t.hour == SWEEP_HOUR else "hot"
    return "patch"


def _run(**context):
    import sys
    if DAGS_PATH not in sys.path:
        sys.path.insert(0, DAGS_PATH)
    from core.clickhouse.runner import run_dag
    mode = pick_mode(context["logical_date"])
    rep = run_dag(DAG_ID, mode=mode, config_conn_id=CONFIG_CONN, ch_conn_id=CH_CONN)
    for g, r in rep["groups"].items():
        print(f"{g} [{mode}]: объектов {r['objects']}")
        for x in r.get("results", []):
            print("   ", {k: v for k, v in x.items() if k != "published"})
    return {"mode": mode, "groups": list(rep["groups"])}


with DAG(
    dag_id=DAG_ID,
    description="Источники → ClickHouse: патч / горячее окно / ночная сверка",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 9, 24),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["clickhouse", "analytics"],
) as dag:
    # одна стабильная задача: группы и их порядок читаются в момент выполнения
    PythonOperator(task_id="sync", python_callable=_run, execution_timeout=timedelta(hours=3))
