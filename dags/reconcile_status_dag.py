"""
reconcile_status — статус сверки по месяцам текущего года для страницы «Сверка» конфигуратора.

Только чтение данных: 1С (NOLOCK), источники, ClickHouse; пишет одну таблицу etl_meta.reconcile_status.
Ничего не чинит — показывает, где расходится и кто должен исправить (patch / hot / sweep / цикл).

  каждый час в :40   — лёгкая проверка: строки + SUM мер по месяцу (~1.5 мин);
  раз в сутки ≥05:00 Алматы (после ночного sweep) — дополнительно полная: отпечаток onec_reconcile (~5 мин).

:40 — вне слота часовой пересборки analytics_sync (начало часа). Создаётся на паузе.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DAGS_PATH = "/home/dev/airflow/dags"
CONFIG_CONN = "etl_prod"


def _run(**context):
    import sys
    if DAGS_PATH not in sys.path:
        sys.path.insert(0, DAGS_PATH)
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from core.tools.reconcile_status import due_full, run
    modes = ["light"] + (["full"] if due_full(PostgresHook(postgres_conn_id=CONFIG_CONN)) else [])
    out = {}
    for m in modes:
        out[m] = run(CONFIG_CONN, m)
        print(m, out[m])
    return out


with DAG(
    dag_id="reconcile_status",
    description="Сверка 1С / источник ↔ ClickHouse по месяцам текущего года → страница «Сверка» конфигуратора",
    schedule="40 * * * *",
    start_date=datetime(2026, 10, 12),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 0},
    tags=["reconcile", "read-only"],
) as dag:
    PythonOperator(task_id="reconcile", python_callable=_run, execution_timeout=timedelta(minutes=40))
