"""
sales_incremental_5min — каждые 5 минут забирает изменения из bd_retail
и подтягивает соответствующие документы из MSSQL в public.sales / public.sales_positions.

Контракт (см. docs/sales_load_modes.md):
  • mode='incremental' → ETLEngine читает MAX(retail_updated_at) из public.sales
    как watermark, делает overlap -5 минут, идёт в bd_retail.public.sales
    за изменениями в окне [watermark, now-5sec).
  • Per-row retail_updated_at пишется в каждую строку sales/sales_positions.
  • post_load_sql после загрузки заполняет sales_id (FK) по recorder/recorder_type.
  • Параллельные запуски блокирует pg_advisory_lock(register_id=62).
  • load_history.status='failed' при любой ошибке → Airflow увидит failure.

НИКАКИХ truncate / delete-period в этом DAG. Только incremental upsert.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_sales_incremental():
    """Точка входа task. Импорт ETLEngine лениво — чтобы DAG-парсер был быстрым."""
    import sys
    if "/home/dev/airflow/dags" not in sys.path:
        sys.path.insert(0, "/home/dev/airflow/dags")
    from core.etl_engine import ETLEngine

    etl = ETLEngine(register_code="sales", mode="incremental")
    result = etl.run()
    print(f"sales_incremental DONE: {result}")
    return result


with DAG(
    dag_id="sales_incremental_5min",
    description="Свежесть данных: каждые 5 минут забираем изменения из bd_retail",
    start_date=datetime(2026, 6, 25),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
        "depends_on_past": False,
    },
    tags=["sales", "incremental", "fresh"],
) as dag:
    PythonOperator(
        task_id="run_sales_incremental",
        python_callable=run_sales_incremental,
    )
