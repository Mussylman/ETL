"""
sales_incremental_5min — каждые 5 минут забирает изменения из bd_retail
и подтягивает соответствующие документы из MSSQL в public.sales / public.sales_positions.

Контракт (см. docs/sales_load_modes.md):
  • mode='incremental' → ETLEngine читает MAX(retail_updated_at) из public.sales
    как watermark, делает overlap -5 минут, идёт в bd_retail.public.sales
    за изменениями в окне [watermark, now-5sec).
  • Per-row retail_updated_at пишется в каждую строку sales/sales_positions.
  • post_load_sql после загрузки заполняет sales_id и все *_id dim-слоя
    (guid→id + stub при первой встрече нового объекта).
  • Параллельные запуски блокирует pg_advisory_lock(register_id=62).
  • load_history.status='failed' при любой ошибке → Airflow увидит failure.

Вторая задача — load_dim_names: post_load создаёт для нового объекта stub-строку
с id, но БЕЗ имени; имя приезжает из справочника 1С. Без этого шага витрина
копила безымянные строки до ручной пересборки (за сутки набегало ~770).
Задача дешёвая: если stub-строк нет, она не ходит ни в 1С, ни в meta API.

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


def load_dim_names():
    """
    Догрузка имён для stub-строк справочников.

    Не роняет DAG-run при недоступности 1С/meta API: факты к этому моменту уже
    загружены и watermark сдвинут, а имена — некритичный путь (на суммы и сверку
    с 1С не влияют, отсутствие имени видно как is_stub=true). Ошибки видны
    в логе и в счётчике failed; следующий тик попробует снова.
    """
    import sys
    if "/home/dev/airflow/dags" not in sys.path:
        sys.path.insert(0, "/home/dev/airflow/dags")
    from core.tools.load_dim_names import enrich_all

    totals = enrich_all(only_stub=True, raise_on_error=False)
    print(f"load_dim_names DONE: {totals}")
    if totals["failed"]:
        print(f"⚠ обогащение не прошло для {totals['failed']} справочников — "
              f"повторится на следующем тике")
    return totals


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
    load_facts = PythonOperator(
        task_id="run_sales_incremental",
        python_callable=run_sales_incremental,
    )

    # Имена справочников — отдельной задачей после фактов: у неё свой лимит
    # времени и своё падение, которое не откатывает уже загруженные данные.
    enrich_names = PythonOperator(
        task_id="load_dim_names",
        python_callable=load_dim_names,
        execution_timeout=timedelta(minutes=3),
    )

    load_facts >> enrich_names
