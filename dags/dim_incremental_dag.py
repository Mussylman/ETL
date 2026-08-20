"""
dim_incremental_15min — инкремент справочников из retail.

DAG намеренно ТУПОЙ: он знает только список таблиц и на каждую поднимает
отдельную таску, которая зовёт скрипт. Ни про ключ, ни про watermark, ни про
источник 1С DAG не знает — всё это скрипт читает из etl_meta
(dags/core/tools/load_dim_from_config.py, режим incremental).

Отдельная таска на таблицу, а не цикл в одной, чтобы в Airflow было видно
поштучно: какая прошла, какая упала и на чём. Падение одной не мешает остальным.

Расширение списка — ОДНА строка в DIM_TABLES. Условие попадания: у регистра
заполнен registers.retail_table (иначе watermark брать неоткуда) и retail
реально ведёт updated_at.

Почему 15 минут: справочники меняются на порядок реже продаж (замер 2026-08-20:
за час в retail.products изменились 2 записи, в warehouses/departments — ни одной).

Хвоста (tail lookback) здесь НЕТ и быть не должно: updated_at всегда ставит сам
retail текущим временем, задней даты не бывает — окно updated_at > watermark
ловит всё. См. память проекта: arch-dim-incremental-no-tail.

sales_incremental_5min этот DAG не трогает — факты отдельная тема.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DIM_TABLES = [
    "dim_nomenklatura",
    "dim_podrazdelenie",
    "dim_sklad",
]


def run_one_dim(dim_code: str):
    """
    Одна таблица — одна таска. Вся логика в скрипте, здесь только вызов.

    Импорт лениво: DAG-парсер Airflow не должен тянуть хуки и psycopg2.
    """
    import sys
    if "/home/dev/airflow/dags" not in sys.path:
        sys.path.insert(0, "/home/dev/airflow/dags")

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    from core.tools.load_dim_from_config import read_config, process

    pg = PostgresHook(postgres_conn_id="postgre_test_base")
    rt = PostgresHook(postgres_conn_id="bd_retail")
    ms = MsSqlHook(mssql_conn_id="mssql_1c_conn")

    cfg = read_config(pg, dim_code)
    if not cfg:
        raise RuntimeError(
            f"{dim_code}: нет конфига в etl_meta (registers / register_sources)")
    if not cfg.get("retail_table"):
        raise RuntimeError(
            f"{dim_code}: не задан registers.retail_table — watermark брать неоткуда")

    # Зовём process (а не process_incremental напрямую): он единая точка входа и
    # ведёт etl_meta.load_history — без этой записи UI-портал показывает
    # «никогда не запускался», хотя DAG отработал.
    res = process(dim_code, pg, rt, ms, mode="incremental", batch=500,
                  dry_run=False, set_mark=True)
    print(f"  {dim_code} DONE: {res}")
    return res


with DAG(
    dag_id="dim_incremental_15min",
    description="Инкремент справочников: retail сигналит изменения, поля приезжают из 1С",
    start_date=datetime(2026, 8, 20),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=10),
        "depends_on_past": False,
    },
    tags=["dim", "incremental", "retail"],
) as dag:
    for _code in DIM_TABLES:
        PythonOperator(
            task_id=f"sync_{_code}",
            python_callable=run_one_dim,
            op_args=[_code],
        )
