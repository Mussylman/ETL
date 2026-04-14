import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
from airflow.models import Variable
from plugins.gfk_client import GFK

local_tz = pendulum.timezone("Asia/Almaty")

def etl_gfk(period: str = None, **context):
    period = period or Variable.get("gfk_period", default_var=None)

    gfk = GFK()
    sales, product = gfk.get_reportId(period)
    print(f"Sales report: {sales} | Product report: {product} | Period param: {period}")

    if sales:
        df_sales = gfk.read_csv_file(sales, "sales")
        gfk.insert_to_db(df_sales, "gfk_csv", "sales", mssql_conn_id="etl_gfk")

    if product:
        df_product = gfk.read_csv_file(product, "product")
        gfk.insert_to_db(df_product, "products", "product", mssql_conn_id="etl_gfk")

    if not sales and not product:
        print("⚠️ Отчёты не найдены")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gfk_update",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule="0 7 * * *",
    catchup=False,
    tags=["gfk", "etl", "mssql"],
) as dag:

    run_etl = PythonOperator(
        task_id="gfk_etl",
        python_callable=etl_gfk,
    )
