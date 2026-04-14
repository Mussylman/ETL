import sys
import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import pendulum

local_tz = pendulum.timezone("Asia/Almaty")
# Добавляем helpers в PYTHONPATH
sys.path.append("/home/dev/airflow")

from helpers import log_setup  # твой кастомный логгер
from helpers.telegram_loggerr import create_telegram_logger


# === Переменные Airflow ===
bot_token = Variable.get("telegram_bot_token")  # токен бота
chat_id = Variable.get("telegram_chat_id")      # чат ID для сообщений

# === Логгер Telegram ===
telegram_logger = create_telegram_logger(bot_token, chat_id)


# === Аргументы по умолчанию для DAG ===
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# === Функции ===
def get_data_inventory(date):
    conn = BaseHook.get_connection("conn_inventory")
    engine = create_engine(
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )

    query = f"""
        SELECT *
        FROM public.orders
        WHERE created_at::date = '{date}'
        ORDER BY created_at ASC
    """

    with engine.connect() as connection:
        raw_conn = connection.connection  # psycopg2 connection
        df = pd.read_sql(query, con=raw_conn)

    return df




def log_order_info(start_date, successful_order, unsuccessful_order):
    logging.info(f"=== Информация о заказах на {start_date} ===")
    logging.info(f"✅ Успешные заказы: {successful_order}")
    logging.info(f"❌ Неуспешные заказы: {unsuccessful_order}")


def run_check_orders(**kwargs):
    """Основная логика проверки заказов"""
    logging.info("Запуск логики проверки заказов")

    start_date = datetime.strptime(kwargs["ds"], "%Y-%m-%d").date()

    df = get_data_inventory(start_date)

    successful_order = (df.status == 3).sum()
    unsuccessful_order = (df.status != 3).sum()

    log_order_info(start_date, successful_order, unsuccessful_order)

    message = (
        f"📦 Информация о заказах на {start_date}\n"
        f"✅ Успешные: {successful_order}\n"
        f"❌ Неуспешные: {unsuccessful_order}"
    )
    telegram_logger.info(message)


# === Определение DAG ===
with DAG(
    dag_id="check_orders_dag",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule="0 7 * * *",   # каждый день в 07:00 UTC
    catchup=False,
    default_args=default_args,
    tags=["orders", "telegram"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_orders = PythonOperator(
        task_id="check_orders",
        python_callable=run_check_orders,
    )

    end = EmptyOperator(task_id="end")

    start >> check_orders >> end
