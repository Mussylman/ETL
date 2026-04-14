import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook


def test_query():
    conn = BaseHook.get_connection("conn_inventory")
    engine = create_engine(
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )

    date = "2025-09-15"

    query = f"""
        SELECT *
        FROM public.orders
        WHERE created_at::date = '{date}'
        ORDER BY created_at ASC
    """

    # 🔹 Используем raw psycopg2 connection
    with engine.connect() as connection:
        raw_conn = connection.connection
        df = pd.read_sql(query, con=raw_conn)

    print(f"Получено строк: {len(df)}")
    print(df.head())


if __name__ == "__main__":
    test_query()
