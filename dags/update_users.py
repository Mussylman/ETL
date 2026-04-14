from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum

local_tz = pendulum.timezone("Asia/Almaty")

# ==============================
# 1) Обновление dim_user_name
# ==============================
def update_dim_user_name():

    sheet_id = "1ywf51nKE2T69DN48XI6LGVAYr9egMZZAHDYA7-MsgeE"
    gid = "265148252"
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

    hook = MsSqlHook(mssql_conn_id="powerbi_connect")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        df = pd.read_csv(url)
        df.columns = ['email', 'employee', 'division', 'division_id']
        df['created_at'] = pd.Timestamp.now(tz=local_tz)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        cursor.execute("DELETE FROM dim_user_name")
        conn.commit()

        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO dim_user_name (email, employee, division, division_id, created_at) VALUES (%s, %s, %s, %s, %s)",
                (row['email'], row['employee'], row['division'], row['division_id'], row['created_at'])
            )


        conn.commit()

    finally:
        conn.close()


# ==============================
# 2)  dim_asp_products
# ==============================
def update_dim_asp_products():

    sheet_id = "1PVtxjpDIG332mr4Qo2vlB6XPsRe80StvlYpMDq9s7MY"
    gid = "0"
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

    hook = MsSqlHook(mssql_conn_id="powerbi_connect")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        df = pd.read_csv(url)

        # Убираем Unnamed и пустые
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
        df = df.dropna(axis=1, how="all")
        df = df.iloc[:, :4]

        df.columns = ["product_id", "max_asp", "start_date", "end_date"]

        # Чистим max_asp
        df["max_asp"] = df["max_asp"].astype(str).str.replace(r"\s+", "", regex=True)

        # Trim
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # ===== 📌 Преобразуем ДАТЫ =====
        df["start_date"] = df["start_date"].apply(lambda x: datetime.strptime(x, "%d.%m.%Y").date() if isinstance(x, str) and x.strip() else None)
        df["end_date"] = df["end_date"].apply(lambda x: datetime.strptime(x, "%d.%m.%Y").date() if isinstance(x, str) and x.strip() else None)


        # ===== 📌 Убираем ВСЕ nan ➜ None =====
        df = df.where(pd.notnull(df), None)

        # но max_asp остаётся строкой → убрать "nan"
        df["max_asp"] = df["max_asp"].apply(lambda x: None if x in ["nan", "None", ""] else x)

        # ===== Стираем таблицу =====
        cursor.execute("DELETE FROM dim_asp_products")
        conn.commit()

        # ===== Вставляем =====
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO dim_asp_products (product_id, max_asp, start_date, end_date)
                VALUES (%s, %s, %s, %s)
                """,
                (row["product_id"], row["max_asp"], row["start_date"], row["end_date"])
            )

        conn.commit()

    finally:
        conn.close()






# ==============================
# DAG
# ==============================
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="update_users_and_products",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule="0 6 * * *",
    catchup=False,
    tags=["mssql", "google_sheets", "etl"],
) as dag:

    update_users_task = PythonOperator(
        task_id="update_dim_user_name",
        python_callable=update_dim_user_name,
    )

    update_products_task = PythonOperator(
        task_id="update_dim_asp_products",
        python_callable=update_dim_asp_products,
    )

    # Параллельное выполнение (ничего связывать не нужно)
    # Если хочешь последовательно:
    # update_users_task >> update_products_task
