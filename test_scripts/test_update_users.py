import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

def update_dim_user_name():
    sheet_id = "1Hp_w2xAIygucF76mQsEIABNjvc3TKOjlZ2ppSJfAb0E"
    gid = "265148252"
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

    hook = MsSqlHook(mssql_conn_id="powerbi_connect")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        df = pd.read_csv(url)
        df.columns = ['email', 'employee', 'division']
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        cursor.execute("DELETE FROM dim_user_name")
        conn.commit()

        rows = df[['email', 'employee', 'division']].values.tolist()
        cursor.executemany(
            "INSERT INTO dim_user_name (email, employee, division) VALUES (%s, %s, %s)",
            rows
        )
        conn.commit()

    finally:
        conn.close()

if __name__ == "__main__":
    update_dim_user_name()
    print("✅ Скрипт отработал")
