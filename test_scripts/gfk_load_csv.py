"""
Загрузка sales-данных GFK из CSV-файлов (скачанных с портала) в MSSQL.

1. Читает все .csv файлы из папки gfk_csv_files/
2. Для каждого файла определяет периоды (недели)
3. Удаляет эти периоды из таблицы gfk_csv
4. Вставляет новые данные

Запуск:
    python test_scripts/gfk_load_csv.py

Файлы класть в: test_scripts/gfk_csv_files/
"""
import sys
import os
import glob
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

CSV_DIR = os.path.join(os.path.dirname(__file__), "gfk_csv_files")
TABLE_NAME = "gfk_csv"
MSSQL_CONN_ID = "etl_gfk"
BATCH_SIZE = 5000

RENAME_DICT = {
    "CONSUMER_BUSINE": "ConsumerBusiness",
    "Period": "Period",
    "POSType": "Postype",
    "REGION": "Region",
    "CITY": "City",
    "Retailer": "Retailer",
    "Channel": "Channel",
    "ID": "Id",
    "SALES UNITS": "SalesUnits",
    "SALES VALUE KZT": "SalesValue",
}


def read_sales_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    df = df.rename(columns=RENAME_DICT)

    for c in ["SalesUnits", "SalesValue"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if "Period" in df.columns:
        df["Period"] = (
            df["Period"]
            .astype(str)
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
        )

    return df.fillna("")


def main():
    if not os.path.isdir(CSV_DIR):
        os.makedirs(CSV_DIR)
        print(f"Создана папка: {CSV_DIR}")
        print("Положите туда CSV-файлы и запустите скрипт снова.")
        return

    csv_files = sorted(glob.glob(os.path.join(CSV_DIR, "*.csv")))
    if not csv_files:
        print(f"Нет CSV-файлов в {CSV_DIR}")
        return

    print(f"Найдено файлов: {len(csv_files)}")
    for f in csv_files:
        print(f"  - {os.path.basename(f)}")

    # Читаем все файлы в один DataFrame
    frames = []
    for filepath in csv_files:
        df = read_sales_csv(filepath)
        frames.append(df)
        print(f"\n{os.path.basename(filepath)}: {len(df)} строк")
        if "Period" in df.columns:
            periods = df["Period"].unique()
            print(f"  Периоды: {', '.join(periods)}")

    df_all = pd.concat(frames, ignore_index=True)

    if "Period" not in df_all.columns:
        print("Колонка 'Period' не найдена в данных!")
        return

    all_periods = df_all["Period"].unique().tolist()
    print(f"\n{'='*70}")
    print(f"Всего строк: {len(df_all)}")
    print(f"Периоды для загрузки ({len(all_periods)}): {', '.join(all_periods)}")
    print(f"{'='*70}")

    # Подключение к БД
    hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Получаем колонки таблицы
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = %s
            """,
            (TABLE_NAME,),
        )
        table_cols = {r[0] for r in cursor.fetchall()}

        if table_cols:
            skip = [c for c in df_all.columns if c not in table_cols]
            if skip:
                print(f"Колонки не в БД (пропущены): {', '.join(skip)}")
            df_all = df_all[[c for c in df_all.columns if c in table_cols]]

        # Удаляем существующие периоды
        for period in all_periods:
            cursor.execute(
                f"DELETE FROM {TABLE_NAME} WHERE LTRIM(RTRIM(Period)) = %s",
                (period,),
            )
            deleted = cursor.rowcount
            print(f"DELETE Period='{period}': удалено {deleted} строк")

        conn.commit()

        # Вставка батчами
        columns = ", ".join(df_all.columns)
        placeholders = ", ".join(["%s"] * len(df_all.columns))
        insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"

        data_tuples = list(df_all.itertuples(index=False, name=None))
        total = len(data_tuples)
        inserted = 0

        for i in range(0, total, BATCH_SIZE):
            batch = data_tuples[i : i + BATCH_SIZE]
            cursor.executemany(insert_query, batch)
            inserted += len(batch)
            print(f"INSERT: {inserted}/{total}")

        conn.commit()

        print(f"\n{'='*70}")
        print(f"ГОТОВО: {total} строк загружено в {TABLE_NAME}")
        print(f"Периоды: {', '.join(all_periods)}")
        print(f"{'='*70}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
