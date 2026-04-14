import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

BASE_URL = "https://startrack.mi.gfk.com"


class GFK:
    def __init__(self, base_url: str = BASE_URL, api_key: str = None):
        self.base_url = base_url
        self.api_key = api_key or Variable.get("gfk_token")

    def _headers(self):
        return {"x-api-key": self.api_key, "Accept": "application/json"}

    def get_reportId(self, period: str = None, published_since_days_offset: int = 30):
        """
        Returns: (sales_report_id, product_report_id)
        sales: Flat File (CSV) (.csv) for weekly periods (W..)
        product: Flat File (SSV) (.csv)
        """
        url = (
            f"{self.base_url}/api/v1/Reports"
            f"?publishedSinceDaysOffset={published_since_days_offset}&page=1&pageSize=100"
        )
        response = requests.get(url, headers=self._headers())
        response.raise_for_status()

        reports = response.json()
        if not reports:
            return None, None

        id_week_sales = [
            r
            for r in reports
            if r.get("toolName") == "Flat File (CSV) (.csv)"
            and (
                r.get("periodName") == period
                if period
                else r.get("periodName", "").startswith("W")
            )
        ]

        id_product_tables = [
            r for r in reports if r.get("toolName") == "Flat File (SSV) (.csv)"
        ]

        # NOTE: API order may already be newest first, but we keep first match
        sales_id = id_week_sales[0]["reportId"] if id_week_sales else None
        product_id = id_product_tables[0]["reportId"] if id_product_tables else None

        return sales_id, product_id

    def read_csv_file(self, report_id: str, table_type: str) -> pd.DataFrame:
        url = f"{self.base_url}/api/v1/Reports/{report_id}/file"
        response = requests.get(url, headers=self._headers(), stream=True)
        response.raise_for_status()

        csv_data = StringIO(response.text)

        if table_type == "sales":
            df = pd.read_csv(csv_data)

            rename_dict = {
                "CONSUMER_BUSINE": "ConsumerBusiness",
                "Period": "Period",
                "POSType": "Postype",
                "REGION": "Region",   # может быть в файле, но в БД может не быть
                "CITY": "City",       # может быть в файле, но в БД может не быть
                "Retailer": "Retailer",
                "Channel": "Channel",
                "ID": "Id",
                "SALES UNITS": "SalesUnits",
                "SALES VALUE KZT": "SalesValue",
            }
            df = df.rename(columns=rename_dict)

            # числа
            for c in ["SalesUnits", "SalesValue"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        elif table_type == "product":
            df = pd.read_csv(csv_data, sep=";")

            # "Unnamed" сегменты -> Segment11..Segment16
            expected_columns = ["Segment11", "Segment12", "Segment13", "Segment14", "Segment15", "Segment16"]
            new_cols = []
            for col in df.columns:
                if "Unnamed" in str(col) and expected_columns:
                    new_cols.append(expected_columns.pop(0))
                else:
                    new_cols.append(col)
            df.columns = new_cols

            # ZZZ нормализация
            if "ZZZ" in df.columns:
                df["ZZZ"] = (
                    df["ZZZ"]
                    .fillna(0)
                    .astype(str)
                    .str.strip()
                    .replace({"": "0"})
                    .str.replace(",", ".", regex=True)
                )
                df["ZZZ"] = pd.to_numeric(df["ZZZ"], errors="coerce").fillna(0)

        else:
            raise ValueError(f"Unknown table_type: {table_type}")

        df["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return df.fillna("")

    def insert_to_db(
        self,
        df: pd.DataFrame,
        table_name: str,
        table_type: str,
        mssql_conn_id: str = "mssql_gfk",
        batch_size: int = 5000,
    ):
        if df.empty:
            print("⚠️ DATAFRAME ПУСТ. ЗАГРУЗКА ОТМЕНЕНА.")
            return

        hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # -------------------------------
            # PERIOD NORMALIZATION (sales)
            # -------------------------------
            period_info = ""
            df_period = None
            if "Period" in df.columns:
                df["Period"] = (
                    df["Period"]
                    .astype(str)
                    .str.replace("\u00A0", " ", regex=False)
                    .str.strip()
                )
                uniq = df["Period"].unique()
                period_info = ", ".join(uniq[:10])  # на всякий, чтобы не раздуть лог
                df_period = df["Period"].iloc[0]

            # -------------------------------
            # SALES: SKIP IF PERIOD EXISTS
            # -------------------------------
            if table_type == "sales":
                if df_period is None:
                    raise ValueError("Column 'Period' not found in sales dataframe")

                cursor.execute(
                    f"SELECT 1 FROM {table_name} WHERE LTRIM(RTRIM(Period)) = %s",
                    (df_period,),
                )
                if cursor.fetchone():
                    print(f"⚠️ PERIOD {df_period} ALREADY EXISTS IN {table_name.upper()}. SKIP LOAD.")
                    return

            # -------------------------------
            # PRODUCT: TRUNCATE + DEDUP
            # -------------------------------
            elif table_type == "product":
                print(f"⚠️ TRUNCATE TABLE {table_name.upper()} BEFORE LOAD")
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                conn.commit()

                # dedup внутри файла по ID (ключ таблицы по твоему SELECT)
                if "ID" in df.columns:
                    before = len(df)
                    df = df.drop_duplicates(subset=["ID"], keep="last")
                    after = len(df)
                    if after != before:
                        print(f"🧹 DEDUP PRODUCTS IN FILE: {before} -> {after} (BY ID)")

            else:
                raise ValueError(f"Unknown table_type: {table_type}")

            # -------------------------------
            # KEEP ONLY SQL COLUMNS (future-proof)
            # -------------------------------
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = %s
                """,
                (table_name,),
            )
            table_cols = {r[0] for r in cursor.fetchall()}

            if not table_cols:
                # если вдруг схема другая или база не та — лучше явно сказать
                print(f"⚠️ NO COLUMNS FOUND IN INFORMATION_SCHEMA FOR TABLE {table_name}. CHECK SCHEMA/DB.")
                # все равно попробуем вставить как есть (или можно raise)
                # raise ValueError("Cannot read table columns from INFORMATION_SCHEMA")

            df_cols = [c for c in df.columns if c in table_cols] if table_cols else list(df.columns)
            missing = [c for c in df.columns if c not in table_cols] if table_cols else []

            if missing:
                print(f"⚠️ COLUMNS NOT IN SQL (SKIPPED): {', '.join(missing)}")

            df = df[df_cols]

            # -------------------------------
            # LOG HEADER
            # -------------------------------
            print("=" * 90)
            print("START LOADING DATA")
            print(f"TABLE      : {table_name.upper()}")
            print(f"TYPE       : {table_type.upper()}")
            if period_info:
                print(f"PERIOD     : {period_info}")
            print(f"TOTAL ROWS : {len(df)}")
            print(f"COLUMNS    : {len(df.columns)}")
            print("=" * 90)

            # -------------------------------
            # INSERT BATCHES
            # -------------------------------
            data_tuples = list(df.itertuples(index=False, name=None))

            columns = ", ".join(df.columns)
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            total_rows = len(data_tuples)
            inserted = 0

            for i in range(0, total_rows, batch_size):
                batch = data_tuples[i : i + batch_size]
                cursor.executemany(insert_query, batch)
                inserted += len(batch)

                print(
                    f"[{table_name.upper()} | {table_type.upper()} | {period_info or 'NO_PERIOD'}] "
                    f"INSERTED {inserted}/{total_rows}"
                )

            conn.commit()

            # -------------------------------
            # LOG FOOTER
            # -------------------------------
            print("=" * 90)
            print("FINISHED LOADING")
            print(f"TABLE  : {table_name.upper()}")
            if period_info:
                print(f"PERIOD : {period_info}")
            print(f"ROWS   : {total_rows}")
            print("=" * 90)

        finally:
            cursor.close()
            conn.close()


# ===== LOCAL TEST =====
if __name__ == "__main__":
    gfk = GFK()
    sales, product = gfk.get_reportId(period=None)
    print("Sales report:", sales, "Product report:", product)

    if sales:
        df_sales = gfk.read_csv_file(sales, "sales")
        print(df_sales.head())

    if product:
        df_product = gfk.read_csv_file(product, "product")
        print(df_product.head())
