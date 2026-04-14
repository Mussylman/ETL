from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataChecker:
    """
    Определяет список document_uid, которые нужно обновить в ETL.

    Логика:
      1. Смотрим в ETL таблицу — какой последний updated_at уже загружен.
      2. В retail ищем все документы, у которых updated_at > last_update.
      3. Возвращаем список UID, которые нужно заново загрузить из MSSQL (1С).
    """

    def __init__(
        self,
        retail_table="sales",
        retail_conn_id="bd_retail",
        etl_table="sales_register",
        etl_conn_id="postgre_test_base",
        key_column="document_uid",
    ):
        self.retail_table = retail_table
        self.retail_conn_id = retail_conn_id
        self.etl_table = etl_table
        self.etl_conn_id = etl_conn_id
        self.key_column = key_column

    # -------------------------------------------------------------------
    # 1) Берём последний updated_at из ETL
    # -------------------------------------------------------------------
    def get_last_update(self):
        pg = PostgresHook(postgres_conn_id=self.etl_conn_id)

        sql = f"""
            SELECT COALESCE(MAX(updated_at), '2000-01-01 00:00:00') AS last_update
            FROM {self.etl_table};
        """

        df = pg.get_pandas_df(sql)

        if df.empty or df.iloc[0]["last_update"] is None:
            return "2000-01-01 00:00:00"

        return df.iloc[0]["last_update"]

    # -------------------------------------------------------------------
    # 2) Находим новые / изменённые документы
    # -------------------------------------------------------------------
    def get_changed_uids(self):
        last_update = self.get_last_update()

        sql = f"""
            SELECT {self.key_column} AS uid, updated_at
            FROM {self.retail_table}
            WHERE updated_at > '{last_update}'
            ORDER BY updated_at ASC;
        """

        pg = PostgresHook(postgres_conn_id=self.retail_conn_id)
        df = pg.get_pandas_df(sql)

        print(f"🔵 Incremental: найдено {len(df)} документов после {last_update}")

        return df  # важно вернуть DataFrame (uid + updated_at)

    # -------------------------------------------------------------------
    # Точка входа
    # -------------------------------------------------------------------
    def detect_changes(self):
        """
        Возвращает DataFrame:
            uid | updated_at

        Не просто список UIDs — потому что updated_at нужно присвоить в ETL.
        """
        return self.get_changed_uids()
