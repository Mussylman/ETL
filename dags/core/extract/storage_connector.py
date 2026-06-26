from typing import List, Tuple, Optional, Set, Union
import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


# Legacy: для обратной совместимости
DOCUMENT_SCHEMAS = {
    "sales": {
        "main": "_AccumRg17844",
        "details": [
            "_AccumRg17844_VT",
            "_AccumRg17844_DT"
        ],
        "uid_column": "_RecorderRRef"
    }
}


class StorageConnector:
    """
    Универсальный загрузчик данных из MSSQL (1С).

    Основные методы:
     - execute_query()  : выполнение произвольного SQL (для ETLEngine)
     - load_table()     : загрузка таблицы по периоду/ключу (legacy)
     - load_document()  : загрузка документа с табличными частями (legacy)
    """

    def __init__(self, src_conn_id: str = "mssql_1c_conn", database: str = "UPP_JAN"):
        self.src_conn_id = src_conn_id
        self.database = database

    # ======================================================================
    #  НОВЫЙ МЕТОД: выполнение произвольного SQL
    # ======================================================================
    def execute_query(
        self,
        sql: str,
        detect_binary: bool = True,
    ) -> Tuple[pd.DataFrame, List[str]]:
        """
        Выполняет произвольный SQL-запрос и возвращает DataFrame.

        Args:
            sql: SQL-запрос для выполнения
            detect_binary: автоматически определять binary-колонки

        Returns:
            (DataFrame, list[binary_columns])
        """
        hook = MsSqlHook(mssql_conn_id=self.src_conn_id)

        df = hook.get_pandas_df(sql)

        binary_columns = []
        if detect_binary and not df.empty:
            binary_columns = self._detect_binary_columns(df)

        print(f"MSSQL execute: {len(df)} rows")
        return df, binary_columns

    def _detect_binary_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Определяет binary-колонки по содержимому DataFrame.
        """
        binary_cols = []
        for col in df.columns:
            sample = df[col].dropna().head(1)
            if not sample.empty:
                val = sample.iloc[0]
                if isinstance(val, (bytes, bytearray, memoryview)):
                    binary_cols.append(col)
        return binary_cols

    def get_binary_columns_from_schema(
        self,
        table_name: str,
        schema: str = "dbo",
    ) -> List[str]:
        """
        Получает список binary-колонок из INFORMATION_SCHEMA.
        """
        hook = MsSqlHook(mssql_conn_id=self.src_conn_id)

        sql = f"""
            SELECT COLUMN_NAME
            FROM [{self.database}].INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{table_name}'
              AND DATA_TYPE IN ('binary', 'varbinary')
        """

        df = hook.get_pandas_df(sql)
        return df["COLUMN_NAME"].tolist() if not df.empty else []

    # ======================================================================
    #  LEGACY: для обратной совместимости с etl_core.py
    # ======================================================================

    def read_from_mssql(
        self,
        table_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        key_column: Optional[str] = None,
        key_values: Optional[List[str]] = None,
    ) -> Tuple[pd.DataFrame, List[str]]:
        """
        Универсальный метод чтения из MSSQL (для etl_core.py).

        Поддерживает два режима:
          1. По периоду: start_date + end_date
          2. По списку ключей: key_column + key_values

        Returns:
            (DataFrame, list[binary_columns])
        """
        hook = MsSqlHook(mssql_conn_id=self.src_conn_id)

        # Получаем binary-колонки
        binary_columns = self.get_binary_columns_from_schema(table_name)

        # Формируем WHERE
        where_parts = []

        if start_date and end_date:
            where_parts.append(f"_Period BETWEEN '{start_date}' AND '{end_date}'")
        elif start_date:
            where_parts.append(f"_Period >= '{start_date}'")

        if key_column and key_values:
            from ..transform.binary import uuid_to_mssql_hex_1c
            hex_values = [uuid_to_mssql_hex_1c(v) for v in key_values]
            hex_values = [h for h in hex_values if h is not None]
            if hex_values:
                where_parts.append(f"{key_column} IN ({', '.join(hex_values)})")
            else:
                where_parts.append("1=0")

        where_clause = " AND ".join(where_parts) if where_parts else ""

        sql = f"""
            SELECT *
            FROM [{self.database}].[dbo].[{table_name}]
            {"WHERE " + where_clause if where_clause else ""}
        """

        df = hook.get_pandas_df(sql)

        print(f"MSSQL read: {table_name} -> {len(df)} rows")
        return df, binary_columns

    # ----------------------------------------------------------------------
    #  Универсальный метод загрузки одной таблицы (header или details)
    # ----------------------------------------------------------------------
    def load_table(self, table_name, key_column=None, key_value=None,
                   start_date=None, end_date=None):

        hook = MsSqlHook(mssql_conn_id=self.src_conn_id)

        # типы колонок → binary columns
        type_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
        """

        type_df = hook.get_pandas_df(type_query)
        binary_columns = type_df[type_df["DATA_TYPE"].isin(["binary", "varbinary"])]["COLUMN_NAME"].tolist()

        # WHERE
        where = ""

        if key_column and key_value:
            from ..transform.binary import uuid_to_mssql_hex_1c
            hex_lit = uuid_to_mssql_hex_1c(key_value)
            if hex_lit is None:
                where = "WHERE 1=0"
            else:
                where = f"WHERE {key_column} = {hex_lit}"  # binary сравнение (1С-формат)
        elif start_date and end_date:
            where = f"WHERE _Period BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            where = f"WHERE _Period >= '{start_date}'"

        sql = f"""
            SELECT *
            FROM [UPP_JAN].[dbo].[{table_name}]
            {where}
        """

        df = hook.get_pandas_df(sql)

        print(f"⏱ MSSQL load: {table_name} → {len(df)} строк")
        return df, binary_columns

    # ----------------------------------------------------------------------
    #  Загрузка полного документа по UID (header + detail tables)
    # ----------------------------------------------------------------------
    def load_document(self, document_type: str, uid: str):
        """
        Возвращает:
        {
            "header": df_header,
            "details": {table_name: df}
        }
        """

        if document_type not in DOCUMENT_SCHEMAS:
            raise ValueError(f"Неизвестный document_type: {document_type}")

        schema = DOCUMENT_SCHEMAS[document_type]

        main_table = schema["main"]
        detail_tables = schema["details"]
        uid_column = schema["uid_column"]

        # Шапка
        header_df, header_binary = self.load_table(
            table_name=main_table,
            key_column=uid_column,
            key_value=uid
        )

        # Детали
        details = {}
        for tbl in detail_tables:
            df, bin_cols = self.load_table(
                table_name=tbl,
                key_column=uid_column,
                key_value=uid
            )
            details[tbl] = (df, bin_cols)

        return {
            "header": (header_df, header_binary),
            "details": details
        }
