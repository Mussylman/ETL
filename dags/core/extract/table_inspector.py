"""
Инспектор структуры MSSQL-таблиц и генератор DDL для PostgreSQL.

Использование:
    from core.extract import TableInspector

    inspector = TableInspector(src_conn_id="mssql_1c_conn", database="UPP_JAN")

    # Получить структуру таблицы
    columns_df = inspector.get_columns("_Document123")

    # Сгенерировать DDL
    ddl = inspector.generate_pg_ddl("_Document123", "sales")

    # Создать таблицу в PostgreSQL
    inspector.create_target_table(ddl, dst_conn_id="postgre_test_base")
"""

import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Маппинг типов MSSQL → PostgreSQL
MSSQL_TO_PG_TYPE = {
    # Binary
    "binary":    lambda size: "UUID" if size == 16 else ("INTEGER" if size == 4 else ("BOOLEAN" if size == 1 else "BYTEA")),
    "varbinary": lambda size: "BYTEA",

    # String
    "nvarchar":  lambda size: "TEXT",
    "varchar":   lambda size: "TEXT",
    "nchar":     lambda size: "TEXT",
    "char":      lambda size: "TEXT",
    "ntext":     lambda size: "TEXT",
    "text":      lambda size: "TEXT",

    # Numeric
    "numeric":   lambda size: None,  # handled separately with precision/scale
    "decimal":   lambda size: None,  # handled separately with precision/scale
    "int":       lambda size: "INTEGER",
    "bigint":    lambda size: "BIGINT",
    "smallint":  lambda size: "SMALLINT",
    "tinyint":   lambda size: "SMALLINT",
    "float":     lambda size: "DOUBLE PRECISION",
    "real":      lambda size: "REAL",
    "money":     lambda size: "NUMERIC(19,4)",
    "smallmoney": lambda size: "NUMERIC(10,4)",

    # Date/time
    "datetime":  lambda size: "TIMESTAMP",
    "datetime2": lambda size: "TIMESTAMP",
    "smalldatetime": lambda size: "TIMESTAMP",
    "date":      lambda size: "DATE",
    "time":      lambda size: "TIME",

    # Boolean
    "bit":       lambda size: "BOOLEAN",

    # Other
    "uniqueidentifier": lambda size: "UUID",
    "image":     lambda size: "BYTEA",
    "xml":       lambda size: "TEXT",
}


class TableInspector:
    """
    Утилита для обнаружения структуры MSSQL-таблиц
    и генерации DDL для PostgreSQL.
    """

    def __init__(self, src_conn_id: str = "mssql_1c_conn", database: str = "UPP_JAN"):
        self.src_conn_id = src_conn_id
        self.database = database

    # ------------------------------------------------------------------
    # Получение структуры таблицы из MSSQL
    # ------------------------------------------------------------------
    def get_columns(self, table_name: str, schema: str = "dbo") -> pd.DataFrame:
        """
        Возвращает DataFrame с информацией о колонках MSSQL-таблицы.

        Колонки результата:
            COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, ORDINAL_POSITION
        """
        hook = MsSqlHook(mssql_conn_id=self.src_conn_id)

        sql = f"""
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                ORDINAL_POSITION
            FROM [{self.database}].INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """

        df = hook.get_pandas_df(sql)
        print(f"TableInspector: {table_name} -> {len(df)} columns")
        return df

    # ------------------------------------------------------------------
    # Маппинг одной колонки MSSQL → PostgreSQL тип
    # ------------------------------------------------------------------
    def _map_column_type(self, row) -> str:
        """Определяет PostgreSQL-тип для одной MSSQL-колонки."""
        data_type = row["DATA_TYPE"].lower()
        char_len = row.get("CHARACTER_MAXIMUM_LENGTH")
        precision = row.get("NUMERIC_PRECISION")
        scale = row.get("NUMERIC_SCALE")

        # numeric/decimal — с точностью
        if data_type in ("numeric", "decimal"):
            if precision is not None and scale is not None:
                return f"NUMERIC({int(precision)},{int(scale)})"
            elif precision is not None:
                return f"NUMERIC({int(precision)})"
            return "NUMERIC"

        # binary — по размеру
        if data_type == "binary" and char_len is not None:
            size = int(char_len)
            mapper = MSSQL_TO_PG_TYPE.get("binary")
            return mapper(size)

        # Остальные типы
        mapper = MSSQL_TO_PG_TYPE.get(data_type)
        if mapper:
            result = mapper(char_len)
            if result is not None:
                return result

        # Fallback
        return "TEXT"

    # ------------------------------------------------------------------
    # Генерация DDL для PostgreSQL
    # ------------------------------------------------------------------
    def generate_pg_ddl(
        self,
        mssql_table: str,
        pg_table: str,
        pg_schema: str = "public",
        mssql_schema: str = "dbo",
    ) -> str:
        """
        Генерирует CREATE TABLE DDL для PostgreSQL
        на основе структуры MSSQL-таблицы.

        Добавляет служебные колонки: etl_loaded_at, updated_at.
        """
        columns_df = self.get_columns(mssql_table, schema=mssql_schema)

        if columns_df.empty:
            raise ValueError(f"Таблица {mssql_table} не найдена или пуста")

        lines = []
        for _, row in columns_df.iterrows():
            col_name = row["COLUMN_NAME"]
            pg_type = self._map_column_type(row)
            nullable = "" if row["IS_NULLABLE"] == "YES" else " NOT NULL"
            lines.append(f'    "{col_name}" {pg_type}{nullable}')

        # Служебные колонки
        lines.append('    "etl_loaded_at" TIMESTAMP')
        lines.append('    "updated_at" TIMESTAMP')

        columns_sql = ",\n".join(lines)

        ddl = f"""CREATE TABLE IF NOT EXISTS {pg_schema}.{pg_table} (\n{columns_sql}\n);"""
        return ddl

    # ------------------------------------------------------------------
    # Создание таблицы в PostgreSQL
    # ------------------------------------------------------------------
    def create_target_table(self, ddl: str, dst_conn_id: str = "postgre_test_base"):
        """Выполняет DDL в целевой PostgreSQL."""
        pg = PostgresHook(postgres_conn_id=dst_conn_id)

        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

        print(f"DDL executed successfully")
