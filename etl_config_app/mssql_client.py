"""
Lightweight MSSQL client for the ETL Config App.
Used to discover recorder types from accumulation registers.
"""
import pymssql

import os


def _required(name: str) -> str:
    """Обязательная переменная окружения; нет — отказ до подключения. Значение в лог не пишется."""
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"{name} не задан — учётные данные 1С приходят только из окружения "
                           f"(PROD: ~/.config/etl_config/prod.env, см. etl_config_app/RUNNING.md)")
    return v


def get_conn():
    return pymssql.connect(
        server=_required("ETL_CONFIG_MSSQL_SERVER"),
        port=int(os.getenv("ETL_CONFIG_MSSQL_PORT", "1433")),
        database=_required("ETL_CONFIG_MSSQL_DATABASE"),
        user=_required("ETL_CONFIG_MSSQL_USER"),
        password=_required("ETL_CONFIG_MSSQL_PASSWORD"),
    )


def get_column_types(table: str) -> dict:
    """
    Get MSSQL column data types for a table via INFORMATION_SCHEMA.
    Returns: {"_Fld13608RRef": {"data_type": "binary", "max_length": 16, "precision": None, "scale": None}, ...}
    """
    mssql_table = table if table.startswith("_") else f"_{table}"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "
                "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
                (mssql_table,),
            )
            result = {}
            for row in cur.fetchall():
                result[row[0]] = {
                    "data_type": row[1],
                    "max_length": row[2],
                    "precision": row[3],
                    "scale": row[4],
                }
            return result
    finally:
        conn.close()


def query_distinct_recorder_types(table: str) -> list:
    """
    Query MSSQL for distinct _RecorderTRef values from an accumulation register.
    Returns list of ints, e.g. [254, 316, 352, 415, 443, 476]
    """
    # 1C API returns table names without _ prefix, MSSQL has it
    mssql_table = table if table.startswith("_") else f"_{table}"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT [_RecorderTRef] FROM [dbo].[{mssql_table}]")
            rows = cur.fetchall()
            results = []
            for row in rows:
                val = row[0]
                if isinstance(val, bytes):
                    results.append(int.from_bytes(val, byteorder="big"))
                elif isinstance(val, int):
                    results.append(val)
            return sorted(results)
    finally:
        conn.close()
