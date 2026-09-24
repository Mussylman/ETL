"""
Цель — ClickHouse. Только аналитические данные: управляющих таблиц здесь нет.

Креды берутся из Airflow connection и передаются клиенту переменными окружения,
чтобы пароль не появлялся в аргументах процесса и не был виден в ps.
"""

import os
import subprocess
from datetime import datetime
from decimal import Decimal
from typing import Iterator, List, Optional

DEFAULT_CONN = "clickhouse_etl"


class ClickHouse:
    def __init__(self, conn_id: str = DEFAULT_CONN):
        from airflow.hooks.base import BaseHook
        c = BaseHook.get_connection(conn_id)
        self.database = c.schema
        # max_query_size: список изменившихся документов в патче может быть длинным
        self.base = ["clickhouse-client", "--host", c.host, "--max_query_size", "104857600"]
        self.env = dict(os.environ)
        self.env["CLICKHOUSE_USER"] = c.login or "default"
        # .password, а не get_password(): внутри таски Airflow 3 отдаёт Connection
        # из airflow.sdk, у которого метода get_password() нет. Вне таски приходит
        # ORM-класс, и там .password тоже есть — одно работает в обоих контекстах.
        self.env["CLICKHOUSE_PASSWORD"] = (c.password or "")

    # Запрос уходит через stdin, а не аргументом --query: список документов для добора
    # хвоста за 15 суток — десятки тысяч uuid, и аргумент командной строки упирался
    # в ограничение ОС на длину аргументов (Argument list too long).
    def query(self, sql: str, fmt: str = "TSV") -> str:
        r = subprocess.run(self.base, input=f"{sql} FORMAT {fmt}",
                           capture_output=True, text=True, env=self.env)
        if r.returncode:
            raise RuntimeError(f"ClickHouse: {r.stderr.strip()[:400]}")
        return r.stdout.strip()

    def row(self, sql: str) -> List[str]:
        out = self.query(sql)
        return out.split("\t") if out else []

    def scalar(self, sql: str, default=0):
        out = self.query(sql)
        return out if out else default

    def execute(self, sql: str) -> None:
        r = subprocess.run(self.base, input=sql, capture_output=True, text=True, env=self.env)
        if r.returncode:
            raise RuntimeError(f"ClickHouse: {r.stderr.strip()[:400]}")

    def insert_tsv(self, table: str, cols: List[str], lines: Iterator[str]) -> None:
        sql = f"INSERT INTO {table} ({', '.join(cols)}) FORMAT TSV"
        p = subprocess.Popen(self.base + ["--query", sql], stdin=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, env=self.env)
        try:
            for line in lines:
                p.stdin.write(line)
        finally:
            p.stdin.close()
        if p.wait():
            raise RuntimeError(f"ClickHouse INSERT: {p.stderr.read().strip()[:400]}")

    # --- операции над партициями -----------------------------------------
    def table_exists(self, fqn: str) -> bool:
        db, tbl = fqn.split(".", 1)
        return self.query(
            f"SELECT count() FROM system.tables WHERE database='{db}' AND name='{tbl}'") == "1"

    def partition_rows(self, fqn: str, part_expr: str, key: Optional[str]) -> int:
        where = "" if key is None else f" WHERE {part_expr} = {int(key)}"
        return int(self.scalar(f"SELECT count() FROM {fqn}{where}") or 0)


def fmt_value(v) -> str:
    """Значение → поле TSV."""
    if v is None:
        return "0"
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, float):
        # Из MSSQL numeric приходит float64. str(0.00001) даёт '1e-05', которую
        # ClickHouse в Decimal не примет. Decimal(repr(v)) — та же кратчайшая
        # запись, что уходит в PostgreSQL у старого пути, но без экспоненты.
        if v != v:                       # NaN
            return "0"
        return format(Decimal(repr(float(v))), "f")   # float(): у numpy 2 repr = "np.float64(..)"
    if isinstance(v, bool):
        return "1" if v else "0"
    s = str(v)
    # В целевых таблицах строковые колонки допускаются (LowCardinality/String),
    # поэтому спецсимволы TSV экранируются, а не игнорируются.
    if "\\" in s or "\t" in s or "\n" in s or "\r" in s:
        s = (s.replace("\\", "\\\\").replace("\t", "\\t")
              .replace("\n", "\\n").replace("\r", "\\r"))
    return s
