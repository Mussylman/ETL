"""
Адаптеры источников. Каждый говорит на своём диалекте — выражения ClickHouse
на источник не попадают никогда.

Ключ партиции везде целое число и совпадает с тем, что даёт ClickHouse:
    month → toYYYYMM      = год*100   + месяц
    day   → toYYYYMMDD    = год*10000 + месяц*100 + день
"""

from typing import Iterator, List, Optional, Tuple


class Source:
    """Общая часть: сборка SELECT из конфигурации и потоковое чтение."""

    def __init__(self, spec, conn):
        self.spec = spec
        self.conn = conn

    # --- диалект ----------------------------------------------------------
    def partition_key_expr(self, col: str) -> str:
        raise NotImplementedError

    def date_trunc_bounds(self, key: str) -> Tuple[str, str]:
        raise NotImplementedError

    def quote(self, schema: Optional[str], obj: str) -> str:
        raise NotImplementedError

    # --- сборка запроса ---------------------------------------------------
    def base_query(self) -> str:
        """Тело источника: либо явный SELECT из конфига, либо таблица/VIEW целиком."""
        spec = self.spec
        if spec.source_query:
            return f"({spec.source_query}) src"
        return self.quote(spec.source_schema, spec.source_object)

    def select_sql(self, where: str = "") -> str:
        """
        SELECT колонок в порядке ordinal. Выражения берутся из конфигурации.

        Обогащаемые колонки сюда не попадают: в источнике их нет, их значение
        подставляется соединением со справочником уже внутри ClickHouse.
        """
        cols = ",\n       ".join(
            f"{c.source_expr} AS {c.target_column}" for c in self.spec.stream_columns)
        return f"SELECT {cols}\n  FROM {self.base_query()}{where}"

    def partition_filter(self, key: str) -> str:
        """Ограничение одной партицией — на диалекте источника."""
        col = self.spec.partition_column
        return f" WHERE {self.partition_key_expr(col)} = {int(key)}"

    def projected(self, where: str = "") -> str:
        """
        Источник, уже приведённый к целевым колонкам через source_expr.

        Отпечаток обязан считаться именно по нему, а не по сырым колонкам: в
        ClickHouse лежат значения ПОСЛЕ coalesce(...,0), а в PostgreSQL concat_ws
        пропускает NULL целиком — канонические строки разошлись бы на каждой
        строке с пустой ссылкой, и sweep объявлял бы расхождением всю историю.
        """
        return f"({self.select_sql(where)}) proj"

    # --- чтение -----------------------------------------------------------
    def stream(self, sql: str, batch: int) -> Iterator[tuple]:
        raise NotImplementedError


class PostgresSource(Source):
    def partition_key_expr(self, col: str) -> str:
        g = self.spec.partition_granularity
        if g == "month":
            return f"(extract(year from {col})::int * 100 + extract(month from {col})::int)"
        if g == "day":
            return (f"(extract(year from {col})::int * 10000 + extract(month from {col})::int * 100"
                    f" + extract(day from {col})::int)")
        raise RuntimeError(f"неизвестная гранулярность партиции: {g!r}")

    def quote(self, schema, obj) -> str:
        return f'"{schema}"."{obj}"' if schema else f'"{obj}"'

    def stream(self, sql: str, batch: int) -> Iterator[tuple]:
        import psycopg2.extras  # noqa: F401  (нужен для именованного курсора)
        conn = self.conn.get_conn()
        try:
            # серверный курсор: партиция не тянется в память целиком
            cur = conn.cursor(name=f"ch_sync_{self.spec.id}_{id(sql) & 0xffff}")
            cur.itersize = batch
            cur.execute(sql)
            while True:
                chunk = cur.fetchmany(batch)
                if not chunk:
                    break
                for row in chunk:
                    yield row
            cur.close()
        finally:
            conn.close()

    def scalar(self, sql: str):
        return self.conn.get_first(sql)


class MssqlSource(Source):
    def partition_key_expr(self, col: str) -> str:
        g = self.spec.partition_granularity
        if g == "month":
            return f"(YEAR({col}) * 100 + MONTH({col}))"
        if g == "day":
            return f"(YEAR({col}) * 10000 + MONTH({col}) * 100 + DAY({col}))"
        raise RuntimeError(f"неизвестная гранулярность партиции: {g!r}")

    def quote(self, schema, obj) -> str:
        return f"[{schema}].[{obj}]" if schema else f"[{obj}]"

    def base_query(self) -> str:
        # NOLOCK: читать боевую 1С/PowerBI без блокировок — правило проекта
        q = super().base_query()
        return q if self.spec.source_query else f"{q} WITH (NOLOCK)"

    def stream(self, sql: str, batch: int) -> Iterator[tuple]:
        conn = self.conn.get_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            while True:
                chunk = cur.fetchmany(batch)
                if not chunk:
                    break
                for row in chunk:
                    yield row
            cur.close()
        finally:
            conn.close()

    def scalar(self, sql: str):
        return self.conn.get_first(sql)


def open_source(spec) -> Source:
    """Источник по конфигурации. Креды берутся из Airflow connection, не из конфига."""
    if spec.source_type == "postgres":
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        return PostgresSource(spec, PostgresHook(postgres_conn_id=spec.source_conn_id))
    if spec.source_type == "mssql":
        from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
        return MssqlSource(spec, MsSqlHook(mssql_conn_id=spec.source_conn_id))
    raise RuntimeError(f"неизвестный source_type: {spec.source_type!r}")
