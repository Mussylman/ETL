"""
Генератор SQL-запросов на основе конфигурации.
Поддерживает JOIN (header + details) и UNION ALL.
"""

from typing import List, Optional, Set
from ..config.models import (
    SourceConfig,
    UnionConfig,
    RegisterConfig,
    ColumnMapping,
)


class QueryBuilder:
    """
    Генерирует SQL-запросы на основе конфигурации регистра.

    Поддерживает:
      - Простые SELECT из одной таблицы
      - JOIN (header + detail таблицы)
      - UNION ALL (несколько документов в один регистр)
    """

    def __init__(self, database: str = "UPP_JAN"):
        self.database = database

    def build_source_query(
        self,
        source: SourceConfig,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        key_column: Optional[str] = None,
        key_values: Optional[List[str]] = None,
        extra_where: Optional[str] = None,
    ) -> str:
        """
        Строит SELECT для одного источника.
        Если source.parent_source задан — строит JOIN.
        """
        # Определяем все таблицы для FROM/JOIN
        tables = self._collect_tables(source)

        # SELECT columns
        select_clause = self._build_select_clause(source, tables)

        # FROM + JOIN
        from_clause = self._build_from_clause(source, tables)

        # WHERE
        where_clause = self._build_where_clause(
            source=source,
            tables=tables,
            period_start=period_start,
            period_end=period_end,
            key_column=key_column,
            key_values=key_values,
            extra_where=extra_where,
        )

        sql = f"SELECT {select_clause}\nFROM {from_clause}"
        if where_clause:
            sql += f"\nWHERE {where_clause}"

        return sql

    def build_union_query(
        self,
        union: UnionConfig,
        register: RegisterConfig,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        key_column: Optional[str] = None,
        key_values: Optional[List[str]] = None,
    ) -> str:
        """
        Строит UNION ALL из нескольких источников.
        """
        queries = []

        for member in union.members:
            if not member.source:
                continue

            # Строим запрос для каждого члена UNION
            subquery = self._build_union_member_query(
                source=member.source,
                output_columns=union.output_columns,
                period_start=period_start,
                period_end=period_end,
                key_column=key_column,
                key_values=key_values,
                extra_where=member.where_clause,
            )

            queries.append(f"({subquery})")

        return "\nUNION ALL\n".join(queries)

    def _collect_tables(self, source: SourceConfig) -> List[SourceConfig]:
        """
        Собирает список всех таблиц для запроса (source + parent).
        Возвращает в порядке: parent (если есть), source.
        """
        tables = []

        if source.parent_source:
            tables.append(source.parent_source)

        tables.append(source)

        return tables

    def _build_select_clause(
        self,
        source: SourceConfig,
        tables: List[SourceConfig],
    ) -> str:
        """
        Строит SELECT-часть на основе маппингов колонок.
        """
        columns = []

        # Собираем колонки из всех таблиц
        for table in tables:
            alias = table.source_code

            for col in table.columns:
                if col.is_expression:
                    # SQL-выражение (например: -1 * _Fld17876)
                    expr = col.source_column.replace("{alias}", alias)
                    columns.append(f"({expr}) AS [{col.target_column}]")
                else:
                    columns.append(f"[{alias}].[{col.source_column}] AS [{col.target_column}]")

        if not columns:
            return "*"

        return ",\n       ".join(columns)

    def _build_from_clause(
        self,
        source: SourceConfig,
        tables: List[SourceConfig],
    ) -> str:
        """
        Строит FROM + JOIN.
        """
        if len(tables) == 1:
            # Одна таблица
            t = tables[0]
            return f"[{self.database}].[{t.mssql_schema}].[{t.mssql_table}] AS [{t.source_code}]"

        # Есть JOIN (parent + detail)
        parent = tables[0]
        detail = tables[1]

        parent_table = f"[{self.database}].[{parent.mssql_schema}].[{parent.mssql_table}] AS [{parent.source_code}]"
        detail_table = f"[{self.database}].[{detail.mssql_schema}].[{detail.mssql_table}] AS [{detail.source_code}]"

        join_type = detail.join_type or "INNER"

        return f"""{parent_table}
    {join_type} JOIN {detail_table}
        ON [{detail.source_code}].[{detail.join_key_source}] = [{parent.source_code}].[{detail.join_key_parent}]"""

    def _build_where_clause(
        self,
        source: SourceConfig,
        tables: List[SourceConfig],
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        key_column: Optional[str] = None,
        key_values: Optional[List[str]] = None,
        extra_where: Optional[str] = None,
    ) -> str:
        """
        Строит WHERE-часть.
        """
        conditions = []

        # Основная таблица для фильтрации (header или standalone)
        main_table = tables[0]
        alias = main_table.source_code

        # Период
        if period_start and period_end:
            conditions.append(f"[{alias}].[_Period] BETWEEN '{period_start}' AND '{period_end}'")
        elif period_start:
            conditions.append(f"[{alias}].[_Period] >= '{period_start}'")

        # Список ключей (для incremental)
        if key_values and key_column:
            hex_values = [f"0x{v.replace('-', '')}" for v in key_values]
            conditions.append(f"[{alias}].[{key_column}] IN ({', '.join(hex_values)})")

        # Дополнительный фильтр из конфигурации источника
        for table in tables:
            if table.where_clause:
                # Заменяем {alias} на реальный alias таблицы
                where = table.where_clause.replace("{alias}", table.source_code)
                conditions.append(f"({where})")

        # Дополнительный WHERE (из union member)
        if extra_where:
            conditions.append(f"({extra_where})")

        return " AND ".join(conditions)

    def _build_union_member_query(
        self,
        source: SourceConfig,
        output_columns: List[str],
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        key_column: Optional[str] = None,
        key_values: Optional[List[str]] = None,
        extra_where: Optional[str] = None,
    ) -> str:
        """
        Строит запрос для одного члена UNION.
        Выбирает только колонки, указанные в output_columns.
        """
        # Собираем таблицы
        tables = self._collect_tables(source)

        # Создаём маппинг target_column -> выражение
        column_map = {}
        for table in tables:
            alias = table.source_code
            for col in table.columns:
                if col.is_expression:
                    expr = col.source_column.replace("{alias}", alias)
                    column_map[col.target_column] = f"({expr})"
                else:
                    column_map[col.target_column] = f"[{alias}].[{col.source_column}]"

        # Строим SELECT только для output_columns
        select_parts = []
        for target_col in output_columns:
            if target_col in column_map:
                select_parts.append(f"{column_map[target_col]} AS [{target_col}]")
            else:
                # Колонка не найдена — NULL
                select_parts.append(f"NULL AS [{target_col}]")

        select_clause = ",\n       ".join(select_parts)

        # FROM + JOIN
        from_clause = self._build_from_clause(source, tables)

        # WHERE
        where_clause = self._build_where_clause(
            source=source,
            tables=tables,
            period_start=period_start,
            period_end=period_end,
            key_column=key_column,
            key_values=key_values,
            extra_where=extra_where,
        )

        sql = f"SELECT {select_clause}\n    FROM {from_clause}"
        if where_clause:
            sql += f"\n    WHERE {where_clause}"

        return sql

    def get_binary_columns_query(self, table_name: str, schema: str = "dbo") -> str:
        """
        Запрос для получения списка binary-колонок таблицы.
        """
        return f"""
            SELECT COLUMN_NAME
            FROM [{self.database}].INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{table_name}'
              AND DATA_TYPE IN ('binary', 'varbinary')
        """
