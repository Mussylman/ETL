"""
Генератор SQL-запросов на основе конфигурации.
Поддерживает JOIN (header + details), UNION ALL и accumrg_with_documents.
"""

import re
from typing import List, Optional, Set
from ..config.models import (
    SourceConfig,
    UnionConfig,
    RegisterConfig,
    TargetConfig,
    ColumnMapping,
)
from ..transform.binary import uuid_to_mssql_hex_1c


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

        join_type = self._normalize_join_type(detail.join_type)

        return f"""{parent_table}
    {join_type} JOIN {detail_table}
        ON [{detail.source_code}].[{detail.join_key_source}] = [{parent.source_code}].[{detail.join_key_parent}]"""

    @staticmethod
    def _normalize_join_type(join_type) -> str:
        """
        Защита от исторического бага «INNER JOIN JOIN»: в etl_meta встречается
        join_type='INNER JOIN', а ниже добавляется слово JOIN. Нормализуем:
        'INNER JOIN'/'inner'/None → 'INNER'; всё неизвестное → 'INNER' (fallback).
        """
        v = " ".join(str(join_type or "INNER").upper().split())
        if v.endswith(" JOIN"):
            v = v[: -len(" JOIN")].strip()
        return v if v in ("INNER", "LEFT", "RIGHT") else "INNER"

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

        # Период. Колонка берётся из source.period_column:
        #   '_Period' (default/legacy), '_Date_Time' (документы), None — фильтра нет.
        # getattr — fallback на '_Period' для конфигов без поля (старые вызовы).
        period_col = getattr(main_table, "period_column", "_Period")
        if period_col:
            if period_start and period_end:
                conditions.append(f"[{alias}].[{period_col}] BETWEEN '{period_start}' AND '{period_end}'")
            elif period_start:
                conditions.append(f"[{alias}].[{period_col}] >= '{period_start}'")

        # Список ключей (для incremental).
        # 1С хранит UUID в перевёрнутом порядке байтов, поэтому используем
        # uuid_to_mssql_hex_1c (инверсия binary_to_uuid). Невалидные UUID
        # игнорируем — иначе SQL развалится на "0xNone".
        if key_values and key_column:
            hex_values = [uuid_to_mssql_hex_1c(v) for v in key_values]
            hex_values = [h for h in hex_values if h is not None]
            if hex_values:
                conditions.append(
                    f"[{alias}].[{key_column}] IN ({', '.join(hex_values)})"
                )
            else:
                # Все uid-ы кривые — даём WHERE 1=0, чтобы не получить полный перегон.
                conditions.append("1=0")

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

    # ──────────────────────────────────────────────────────────────────
    # pipeline_type = 'accumrg_with_documents'
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _header_type_code(header: SourceConfig) -> Optional[int]:
        """_Document{N} → N (integer)."""
        m = re.match(r"^_Document(\d+)$", header.mssql_table or "")
        return int(m.group(1)) if m else None

    @staticmethod
    def _vt_meta(vt: SourceConfig) -> Optional[dict]:
        """
        Из mssql_table формата _Document{N}_VT{M} извлекает:
          • doc_n   — тип документа (_RecorderTRef value)
          • vt_n    — номер табчасти
          • fk_col  — '_Document{N}_IDRRef' (FK на header)
          • line_col — '_LineNo{M+1}' (номер строки в табчасти; pattern из 1C)
        """
        m = re.match(r"^_Document(\d+)_VT(\d+)$", vt.mssql_table or "")
        if not m:
            return None
        doc_n = int(m.group(1))
        vt_n = int(m.group(2))
        return {
            "doc_n": doc_n,
            "vt_n": vt_n,
            "fk_col": f"_Document{doc_n}_IDRRef",
            "line_col": f"_LineNo{vt_n + 1}",
        }

    @staticmethod
    def _expr_or_column(mapping: ColumnMapping, alias: str) -> str:
        """[alias].[col] или (expression с заменой {alias})."""
        if mapping.is_expression:
            return f"({mapping.source_column.replace('{alias}', alias)})"
        return f"[{alias}].[{mapping.source_column}]"

    def build_accumrg_with_documents(
        self,
        register: RegisterConfig,
        target: TargetConfig,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        key_column: Optional[str] = None,
        key_values: Optional[List[str]] = None,
    ) -> str:
        """
        SQL для register с pipeline_type='accumrg_with_documents'.

        Базовый источник — AccumRg main (standalone). Документы и VT
        присоединяются LEFT JOIN с фильтром по _RecorderTRef:
          • header_N : ON AccumRg._RecorderRRef = d_N._IDRRef
                        AND AccumRg._RecorderTRef = 0x{N:08X}
          • VT       : ON vt._Document{N}_IDRRef = AccumRg._RecorderRRef
                        AND AccumRg._RecorderTRef = 0x{N:08X}
                        AND vt._LineNo{M+1} = AccumRg._LineNo

        SELECT собирается из target.include_columns по правилам:
          • если есть маппинг в AccumRg main → берём из него (источник истины)
          • для target_role=dimension → COALESCE по header'ам
          • для target_role=fact      → COALESCE по VT
          • иначе → NULL
        """
        main_src = next(
            (s for s in register.sources if s.source_type == "standalone"), None
        )
        if main_src is None:
            raise ValueError(
                f"register '{register.code}' (pipeline_type=accumrg_with_documents): "
                "main standalone source (AccumRg) не найден"
            )

        headers = [s for s in register.sources if s.source_type == "header"]
        vts = [s for s in register.sources if s.source_type == "detail"]
        main_alias = main_src.source_code

        # Удобные индексы по target_column для каждого источника
        def _mapping(src: SourceConfig, target_col: str) -> Optional[ColumnMapping]:
            return next(
                (c for c in src.columns if c.target_column == target_col),
                None,
            )

        # SELECT — по target.include_columns
        include = list(target.include_columns or [])
        select_parts: List[str] = []

        # Технические системные колонки никогда не выбираем из MSSQL
        SYSTEM = {"id", "sales_id", "etl_loaded_at", "etl_hash", "updated_at"}

        for target_col in include:
            if target_col in SYSTEM:
                continue

            # 1) Базовый источник AccumRg — приоритет
            main_map = _mapping(main_src, target_col)
            if main_map:
                expr = self._expr_or_column(main_map, main_alias)
                select_parts.append(f"{expr} AS [{target_col}]")
                continue

            # 2) Для dim — header'ы; для fact — VT
            if (target.target_role or "").lower() == "dimension":
                candidates = headers
                prefix = "d_"
            else:
                candidates = vts
                prefix = "vt_"

            sub_exprs: List[str] = []
            for src in candidates:
                m = _mapping(src, target_col)
                if m:
                    alias = prefix + src.source_code
                    sub_exprs.append(self._expr_or_column(m, alias))

            if not sub_exprs:
                select_parts.append(f"NULL AS [{target_col}]")
            elif len(sub_exprs) == 1:
                select_parts.append(f"{sub_exprs[0]} AS [{target_col}]")
            else:
                select_parts.append(
                    f"COALESCE({', '.join(sub_exprs)}) AS [{target_col}]"
                )

        select_clause = ",\n       ".join(select_parts) if select_parts else "*"

        # FROM + JOINs
        from_main = (
            f"[{self.database}].[{main_src.mssql_schema}].[{main_src.mssql_table}] "
            f"AS [{main_alias}]"
        )

        join_lines: List[str] = []
        for h in headers:
            tc = self._header_type_code(h)
            if tc is None:
                continue
            a = "d_" + h.source_code
            hex_tc = f"0x{tc:08X}"
            join_lines.append(
                f"LEFT JOIN [{self.database}].[{h.mssql_schema}].[{h.mssql_table}] AS [{a}]\n"
                f"    ON [{main_alias}].[_RecorderRRef] = [{a}].[_IDRRef]\n"
                f"   AND [{main_alias}].[_RecorderTRef] = {hex_tc}"
            )

        for vt in vts:
            meta = self._vt_meta(vt)
            if not meta:
                continue
            a = "vt_" + vt.source_code
            hex_tc = f"0x{meta['doc_n']:08X}"
            join_lines.append(
                f"LEFT JOIN [{self.database}].[{vt.mssql_schema}].[{vt.mssql_table}] AS [{a}]\n"
                f"    ON [{a}].[{meta['fk_col']}] = [{main_alias}].[_RecorderRRef]\n"
                f"   AND [{main_alias}].[_RecorderTRef] = {hex_tc}\n"
                f"   AND [{a}].[{meta['line_col']}] = [{main_alias}].[_LineNo]"
            )

        from_clause = from_main + (("\n  " + "\n  ".join(join_lines)) if join_lines else "")

        # WHERE
        conditions: List[str] = []
        period_col = getattr(main_src, "period_column", "_Period") or "_Period"
        if period_start and period_end:
            conditions.append(
                f"[{main_alias}].[{period_col}] BETWEEN '{period_start}' AND '{period_end}'"
            )
        elif period_start:
            conditions.append(f"[{main_alias}].[{period_col}] >= '{period_start}'")

        if key_values and key_column:
            hex_values = [uuid_to_mssql_hex_1c(v) for v in key_values]
            hex_values = [h for h in hex_values if h is not None]
            if hex_values:
                conditions.append(
                    f"[{main_alias}].[{key_column}] IN ({', '.join(hex_values)})"
                )
            else:
                conditions.append("1=0")

        if main_src.where_clause:
            where = main_src.where_clause.replace("{alias}", main_alias)
            conditions.append(f"({where})")

        sql = f"SELECT {select_clause}\nFROM {from_clause}"
        if conditions:
            sql += "\nWHERE " + " AND ".join(conditions)
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
