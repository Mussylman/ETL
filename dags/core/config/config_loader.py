"""
Загрузчик конфигурации ETL из PostgreSQL.
"""

from typing import List, Optional
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from .models import (
    ColumnMapping,
    SourceConfig,
    UnionConfig,
    UnionMember,
    TargetConfig,
    RegisterConfig,
)


class ConfigLoader:
    """
    Загружает полную конфигурацию регистра из PostgreSQL (схема etl_meta).
    """

    def __init__(self, config_conn_id: str = None):
        from ..conn import require_conn
        require_conn("config_conn_id", config_conn_id)
        self.config_conn_id = config_conn_id

    def load_register(self, register_code: str) -> RegisterConfig:
        """Загружает полную конфигурацию регистра по его коду."""
        hook = PostgresHook(postgres_conn_id=self.config_conn_id)

        # 1. Основная информация о регистре
        register = self._load_register_base(hook, register_code)

        # 2. Источники данных
        register.sources = self._load_sources(hook, register.id)

        # 3. Маппинги колонок для каждого источника
        for source in register.sources:
            source.columns = self._load_column_mappings(hook, source.id)

        # 4. Связываем parent_source
        self._link_parent_sources(register)

        # 5. UNION-ы
        register.unions = self._load_unions(hook, register.id)

        # 6. Связываем union members с sources
        self._link_union_members(register)

        # 7. Целевые таблицы
        register.targets = self._load_targets(hook, register.id)

        # 8. Связываем targets с unions/sources
        self._link_targets(register)

        return register

    def _load_register_base(self, hook, register_code: str) -> RegisterConfig:
        """Загрузка основной информации о регистре."""
        sql = """
            SELECT id, code, name, description, default_mode,
                   pipeline_type, retail_table, retail_uid_column
            FROM etl_meta.registers
            WHERE code = %s AND is_active = TRUE
        """
        df = hook.get_pandas_df(sql, parameters=[register_code])

        if df.empty:
            raise ValueError(f"Register '{register_code}' not found or inactive")

        row = df.iloc[0]
        import pandas as _pd  # noqa: F401 — для pd.notna ниже
        return RegisterConfig(
            id=int(row["id"]),
            code=row["code"],
            name=row["name"],
            description=row["description"],
            default_mode=row["default_mode"],
            pipeline_type=row["pipeline_type"] if _pd.notna(row.get("pipeline_type")) else None,
            retail_table=row["retail_table"],
            retail_uid_column=row["retail_uid_column"],
        )

    def _load_sources(self, hook, register_id: int) -> List[SourceConfig]:
        """Загрузка источников данных."""
        sql = """
            SELECT id, source_code, source_type, mssql_schema, mssql_table,
                   parent_source_id, join_type, join_key_source, join_key_parent,
                   where_clause, priority, period_column
            FROM etl_meta.register_sources
            WHERE register_id = %s AND is_active = TRUE
            ORDER BY priority, id
        """
        df = hook.get_pandas_df(sql, parameters=[register_id])

        sources = []
        for _, row in df.iterrows():
            # period_column: NULL → '_Period' (legacy), '' → None (без фильтра)
            raw_period = row.get("period_column")
            if raw_period is None or (isinstance(raw_period, float) and pd.isna(raw_period)):
                period_column = "_Period"
            else:
                period_column = str(raw_period).strip() or None

            sources.append(SourceConfig(
                id=int(row["id"]),
                source_code=row["source_code"],
                source_type=row["source_type"],
                mssql_schema=row["mssql_schema"] or "dbo",
                mssql_table=row["mssql_table"],
                parent_source_id=int(row["parent_source_id"]) if pd.notna(row["parent_source_id"]) else None,
                join_type=row["join_type"] if pd.notna(row["join_type"]) else None,
                join_key_source=row["join_key_source"] if pd.notna(row["join_key_source"]) else None,
                join_key_parent=row["join_key_parent"] if pd.notna(row["join_key_parent"]) else None,
                where_clause=row["where_clause"] if pd.notna(row["where_clause"]) else None,
                priority=int(row["priority"]) if pd.notna(row["priority"]) else 0,
                period_column=period_column,
            ))

        return sources

    def _load_column_mappings(self, hook, source_id: int) -> List[ColumnMapping]:
        """Загрузка маппингов колонок."""
        sql = """
            SELECT source_column, target_column, is_expression,
                   target_type, transform_type, transform_params,
                   default_value, is_nullable
            FROM etl_meta.column_mappings
            WHERE source_id = %s AND is_active = TRUE
            ORDER BY id
        """
        df = hook.get_pandas_df(sql, parameters=[source_id])

        mappings = []
        for _, row in df.iterrows():
            mappings.append(ColumnMapping(
                source_column=row["source_column"],
                target_column=row["target_column"],
                is_expression=bool(row["is_expression"]) if pd.notna(row["is_expression"]) else False,
                target_type=row["target_type"] if pd.notna(row["target_type"]) else None,
                transform_type=row["transform_type"] if pd.notna(row["transform_type"]) else None,
                transform_params=row["transform_params"] if pd.notna(row["transform_params"]) else None,
                default_value=row["default_value"] if pd.notna(row["default_value"]) else None,
                is_nullable=bool(row["is_nullable"]) if pd.notna(row["is_nullable"]) else True,
            ))

        return mappings

    def _link_parent_sources(self, register: RegisterConfig):
        """Связывает detail-источники с их parent-ами."""
        for source in register.sources:
            if source.parent_source_id:
                source.parent_source = register.get_source_by_id(source.parent_source_id)

    def _load_unions(self, hook, register_id: int) -> List[UnionConfig]:
        """Загрузка UNION-конфигураций."""
        sql = """
            SELECT id, union_code, description, output_columns
            FROM etl_meta.source_unions
            WHERE register_id = %s AND is_active = TRUE
            ORDER BY id
        """
        df = hook.get_pandas_df(sql, parameters=[register_id])

        unions = []
        for _, row in df.iterrows():
            # Handle output_columns which can be a list/array
            output_columns = row["output_columns"]
            if output_columns is None or (isinstance(output_columns, float) and pd.isna(output_columns)):
                output_columns = []
            elif hasattr(output_columns, 'tolist'):
                output_columns = output_columns.tolist()

            union_config = UnionConfig(
                id=int(row["id"]),
                union_code=row["union_code"],
                description=row["description"] if pd.notna(row["description"]) else None,
                output_columns=output_columns,
            )

            # Загружаем членов UNION-а
            union_config.members = self._load_union_members(hook, union_config.id)
            unions.append(union_config)

        return unions

    def _load_union_members(self, hook, union_id: int) -> List[UnionMember]:
        """Загрузка членов UNION-а."""
        sql = """
            SELECT source_id, priority, where_clause
            FROM etl_meta.source_union_members
            WHERE union_id = %s AND is_active = TRUE
            ORDER BY priority, id
        """
        df = hook.get_pandas_df(sql, parameters=[union_id])

        members = []
        for _, row in df.iterrows():
            members.append(UnionMember(
                source_id=int(row["source_id"]),
                priority=int(row["priority"]) if pd.notna(row["priority"]) else 0,
                where_clause=row["where_clause"] if pd.notna(row["where_clause"]) else None,
            ))

        return members

    def _link_union_members(self, register: RegisterConfig):
        """Связывает union members с их sources."""
        for union in register.unions:
            for member in union.members:
                member.source = register.get_source_by_id(member.source_id)

    def _load_targets(self, hook, register_id: int) -> List[TargetConfig]:
        """Загрузка целевых таблиц."""
        sql = """
            SELECT id, target_schema, target_table, union_id, source_id,
                   load_mode, upsert_keys, pre_load_sql, post_load_sql,
                   include_columns, priority, target_role, is_active
            FROM etl_meta.register_targets
            WHERE register_id = %s AND is_active = TRUE
            ORDER BY priority, id
        """
        df = hook.get_pandas_df(sql, parameters=[register_id])

        targets = []
        for _, row in df.iterrows():
            # Handle upsert_keys which can be a list/array
            upsert_keys = row["upsert_keys"]
            if upsert_keys is None or (isinstance(upsert_keys, float) and pd.isna(upsert_keys)):
                upsert_keys = []
            elif hasattr(upsert_keys, 'tolist'):
                upsert_keys = upsert_keys.tolist()

            # Handle include_columns
            include_columns = row.get("include_columns")
            if include_columns is None or (isinstance(include_columns, float) and pd.isna(include_columns)):
                include_columns = []
            elif hasattr(include_columns, 'tolist'):
                include_columns = include_columns.tolist()

            targets.append(TargetConfig(
                id=int(row["id"]),
                target_schema=row["target_schema"] if pd.notna(row["target_schema"]) else "public",
                target_table=row["target_table"],
                union_id=int(row["union_id"]) if pd.notna(row["union_id"]) else None,
                source_id=int(row["source_id"]) if pd.notna(row["source_id"]) else None,
                load_mode=row["load_mode"] if pd.notna(row["load_mode"]) else "upsert",
                upsert_keys=upsert_keys,
                pre_load_sql=row["pre_load_sql"] if pd.notna(row["pre_load_sql"]) else None,
                post_load_sql=row["post_load_sql"] if pd.notna(row.get("post_load_sql")) else None,
                include_columns=include_columns,
                priority=int(row["priority"]) if pd.notna(row.get("priority")) else 0,
                target_role=row["target_role"] if pd.notna(row.get("target_role")) else None,
                is_active=bool(row["is_active"]) if pd.notna(row["is_active"]) else True,
            ))

        return targets

    def _link_targets(self, register: RegisterConfig):
        """Связывает targets с unions/sources."""
        for target in register.targets:
            if target.union_id:
                target.union_config = register.get_union_by_id(target.union_id)
            if target.source_id:
                target.source_config = register.get_source_by_id(target.source_id)

    def list_registers(self) -> List[dict]:
        """Список всех активных регистров."""
        hook = PostgresHook(postgres_conn_id=self.config_conn_id)
        sql = """
            SELECT code, name, default_mode, is_active
            FROM etl_meta.registers
            WHERE is_active = TRUE
            ORDER BY code
        """
        df = hook.get_pandas_df(sql)
        return df.to_dict(orient="records")
