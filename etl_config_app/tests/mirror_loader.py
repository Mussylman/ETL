"""
mirror_loader — точное зеркало dags/core/config/config_loader.py поверх psycopg2.

Зачем: ConfigLoader жёстко зашит на схему etl_meta и PostgresHook (Airflow).
Golden-тесту нужно читать конфиг из произвольной схемы (etl_test_ref / etl_test)
и получать ТЕ ЖЕ движковые dataclass'ы (core.config.models.RegisterConfig).

ВАЖНО: SQL-строки ниже скопированы из config_loader.py дословно
(заменено только `etl_meta.` → `{schema}.`). Если меняется ConfigLoader —
этот файл обязан меняться синхронно (golden-тест сломается и покажет это).
"""

import os
import sys

import psycopg2.extras

_DAGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "dags"))
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

from core.config.models import (  # noqa: E402
    ColumnMapping, SourceConfig, UnionConfig, UnionMember, TargetConfig, RegisterConfig,
)


def _rows(cur, sql, params=None):
    cur.execute(sql, params or [])
    return [dict(r) for r in cur.fetchall()]


def load_register(conn, register_code: str, schema: str = "etl_meta") -> RegisterConfig:
    """Полное зеркало ConfigLoader.load_register (та же последовательность шагов)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # 1. _load_register_base
        regs = _rows(cur, f"""
            SELECT id, code, name, description, default_mode,
                   retail_table, retail_uid_column
            FROM {schema}.registers
            WHERE code = %s AND is_active = TRUE
        """, [register_code])
        if not regs:
            raise ValueError(f"Register '{register_code}' not found or inactive")
        row = regs[0]
        register = RegisterConfig(
            id=int(row["id"]),
            code=row["code"],
            name=row["name"],
            description=row["description"],
            default_mode=row["default_mode"],
            retail_table=row["retail_table"],
            retail_uid_column=row["retail_uid_column"],
        )

        # 2. _load_sources
        src_rows = _rows(cur, f"""
            SELECT id, source_code, source_type, mssql_schema, mssql_table,
                   parent_source_id, join_type, join_key_source, join_key_parent,
                   where_clause, priority, period_column
            FROM {schema}.register_sources
            WHERE register_id = %s AND is_active = TRUE
            ORDER BY priority, id
        """, [register.id])

        def _period_column(raw):
            # как в ConfigLoader: NULL → '_Period' (legacy), '' → None (без фильтра)
            if raw is None:
                return "_Period"
            return str(raw).strip() or None

        register.sources = [SourceConfig(
            id=int(r["id"]),
            source_code=r["source_code"],
            source_type=r["source_type"],
            mssql_schema=r["mssql_schema"] or "dbo",
            mssql_table=r["mssql_table"],
            parent_source_id=int(r["parent_source_id"]) if r["parent_source_id"] is not None else None,
            join_type=r["join_type"],
            join_key_source=r["join_key_source"],
            join_key_parent=r["join_key_parent"],
            where_clause=r["where_clause"],
            priority=int(r["priority"]) if r["priority"] is not None else 0,
            period_column=_period_column(r["period_column"]),
        ) for r in src_rows]

        # 3. _load_column_mappings
        for source in register.sources:
            map_rows = _rows(cur, f"""
                SELECT source_column, target_column, is_expression,
                       target_type, transform_type, transform_params,
                       default_value, is_nullable
                FROM {schema}.column_mappings
                WHERE source_id = %s AND is_active = TRUE
                ORDER BY id
            """, [source.id])
            source.columns = [ColumnMapping(
                source_column=r["source_column"],
                target_column=r["target_column"],
                is_expression=bool(r["is_expression"]) if r["is_expression"] is not None else False,
                target_type=r["target_type"],
                transform_type=r["transform_type"],
                transform_params=r["transform_params"],
                default_value=r["default_value"],
                is_nullable=bool(r["is_nullable"]) if r["is_nullable"] is not None else True,
            ) for r in map_rows]

        # 4. _link_parent_sources
        for source in register.sources:
            if source.parent_source_id:
                source.parent_source = register.get_source_by_id(source.parent_source_id)

        # 5. _load_unions + 6. members
        union_rows = _rows(cur, f"""
            SELECT id, union_code, description, output_columns
            FROM {schema}.source_unions
            WHERE register_id = %s AND is_active = TRUE
            ORDER BY id
        """, [register.id])
        register.unions = []
        for r in union_rows:
            union = UnionConfig(
                id=int(r["id"]),
                union_code=r["union_code"],
                description=r["description"],
                output_columns=list(r["output_columns"] or []),
            )
            member_rows = _rows(cur, f"""
                SELECT source_id, priority, where_clause
                FROM {schema}.source_union_members
                WHERE union_id = %s AND is_active = TRUE
                ORDER BY priority, id
            """, [union.id])
            union.members = [UnionMember(
                source_id=int(m["source_id"]),
                priority=int(m["priority"]) if m["priority"] is not None else 0,
                where_clause=m["where_clause"],
            ) for m in member_rows]
            register.unions.append(union)

        for union in register.unions:
            for member in union.members:
                member.source = register.get_source_by_id(member.source_id)

        # 7. _load_targets
        target_rows = _rows(cur, f"""
            SELECT id, target_schema, target_table, union_id, source_id,
                   load_mode, upsert_keys, pre_load_sql, post_load_sql,
                   include_columns, priority, target_role, is_active
            FROM {schema}.register_targets
            WHERE register_id = %s AND is_active = TRUE
            ORDER BY priority, id
        """, [register.id])
        register.targets = [TargetConfig(
            id=int(r["id"]),
            target_schema=r["target_schema"] or "public",
            target_table=r["target_table"],
            union_id=int(r["union_id"]) if r["union_id"] is not None else None,
            source_id=int(r["source_id"]) if r["source_id"] is not None else None,
            load_mode=r["load_mode"] or "upsert",
            upsert_keys=list(r["upsert_keys"] or []),
            pre_load_sql=r["pre_load_sql"],
            post_load_sql=r["post_load_sql"],
            include_columns=list(r["include_columns"] or []),
            priority=int(r["priority"]) if r["priority"] is not None else 0,
            target_role=r["target_role"],
            is_active=bool(r["is_active"]) if r["is_active"] is not None else True,
        ) for r in target_rows]

        # 8. _link_targets
        for target in register.targets:
            if target.union_id:
                target.union_config = register.get_union_by_id(target.union_id)
            if target.source_id:
                target.source_config = register.get_source_by_id(target.source_id)

        return register
