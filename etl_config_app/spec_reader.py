"""
spec_reader — чтение конфига регистра из etl_meta/etl_test в RegisterSpec.

Обратная операция к spec_writer.write_spec: id-ссылки БД переводятся
в code-ссылки spec'а. Используется golden-тестом (round-trip) и будущим
экспортом/превью конфига.
"""

import re
from typing import Optional

import psycopg2.extras

from register_spec import (
    RegisterSpec, SourceSpec, ColumnSpec, UnionSpec, UnionMemberSpec, TargetSpec,
)

_SCHEMA_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _rows(cur, sql, params=None):
    cur.execute(sql, params or [])
    return [dict(r) for r in cur.fetchall()]


def read_spec(register_code: str, schema: str = "etl_meta", conn=None) -> RegisterSpec:
    """Читает полный конфиг регистра из указанной схемы и собирает RegisterSpec."""
    if not _SCHEMA_RE.match(schema):
        raise ValueError(f"Недопустимое имя схемы: {schema!r}")

    own_conn = conn is None
    if own_conn:
        import dao
        conn = dao.get_conn()

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            regs = _rows(cur, f"SELECT * FROM {schema}.registers WHERE code=%s", [register_code])
            if not regs:
                raise ValueError(f"Register '{register_code}' не найден в {schema}")
            reg = regs[0]

            src_rows = _rows(cur, f"""
                SELECT * FROM {schema}.register_sources
                WHERE register_id=%s ORDER BY priority, id
            """, [reg["id"]])
            code_by_id = {r["id"]: r["source_code"] for r in src_rows}

            def _period_column(raw):
                # NULL → '_Period' (legacy-default), '' → None (без фильтра)
                if raw is None:
                    return "_Period"
                return str(raw).strip() or None

            sources = []
            for r in src_rows:
                map_rows = _rows(cur, f"""
                    SELECT * FROM {schema}.column_mappings
                    WHERE source_id=%s ORDER BY id
                """, [r["id"]])
                sources.append(SourceSpec(
                    source_code=r["source_code"],
                    source_type=r["source_type"],
                    mssql_schema=r["mssql_schema"] or "dbo",
                    mssql_table=r["mssql_table"],
                    parent_source_code=code_by_id.get(r["parent_source_id"]),
                    join_type=r["join_type"],
                    join_key_source=r["join_key_source"],
                    join_key_parent=r["join_key_parent"],
                    where_clause=r["where_clause"],
                    priority=r["priority"] or 0,
                    period_column=_period_column(r.get("period_column")),
                    is_active=r["is_active"],
                    onec_name=r.get("onec_name"),
                    fields_cache=r.get("fields_cache"),
                    columns=[ColumnSpec(
                        source_column=m["source_column"],
                        target_column=m["target_column"],
                        is_expression=bool(m["is_expression"]),
                        target_type=m["target_type"],
                        transform_type=m["transform_type"],
                        transform_params=m["transform_params"],
                        default_value=m["default_value"],
                        is_nullable=m["is_nullable"] if m["is_nullable"] is not None else True,
                        is_active=m["is_active"] if m["is_active"] is not None else True,
                        onec_name=m.get("onec_name"),
                    ) for m in map_rows],
                ))

            union_rows = _rows(cur, f"""
                SELECT * FROM {schema}.source_unions
                WHERE register_id=%s ORDER BY id
            """, [reg["id"]])
            unions = []
            for u in union_rows:
                member_rows = _rows(cur, f"""
                    SELECT * FROM {schema}.source_union_members
                    WHERE union_id=%s ORDER BY priority, id
                """, [u["id"]])
                unions.append(UnionSpec(
                    union_code=u["union_code"],
                    description=u["description"],
                    output_columns=list(u["output_columns"] or []),
                    is_active=u["is_active"],
                    members=[UnionMemberSpec(
                        source_code=code_by_id[m["source_id"]],
                        priority=m["priority"] or 0,
                        where_clause=m["where_clause"],
                        is_active=m["is_active"],
                    ) for m in member_rows],
                ))
            union_code_by_id = {u["id"]: u["union_code"] for u in union_rows}

            target_rows = _rows(cur, f"""
                SELECT * FROM {schema}.register_targets
                WHERE register_id=%s ORDER BY priority, id
            """, [reg["id"]])
            targets = [TargetSpec(
                target_schema=t["target_schema"] or "public",
                target_table=t["target_table"],
                source_code=code_by_id.get(t["source_id"]),
                union_code=union_code_by_id.get(t["union_id"]),
                load_mode=t["load_mode"] or "upsert",
                upsert_keys=list(t["upsert_keys"] or []),
                pre_load_sql=t["pre_load_sql"],
                post_load_sql=t["post_load_sql"],
                include_columns=list(t["include_columns"] or []),
                priority=t["priority"] or 0,
                target_role=t["target_role"],
                is_active=t["is_active"],
            ) for t in target_rows]

            return RegisterSpec(
                code=reg["code"],
                name=reg["name"],
                description=reg["description"],
                default_mode=reg["default_mode"],
                retail_table=reg["retail_table"],
                retail_uid_column=reg["retail_uid_column"],
                is_active=reg["is_active"],
                recorder_type_map=reg.get("recorder_type_map"),
                sources=sources,
                unions=unions,
                targets=targets,
            )
    finally:
        if own_conn:
            conn.close()
