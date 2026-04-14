"""
Data Access Layer for etl_meta schema — standalone (no Airflow dependency).
"""

import json
import psycopg2
import psycopg2.extras
from typing import List, Optional


DB_CONFIG = {
    "host": "10.10.1.142",
    "port": 5432,
    "dbname": "test",
    "user": "airflow_admin",
    "password": "1234Aa",
}

RETAIL_DB_CONFIG = {
    "host": "10.10.1.142",
    "port": 5432,
    "dbname": "bd_retail",
    "user": "airflow_admin",
    "password": "1234Aa",
}

SCHEMA = "etl_meta"


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def get_retail_conn():
    return psycopg2.connect(**RETAIL_DB_CONFIG)


def query(sql: str, params=None) -> List[dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or [])
            if cur.description is None:
                return []
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def query_one(sql: str, params=None) -> Optional[dict]:
    rows = query(sql, params)
    return rows[0] if rows else None


def execute(sql: str, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
        conn.commit()
    finally:
        conn.close()


def insert_returning(sql: str, params=None) -> int:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or [])
            row = cur.fetchone()
        conn.commit()
        return dict(row)["id"] if row else 0
    finally:
        conn.close()


# ================================================================
#  REGISTERS
# ================================================================

def list_registers(include_inactive=False) -> List[dict]:
    sql = f"""
        SELECT id, code, name, description, default_mode,
               retail_table, retail_uid_column, is_active,
               created_at, updated_at
        FROM {SCHEMA}.registers
        {"" if include_inactive else "WHERE is_active = TRUE"}
        ORDER BY code
    """
    return query(sql)


def get_register(register_id: int) -> Optional[dict]:
    return query_one(
        f"SELECT * FROM {SCHEMA}.registers WHERE id = %s", [register_id]
    )


def create_register(data: dict) -> int:
    sql = f"""
        INSERT INTO {SCHEMA}.registers
            (code, name, description, default_mode, retail_table, retail_uid_column,
             parent_id, parent_join_key, child_join_key)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """
    return insert_returning(sql, [
        data["code"], data["name"], data.get("description"),
        data.get("default_mode", "incremental"),
        data.get("retail_table"), data.get("retail_uid_column"),
        data.get("parent_id"),
        data.get("parent_join_key"), data.get("child_join_key"),
    ])


def update_register(register_id: int, data: dict):
    sql = f"""
        UPDATE {SCHEMA}.registers SET
            code=%s, name=%s, description=%s, default_mode=%s,
            retail_table=%s, retail_uid_column=%s,
            parent_id=%s, parent_join_key=%s, child_join_key=%s,
            updated_at=NOW()
        WHERE id=%s
    """
    execute(sql, [
        data["code"], data["name"], data.get("description"),
        data.get("default_mode", "incremental"),
        data.get("retail_table"), data.get("retail_uid_column"),
        data.get("parent_id"),
        data.get("parent_join_key"), data.get("child_join_key"),
        register_id,
    ])


def delete_register(register_id: int):
    # Cascade delete all related data
    execute(f"DELETE FROM {SCHEMA}.load_history WHERE register_id=%s", [register_id])
    execute(f"DELETE FROM {SCHEMA}.register_targets WHERE register_id=%s", [register_id])
    # Delete union members, then unions
    execute(f"""
        DELETE FROM {SCHEMA}.source_union_members
        WHERE union_id IN (SELECT id FROM {SCHEMA}.source_unions WHERE register_id=%s)
    """, [register_id])
    execute(f"DELETE FROM {SCHEMA}.source_unions WHERE register_id=%s", [register_id])
    # Delete mappings, then sources
    execute(f"""
        DELETE FROM {SCHEMA}.column_mappings
        WHERE source_id IN (SELECT id FROM {SCHEMA}.register_sources WHERE register_id=%s)
    """, [register_id])
    execute(f"UPDATE {SCHEMA}.register_sources SET parent_source_id=NULL WHERE register_id=%s", [register_id])
    execute(f"DELETE FROM {SCHEMA}.register_sources WHERE register_id=%s", [register_id])
    execute(f"DELETE FROM {SCHEMA}.registers WHERE id=%s", [register_id])


def update_register_join_keys(register_id: int, parent_join_key: str, child_join_key: str):
    """Update only the join key fields on a register."""
    execute(
        f"UPDATE {SCHEMA}.registers SET parent_join_key=%s, child_join_key=%s, updated_at=NOW() WHERE id=%s",
        [parent_join_key, child_join_key, register_id],
    )


def update_register_type_map(register_id: int, type_map: dict):
    """Save recorder_type → document table mapping as JSONB."""
    execute(
        f"UPDATE {SCHEMA}.registers SET recorder_type_map=%s, updated_at=NOW() WHERE id=%s",
        [json.dumps(type_map, ensure_ascii=False), register_id],
    )


def toggle_register(register_id: int, is_active: bool):
    execute(
        f"UPDATE {SCHEMA}.registers SET is_active=%s, updated_at=NOW() WHERE id=%s",
        [is_active, register_id],
    )


# ================================================================
#  SOURCES
# ================================================================

def list_sources_for_register(register_id: int) -> List[dict]:
    sql = f"""
        SELECT s.*, ps.source_code as parent_source_code,
               (SELECT COUNT(*) FROM {SCHEMA}.column_mappings cm
                WHERE cm.source_id = s.id AND cm.is_active = TRUE) as column_count
        FROM {SCHEMA}.register_sources s
        LEFT JOIN {SCHEMA}.register_sources ps ON ps.id = s.parent_source_id
        WHERE s.register_id = %s
        ORDER BY s.priority, s.id
    """
    return query(sql, [register_id])


def get_source(source_id: int) -> Optional[dict]:
    sql = f"""
        SELECT s.*, r.code as register_code, r.id as register_id,
               ps.source_code as parent_source_code
        FROM {SCHEMA}.register_sources s
        JOIN {SCHEMA}.registers r ON r.id = s.register_id
        LEFT JOIN {SCHEMA}.register_sources ps ON ps.id = s.parent_source_id
        WHERE s.id = %s
    """
    return query_one(sql, [source_id])


def create_source(data: dict) -> int:
    fc = data.get("fields_cache")
    if fc and not isinstance(fc, str):
        fc = json.dumps(fc, ensure_ascii=False)
    sql = f"""
        INSERT INTO {SCHEMA}.register_sources
            (register_id, source_code, source_type, mssql_schema, mssql_table,
             onec_name, parent_source_id, join_type, join_key_source, join_key_parent,
             where_clause, priority, fields_cache)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """
    return insert_returning(sql, [
        data["register_id"], data["source_code"], data["source_type"],
        data.get("mssql_schema", "dbo"), data["mssql_table"],
        data.get("onec_name") or None,
        data.get("parent_source_id") or None,
        data.get("join_type") or None,
        data.get("join_key_source") or None,
        data.get("join_key_parent") or None,
        data.get("where_clause") or None,
        data.get("priority", 0),
        fc,
    ])


def update_source_fields_cache(source_id: int, fields: list):
    """Save 1C fields structure to cache."""
    fc = json.dumps(fields, ensure_ascii=False) if fields else None
    execute(
        f"UPDATE {SCHEMA}.register_sources SET fields_cache=%s WHERE id=%s",
        [fc, source_id],
    )


# Standard 1C system fields → default target column + type
_AUTO_MAPPING_RULES = {
    # key = field_name_sql.lower() from 1C API
    # source_column = actual MSSQL column name (with _ prefix)
    # Регистры накопления / сведений
    "period":    {"source_column": "_Period",       "target_column": "period",      "target_type": "timestamp", "transform_type": "fix_year", "is_nullable": False},
    "recorder":  {"source_column": "_RecorderRRef", "target_column": "recorder",    "target_type": "uuid",      "transform_type": "binary_to_uuid"},
    "lineno":    {"source_column": "_LineNo",       "target_column": "line_no",     "target_type": "integer"},
    "active":    {"source_column": "_Active",       "target_column": "is_active",   "target_type": "boolean"},
    # Документы / Справочники
    "id":        {"source_column": "_IDRRef",       "target_column": "id_ref",      "target_type": "uuid",      "transform_type": "binary_to_uuid", "is_nullable": False},
    "date_time": {"source_column": "_Date_Time",    "target_column": "date_time",   "target_type": "timestamp", "transform_type": "fix_year"},
    "posted":    {"source_column": "_Posted",       "target_column": "is_posted",   "target_type": "boolean"},
    "marked":    {"source_column": "_Marked",       "target_column": "is_deleted",  "target_type": "boolean"},
    "number":    {"source_column": "_Number",       "target_column": "doc_number",  "target_type": "varchar"},
    "code":      {"source_column": "_Code",         "target_column": "code",        "target_type": "varchar"},
    "description": {"source_column": "_Description", "target_column": "description", "target_type": "varchar"},
    # RecorderTRef — тип документа-регистратора
    # По умолчанию binary_to_int → integer, но если у регистра есть recorder_type_map,
    # auto_create_mappings заменит на recorder_type_lookup → varchar (название документа)
    "recordertref": {"source_column": "_RecorderTRef", "target_column": "recorder_type", "target_type": "varchar", "transform_type": "recorder_type_lookup"},
}


def auto_create_mappings(source_id: int, fields: list):
    """
    Auto-create column_mappings for known system fields from 1C API structure.
    fields = [{"field_name": "Период", "field_name_sql": "Period"}, ...]
    Skips fields that already have a mapping for this source.
    source_column stored as actual MSSQL name (with _ prefix).

    For RecorderTRef: if register has recorder_type_map, uses recorder_type_lookup
    transform with the map as params (converts binary→document name).
    """
    existing = {m["source_column"].lower() for m in list_mappings_for_source(source_id)}
    created = 0

    # Check if register has recorder_type_map for lookup
    source = get_source(source_id)
    reg = get_register(source["register_id"]) if source else None
    type_map = None
    if reg and reg.get("recorder_type_map"):
        raw = reg["recorder_type_map"]
        if isinstance(raw, str):
            raw = json.loads(raw)
        # Build simple {int_str: onec_name} lookup
        type_map = {}
        for k, v in raw.items():
            if isinstance(v, dict):
                type_map[str(k)] = v.get("onec_name") or v.get("mssql_table", str(k))
            else:
                type_map[str(k)] = str(v)

    for f in fields:
        api_name = f["field_name_sql"]
        key = api_name.lower()
        rule = _AUTO_MAPPING_RULES.get(key)
        if rule:
            mssql_col = rule["source_column"]
            if mssql_col.lower() in existing:
                continue
            mapping_data = {
                "source_id": source_id,
                "source_column": mssql_col,
                "onec_name": f["field_name"],
                **{k: v for k, v in rule.items() if k != "source_column"},
            }
            # If RecorderTRef and we have a type_map, inject it as transform_params
            if key == "recordertref" and type_map:
                mapping_data["transform_params"] = json.dumps({"map": type_map}, ensure_ascii=False)

            create_mapping(mapping_data)
            created += 1
    return created


def update_source(source_id: int, data: dict):
    sql = f"""
        UPDATE {SCHEMA}.register_sources SET
            source_code=%s, source_type=%s, mssql_schema=%s, mssql_table=%s,
            onec_name=%s, parent_source_id=%s, join_type=%s,
            join_key_source=%s, join_key_parent=%s,
            where_clause=%s, priority=%s
        WHERE id=%s
    """
    execute(sql, [
        data["source_code"], data["source_type"],
        data.get("mssql_schema", "dbo"), data["mssql_table"],
        data.get("onec_name") or None,
        data.get("parent_source_id") or None,
        data.get("join_type") or None,
        data.get("join_key_source") or None,
        data.get("join_key_parent") or None,
        data.get("where_clause") or None,
        data.get("priority", 0),
        source_id,
    ])


def delete_source(source_id: int):
    # Clear parent references pointing to this source
    execute(
        f"UPDATE {SCHEMA}.register_sources SET parent_source_id=NULL WHERE parent_source_id=%s",
        [source_id],
    )
    # Delete column mappings
    execute(f"DELETE FROM {SCHEMA}.column_mappings WHERE source_id=%s", [source_id])
    # Delete union memberships
    execute(f"DELETE FROM {SCHEMA}.source_union_members WHERE source_id=%s", [source_id])
    # Delete targets referencing this source
    execute(f"DELETE FROM {SCHEMA}.register_targets WHERE source_id=%s", [source_id])
    # Delete the source
    execute(f"DELETE FROM {SCHEMA}.register_sources WHERE id=%s", [source_id])


# ================================================================
#  COLUMN MAPPINGS
# ================================================================

def list_mappings_for_source(source_id: int) -> List[dict]:
    return query(
        f"SELECT * FROM {SCHEMA}.column_mappings WHERE source_id=%s ORDER BY id",
        [source_id],
    )


def get_mapping(mapping_id: int) -> Optional[dict]:
    sql = f"""
        SELECT cm.*, s.source_code, s.register_id
        FROM {SCHEMA}.column_mappings cm
        JOIN {SCHEMA}.register_sources s ON s.id = cm.source_id
        WHERE cm.id = %s
    """
    return query_one(sql, [mapping_id])


def _parse_transform_params(tp):
    """Normalize transform_params to valid JSON string or None."""
    if isinstance(tp, dict):
        return json.dumps(tp)
    if isinstance(tp, str):
        s = tp.strip()
        if not s or s.lower() == "none":
            return None
        try:
            json.loads(s)
            return s
        except json.JSONDecodeError:
            return None
    return None


def create_mapping(data: dict) -> int:
    tp = _parse_transform_params(data.get("transform_params"))

    sql = f"""
        INSERT INTO {SCHEMA}.column_mappings
            (source_id, source_column, target_column, is_expression,
             target_type, transform_type, transform_params,
             default_value, is_nullable, onec_name)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """
    return insert_returning(sql, [
        data["source_id"], data["source_column"], data["target_column"],
        data.get("is_expression", False),
        data.get("target_type") or None,
        data.get("transform_type") or None, tp,
        data.get("default_value") or None,
        data.get("is_nullable", True),
        data.get("onec_name") or None,
    ])


def create_mappings_batch(source_id: int, mappings: List[dict]):
    for m in mappings:
        m["source_id"] = source_id
        create_mapping(m)


def update_mapping(mapping_id: int, data: dict):
    tp = _parse_transform_params(data.get("transform_params"))

    sql = f"""
        UPDATE {SCHEMA}.column_mappings SET
            source_column=%s, target_column=%s, is_expression=%s,
            target_type=%s, transform_type=%s, transform_params=%s,
            default_value=%s, is_nullable=%s
        WHERE id=%s
    """
    execute(sql, [
        data["source_column"], data["target_column"],
        data.get("is_expression", False),
        data.get("target_type") or None,
        data.get("transform_type") or None, tp,
        data.get("default_value") or None,
        data.get("is_nullable", True),
        mapping_id,
    ])


def delete_mapping(mapping_id: int):
    execute(f"DELETE FROM {SCHEMA}.column_mappings WHERE id=%s", [mapping_id])


def toggle_mapping(mapping_id: int, is_active: bool):
    execute(
        f"UPDATE {SCHEMA}.column_mappings SET is_active=%s WHERE id=%s",
        [is_active, mapping_id],
    )


# ================================================================
#  UNIONS
# ================================================================

def list_unions_for_register(register_id: int) -> List[dict]:
    sql = f"""
        SELECT u.*,
               (SELECT COUNT(*) FROM {SCHEMA}.source_union_members m
                WHERE m.union_id = u.id AND m.is_active = TRUE) as member_count
        FROM {SCHEMA}.source_unions u
        WHERE u.register_id = %s ORDER BY u.id
    """
    return query(sql, [register_id])


def get_union(union_id: int) -> Optional[dict]:
    sql = f"""
        SELECT u.*, r.code as register_code, r.id as register_id
        FROM {SCHEMA}.source_unions u
        JOIN {SCHEMA}.registers r ON r.id = u.register_id
        WHERE u.id = %s
    """
    return query_one(sql, [union_id])


def create_union(data: dict) -> int:
    oc = data.get("output_columns", [])
    if isinstance(oc, str):
        oc = [c.strip() for c in oc.split(",") if c.strip()]
    sql = f"""
        INSERT INTO {SCHEMA}.source_unions
            (register_id, union_code, description, output_columns)
        VALUES (%s,%s,%s,%s) RETURNING id
    """
    return insert_returning(sql, [
        data["register_id"], data["union_code"],
        data.get("description") or None, oc,
    ])


def update_union(union_id: int, data: dict):
    oc = data.get("output_columns", [])
    if isinstance(oc, str):
        oc = [c.strip() for c in oc.split(",") if c.strip()]
    sql = f"""
        UPDATE {SCHEMA}.source_unions SET
            union_code=%s, description=%s, output_columns=%s
        WHERE id=%s
    """
    execute(sql, [data["union_code"], data.get("description") or None, oc, union_id])


def delete_union(union_id: int):
    execute(f"DELETE FROM {SCHEMA}.source_union_members WHERE union_id=%s", [union_id])
    execute(f"DELETE FROM {SCHEMA}.register_targets WHERE union_id=%s", [union_id])
    execute(f"DELETE FROM {SCHEMA}.source_unions WHERE id=%s", [union_id])


# ================================================================
#  UNION MEMBERS
# ================================================================

def list_members_for_union(union_id: int) -> List[dict]:
    sql = f"""
        SELECT m.*, s.source_code, s.mssql_table
        FROM {SCHEMA}.source_union_members m
        JOIN {SCHEMA}.register_sources s ON s.id = m.source_id
        WHERE m.union_id = %s ORDER BY m.priority, m.id
    """
    return query(sql, [union_id])


def get_member(member_id: int) -> Optional[dict]:
    sql = f"""
        SELECT m.*, u.union_code, u.register_id
        FROM {SCHEMA}.source_union_members m
        JOIN {SCHEMA}.source_unions u ON u.id = m.union_id
        WHERE m.id = %s
    """
    return query_one(sql, [member_id])


def create_member(data: dict) -> int:
    sql = f"""
        INSERT INTO {SCHEMA}.source_union_members
            (union_id, source_id, priority, where_clause)
        VALUES (%s,%s,%s,%s) RETURNING id
    """
    return insert_returning(sql, [
        data["union_id"], data["source_id"],
        data.get("priority", 0), data.get("where_clause") or None,
    ])


def update_member(member_id: int, data: dict):
    sql = f"""
        UPDATE {SCHEMA}.source_union_members SET
            source_id=%s, priority=%s, where_clause=%s
        WHERE id=%s
    """
    execute(sql, [
        data["source_id"], data.get("priority", 0),
        data.get("where_clause") or None, member_id,
    ])


def delete_member(member_id: int):
    execute(f"DELETE FROM {SCHEMA}.source_union_members WHERE id=%s", [member_id])


# ================================================================
#  TARGETS
# ================================================================

def list_targets_for_register(register_id: int) -> List[dict]:
    sql = f"""
        SELECT t.*, u.union_code, s.source_code
        FROM {SCHEMA}.register_targets t
        LEFT JOIN {SCHEMA}.source_unions u ON u.id = t.union_id
        LEFT JOIN {SCHEMA}.register_sources s ON s.id = t.source_id
        WHERE t.register_id = %s ORDER BY t.id
    """
    return query(sql, [register_id])


def get_target(target_id: int) -> Optional[dict]:
    sql = f"""
        SELECT t.*, r.code as register_code, r.id as register_id
        FROM {SCHEMA}.register_targets t
        JOIN {SCHEMA}.registers r ON r.id = t.register_id
        WHERE t.id = %s
    """
    return query_one(sql, [target_id])


def create_target(data: dict) -> int:
    uk = data.get("upsert_keys", [])
    if isinstance(uk, str):
        uk = [k.strip() for k in uk.split(",") if k.strip()]
    sql = f"""
        INSERT INTO {SCHEMA}.register_targets
            (register_id, target_schema, target_table,
             union_id, source_id, load_mode, upsert_keys, pre_load_sql)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """
    return insert_returning(sql, [
        data["register_id"],
        data.get("target_schema", "public"), data["target_table"],
        data.get("union_id") or None, data.get("source_id") or None,
        data.get("load_mode", "upsert"), uk,
        data.get("pre_load_sql") or None,
    ])


def update_target(target_id: int, data: dict):
    uk = data.get("upsert_keys", [])
    if isinstance(uk, str):
        uk = [k.strip() for k in uk.split(",") if k.strip()]

    # Check if table name or schema changed — sync real DB table
    old = get_target(target_id)
    if old:
        old_schema = old.get("target_schema", "public")
        old_table = old.get("target_table")
        new_schema = data.get("target_schema", "public")
        new_table = data["target_table"]
        _sync_real_table(old_schema, old_table, new_schema, new_table)

    sql = f"""
        UPDATE {SCHEMA}.register_targets SET
            target_schema=%s, target_table=%s,
            union_id=%s, source_id=%s,
            load_mode=%s, upsert_keys=%s, pre_load_sql=%s
        WHERE id=%s
    """
    execute(sql, [
        data.get("target_schema", "public"), data["target_table"],
        data.get("union_id") or None, data.get("source_id") or None,
        data.get("load_mode", "upsert"), uk,
        data.get("pre_load_sql") or None,
        target_id,
    ])


def _sync_real_table(old_schema, old_table, new_schema, new_table):
    """Rename/move the real data table when config changes."""
    if old_schema == new_schema and old_table == new_table:
        return
    exists = query_one(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        [old_schema, old_table],
    )
    if not exists:
        return
    if old_schema != new_schema:
        execute(f'ALTER TABLE "{old_schema}"."{old_table}" SET SCHEMA "{new_schema}"')
    if old_table != new_table:
        execute(f'ALTER TABLE "{new_schema}"."{old_table}" RENAME TO "{new_table}"')


# ================================================================
#  REAL TABLE SYNC — etl_meta config ↔ actual PostgreSQL tables
# ================================================================

# Map etl_meta target_type → PostgreSQL DDL type
_PG_TYPE_MAP = {
    "varchar": "VARCHAR(255)",
    "text": "TEXT",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "numeric": "NUMERIC(18,4)",
    "boolean": "BOOLEAN",
    "uuid": "UUID",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
    "jsonb": "JSONB",
    "bytea": "BYTEA",
}


def _resolve_pg_type(target_type: Optional[str]) -> str:
    """Convert etl_meta target_type to PostgreSQL DDL type string."""
    if not target_type:
        return "TEXT"
    t = target_type.strip().lower()
    # Already has precision e.g. varchar(100), numeric(10,2)
    if "(" in t:
        return t.upper() if t.startswith("var") else t
    return _PG_TYPE_MAP.get(t, "TEXT")


def sync_target_table(target_id: int):
    """
    Ensure the real PostgreSQL table matches the etl_meta config.
    - Creates the table if it doesn't exist
    - Adds missing columns
    - Renames columns if target_column changed
    - Changes column types if target_type changed
    Does NOT drop columns (safety).
    """
    target = get_target(target_id)
    if not target:
        return

    schema = target.get("target_schema", "public")
    table = target["target_table"]

    # Gather columns from ALL sources linked to this target
    source_id = target.get("source_id")
    union_id = target.get("union_id")

    mappings = []
    if source_id:
        mappings = list_mappings_for_source(source_id)
    elif union_id:
        # Union: gather mappings from all member sources
        members = list_members_for_union(union_id)
        seen = set()
        for m in members:
            for cm in list_mappings_for_source(m["source_id"]):
                if cm["target_column"] not in seen:
                    mappings.append(cm)
                    seen.add(cm["target_column"])

    if not mappings:
        return

    # Check if table exists
    exists = query_one(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        [schema, table],
    )

    if not exists:
        # CREATE TABLE with all mapped columns + system columns
        cols = []
        for m in mappings:
            if not m.get("is_active", True):
                continue
            pg_type = _resolve_pg_type(m.get("target_type"))
            nullable = "NULL" if m.get("is_nullable", True) else "NOT NULL"
            default = f"DEFAULT {m['default_value']}" if m.get("default_value") else ""
            cols.append(f'    "{m["target_column"]}" {pg_type} {nullable} {default}'.rstrip())

        # Add standard ETL system columns
        cols.append('    "etl_loaded_at" TIMESTAMP DEFAULT NOW()')
        cols.append('    "etl_hash" TEXT')

        ddl = f'CREATE TABLE "{schema}"."{table}" (\n' + ",\n".join(cols) + "\n)"
        execute(ddl)
        return

    # Table exists — sync columns (ADD missing, ALTER types)
    existing_cols = {
        r["column_name"]: r
        for r in query(
            """SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable
               FROM information_schema.columns
               WHERE table_schema=%s AND table_name=%s""",
            [schema, table],
        )
    }

    for m in mappings:
        if not m.get("is_active", True):
            continue
        col_name = m["target_column"]
        pg_type = _resolve_pg_type(m.get("target_type"))

        if col_name not in existing_cols:
            # ADD column
            nullable = "" if m.get("is_nullable", True) else "NOT NULL"
            default = f"DEFAULT {m['default_value']}" if m.get("default_value") else ""
            execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{col_name}" {pg_type} {nullable} {default}'.rstrip())
        else:
            # Column exists — check if type needs update
            # ALTER TYPE only if explicitly set and different
            if m.get("target_type"):
                execute(
                    f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN "{col_name}" TYPE {pg_type} USING "{col_name}"::{pg_type.split("(")[0]}'
                )


def ensure_target_column(target_id: int, column_name: str, column_type: Optional[str] = None):
    """Ensure a column exists in the real target table (ADD if missing)."""
    target = get_target(target_id)
    if not target:
        return
    schema = target.get("target_schema", "public")
    table = target["target_table"]
    exists = query_one(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s",
        [schema, table, column_name],
    )
    if not exists:
        pg_type = _resolve_pg_type(column_type)
        execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{column_name}" {pg_type} NULL')


def sync_all_targets_for_register(register_id: int):
    """Sync all target tables for a register."""
    targets = list_targets_for_register(register_id)
    for t in targets:
        sync_target_table(t["id"])


def delete_target(target_id: int):
    execute(f"DELETE FROM {SCHEMA}.register_targets WHERE id=%s", [target_id])


def batch_create_document_sources(register_id: int, doc_types: list) -> dict:
    """
    Batch-create sources, mappings, union, members, and target
    for a list of document types.

    doc_types = [{"type_int": 476, "onec_name": "Документ.ЧекККМ"}, ...]

    Returns: {"sources": [...], "union_id": int, "target_id": int}
    """
    reg = get_register(register_id)
    if not reg:
        raise ValueError(f"Register {register_id} not found")

    created_sources = []

    # Document system fields (same as _AUTO_MAPPING_RULES for documents)
    doc_system_mappings = [
        {"source_column": "_IDRRef",    "target_column": "id_ref",     "target_type": "uuid",      "transform_type": "binary_to_uuid", "is_nullable": False},
        {"source_column": "_Date_Time", "target_column": "date_time",  "target_type": "timestamp",  "transform_type": "fix_year"},
        {"source_column": "_Posted",    "target_column": "is_posted",  "target_type": "boolean"},
        {"source_column": "_Marked",    "target_column": "is_deleted", "target_type": "boolean"},
        {"source_column": "_Number",    "target_column": "doc_number", "target_type": "varchar"},
    ]

    # Build lookup of existing sources for this register
    existing_sources = {s["source_code"]: s for s in list_sources_for_register(register_id)}

    for dt in doc_types:
        type_int = dt["type_int"]
        onec_name = dt.get("onec_name") or f"Document{type_int}"
        mssql_table = f"_Document{type_int}"
        source_code = f"doc_{type_int}"

        # Skip if source already exists
        if source_code in existing_sources:
            created_sources.append({
                "id": existing_sources[source_code]["id"],
                "source_code": source_code,
                "type_int": type_int,
                "existed": True,
            })
            continue

        # Create source (with fields_cache from 1C API)
        sid = create_source({
            "register_id": register_id,
            "source_code": source_code,
            "source_type": "standalone",
            "mssql_table": mssql_table,
            "onec_name": onec_name,
            "fields_cache": dt.get("fields", []),
        })

        # Create system field mappings
        for m in doc_system_mappings:
            create_mapping({"source_id": sid, **m})

        # Add doc_type expression mapping (constant = 1C name)
        create_mapping({
            "source_id": sid,
            "source_column": f"'{onec_name}'",
            "target_column": "doc_type",
            "target_type": "varchar",
            "is_expression": True,
        })

        created_sources.append({"id": sid, "source_code": source_code, "type_int": type_int})

    # --- VT (tabular part) sources ---
    created_vt_sources = []
    for dt in doc_types:
        type_int = dt["type_int"]
        parent_src = next((s for s in created_sources if s["type_int"] == type_int), None)
        if not parent_src:
            continue
        for vt in dt.get("vt_tables", []):
            vt_number = vt.get("vt_number", "")
            vt_mssql_table = vt["mssql_table"]  # _Document476_VT13626
            vt_onec_name = vt.get("onec_name") or f"Document{type_int}_VT{vt_number}"
            vt_source_code = f"doc_{type_int}_vt_{vt_number}"
            vt_join_key = f"_Document{type_int}_IDRRef"

            # Skip if VT source already exists
            if vt_source_code in existing_sources:
                created_vt_sources.append({
                    "id": existing_sources[vt_source_code]["id"],
                    "source_code": vt_source_code,
                    "type_int": type_int,
                    "vt_number": vt_number,
                    "parent_source_id": parent_src["id"],
                    "existed": True,
                })
                continue

            vt_sid = create_source({
                "register_id": register_id,
                "source_code": vt_source_code,
                "source_type": "detail",
                "mssql_table": vt_mssql_table,
                "onec_name": vt_onec_name,
                "parent_source_id": parent_src["id"],
                "join_type": "INNER JOIN",
                "join_key_source": vt_join_key,
                "join_key_parent": "_IDRRef",
                "fields_cache": vt.get("fields", []),
            })

            # Auto-create mappings from 1C field structure (if resolved by app.py)
            vt_fields = vt.get("fields", [])
            if vt_fields:
                auto_create_mappings(vt_sid, vt_fields)

            # FK mapping: _Document{N}_IDRRef → parent_id_ref (uuid)
            existing_maps = {m["source_column"].lower() for m in list_mappings_for_source(vt_sid)}
            if vt_join_key.lower() not in existing_maps:
                create_mapping({
                    "source_id": vt_sid,
                    "source_column": vt_join_key,
                    "target_column": "parent_id_ref",
                    "target_type": "uuid",
                    "transform_type": "binary_to_uuid",
                    "is_nullable": False,
                })

            # doc_type expression (same as parent document)
            doc_onec_name = dt.get("onec_name") or f"Document{type_int}"
            create_mapping({
                "source_id": vt_sid,
                "source_column": f"'{doc_onec_name}'",
                "target_column": "doc_type",
                "target_type": "varchar",
                "is_expression": True,
            })

            created_vt_sources.append({
                "id": vt_sid,
                "source_code": vt_source_code,
                "type_int": type_int,
                "vt_number": vt_number,
                "parent_source_id": parent_src["id"],
            })

    # Find or create union
    existing_unions = list_unions_for_register(register_id)
    union_id = None
    for u in existing_unions:
        if u["union_code"] == "all_documents":
            union_id = u["id"]
            break

    new_sources_only = [s for s in created_sources if not s.get("existed")]

    if union_id is None:
        output_columns = ["id_ref", "date_time", "is_posted", "is_deleted", "doc_number", "doc_type"]
        union_id = create_union({
            "register_id": register_id,
            "union_code": "all_documents",
            "description": f"UNION of {len(created_sources)} document types",
            "output_columns": output_columns,
        })
        # Add ALL sources as members (new + existing)
        for src in created_sources:
            create_member({"union_id": union_id, "source_id": src["id"]})
    else:
        # Add only new sources as union members
        existing_member_ids = {m["source_id"] for m in list_members_for_union(union_id)}
        for src in new_sources_only:
            if src["id"] not in existing_member_ids:
                create_member({"union_id": union_id, "source_id": src["id"]})

    # Find or create target
    existing_targets = list_targets_for_register(register_id)
    target_id = None
    for t in existing_targets:
        if t.get("union_id") == union_id:
            target_id = t["id"]
            break

    if target_id is None:
        target_id = create_target({
            "register_id": register_id,
            "target_schema": "public",
            "target_table": reg["code"],
            "union_id": union_id,
            "load_mode": "upsert",
            "upsert_keys": ["id_ref"],
        })

    # Sync the target table
    sync_target_table(target_id)

    return {
        "sources": created_sources,
        "vt_sources": created_vt_sources,
        "union_id": union_id,
        "target_id": target_id,
    }


# ================================================================
#  RETAIL — incremental change detection
# ================================================================

def get_retail_changes(retail_table: str, uid_column: str, since: str = None) -> List[dict]:
    """
    Query retail DB for changed records.
    Returns list of {uid, updated_at} for records changed since `since`.
    If since is None, returns latest 100 for preview.
    """
    conn = get_retail_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if since:
                cur.execute(
                    f"SELECT {uid_column} as uid, updated_at FROM public.{retail_table} "
                    f"WHERE updated_at > %s ORDER BY updated_at DESC",
                    [since],
                )
            else:
                cur.execute(
                    f"SELECT {uid_column} as uid, updated_at FROM public.{retail_table} "
                    f"ORDER BY updated_at DESC LIMIT 100",
                )
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_retail_tables() -> List[str]:
    """List all tables in retail DB."""
    conn = get_retail_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name"
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def get_retail_columns(table: str) -> List[dict]:
    """Get columns for a retail table."""
    conn = get_retail_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s "
                "ORDER BY ordinal_position",
                [table],
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ================================================================
#  LOAD HISTORY
# ================================================================

def list_load_history(register_id: int, limit: int = 20) -> List[dict]:
    sql = f"""
        SELECT h.*, t.target_table
        FROM {SCHEMA}.load_history h
        LEFT JOIN {SCHEMA}.register_targets t ON t.id = h.target_id
        WHERE h.register_id = %s
        ORDER BY h.started_at DESC LIMIT %s
    """
    return query(sql, [register_id, limit])
