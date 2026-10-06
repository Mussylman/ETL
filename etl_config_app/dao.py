"""
Data Access Layer for etl_meta schema — standalone (no Airflow dependency).
"""

import json
import os
import re
import psycopg2
import psycopg2.extras
from typing import List, Optional, Dict


# Подключение к etl_meta задаётся окружением: PROD UI :5556 → ETL_CONFIG_DB_NAME=etl_prod.
# База по умолчанию намеренно не задана: TEST-экземпляр :5555 (база test) удалён 2026-09-28,
# и запуск без явного ETL_CONFIG_DB_NAME не должен молча писать в пассивную тестовую базу.


def _required(name: str) -> str:
    """Обязательная переменная окружения; нет — отказ до подключения. Значение в лог не пишется."""
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"{name} не задан — секреты и база конфигуратора приходят только из окружения "
                           f"(PROD: ~/.config/etl_config/prod.env, см. etl_config_app/RUNNING.md)")
    return v


_required("ETL_CONFIG_DB_NAME")
DB_CONFIG = {
    "host": os.getenv("ETL_CONFIG_DB_HOST", "10.10.1.142"),
    "port": int(os.getenv("ETL_CONFIG_DB_PORT", "5432")),
    "dbname": os.environ["ETL_CONFIG_DB_NAME"],
    "user": _required("ETL_CONFIG_DB_USER"),
    "password": _required("ETL_CONFIG_DB_PASSWORD"),
}
IS_PROD = DB_CONFIG["dbname"] == "etl_prod" or os.getenv("ETL_CONFIG_ENV", "").lower() == "prod"
ENV_LABEL = os.getenv("ETL_CONFIG_ENV_LABEL") or (
    f"PROD / {DB_CONFIG['dbname']}" if IS_PROD else f"TEST / {DB_CONFIG['dbname']}"
)
# Destructive DDL (DROP COLUMN / сужение типа / recreate таблицы) через UI выполняется
# только при явном ETL_CONFIG_ALLOW_DESTRUCTIVE=1. По умолчанию — и в PROD, и в TEST —
# такие действия остаются в плане с requires_confirm, но не применяются: их место в миграции.
ALLOW_DESTRUCTIVE = os.getenv("ETL_CONFIG_ALLOW_DESTRUCTIVE", "0") == "1"

# retail конфигуратора — только из окружения; проверяется при первом подключении (страницы retail)
RETAIL_ENV = ("ETL_CONFIG_RETAIL_DB_HOST", "ETL_CONFIG_RETAIL_DB_NAME",
              "ETL_CONFIG_RETAIL_DB_USER", "ETL_CONFIG_RETAIL_DB_PASSWORD")

SCHEMA = "etl_meta"


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def get_retail_conn():
    host, dbname, user, password = (_required(n) for n in RETAIL_ENV)
    return psycopg2.connect(host=host, port=int(os.getenv("ETL_CONFIG_RETAIL_DB_PORT", "5432")),
                            dbname=dbname, user=user, password=password)


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
        SELECT r.id, r.code, r.name, r.description, r.default_mode,
               r.retail_table, r.retail_uid_column, r.is_active,
               r.created_at, r.updated_at,
               (SELECT array_agg(t.target_table ORDER BY t.priority, t.id)
                  FROM {SCHEMA}.register_targets t
                  WHERE t.register_id = r.id AND t.is_active) AS target_tables,
               -- последняя синхронизация — из обоих журналов: load_history (загрузка в PostgreSQL:
               -- справочники, старый путь фактов) и ch_sync_history (прямой путь 1С → ClickHouse,
               -- активные ch_sync с source_object = код регистра)
               GREATEST(
                   (SELECT MAX(h.finished_at) FROM {SCHEMA}.load_history h
                      WHERE h.register_id = r.id AND h.status = 'success'),
                   (SELECT MAX(h.finished_at) FROM {SCHEMA}.ch_sync_history h
                      JOIN {SCHEMA}.ch_sync s ON s.id = h.sync_id
                      WHERE s.source_type = 'onec_register' AND s.source_object = r.code
                        AND s.is_active AND h.status = 'success')) AS last_success_at,
               (SELECT x.status FROM (
                    SELECT h.status, h.started_at, 0 AS ord FROM {SCHEMA}.load_history h
                     WHERE h.register_id = r.id
                    UNION ALL
                    SELECT h.status, h.started_at, 1 FROM {SCHEMA}.ch_sync_history h
                      JOIN {SCHEMA}.ch_sync s ON s.id = h.sync_id
                     WHERE s.source_type = 'onec_register' AND s.source_object = r.code AND s.is_active) x
                  ORDER BY x.started_at DESC NULLS LAST, x.ord DESC LIMIT 1) AS last_status
        FROM {SCHEMA}.registers r
        {"" if include_inactive else "WHERE r.is_active = TRUE"}
        ORDER BY r.code
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
             parent_id, parent_join_key, child_join_key, pipeline_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """
    return insert_returning(sql, [
        data["code"], data["name"], data.get("description"),
        data.get("default_mode", "incremental"),
        data.get("retail_table"), data.get("retail_uid_column"),
        data.get("parent_id"),
        data.get("parent_join_key"), data.get("child_join_key"),
        data.get("pipeline_type"),
    ])


def update_register(register_id: int, data: dict):
    """
    PATCH-семантика: обновляются только ключи, присутствующие в data.
    pipeline_type / retail_table / retail_uid_column / default_mode и т.д. не затираются,
    если форма их не прислала. updated_at обновляется всегда.
    """
    cols = ("code", "name", "description", "default_mode", "retail_table", "retail_uid_column",
            "parent_id", "parent_join_key", "child_join_key", "pipeline_type", "recorder_type_map")
    sets, params = [], []
    for c in cols:
        if c in data:
            v = data[c]
            if c in ("description", "retail_table", "retail_uid_column", "parent_id",
                     "parent_join_key", "child_join_key", "pipeline_type"):
                v = v or None
            if c == "recorder_type_map" and v is not None and not isinstance(v, str):
                v = json.dumps(v, ensure_ascii=False)
            sets.append(f"{c}=%s"); params.append(v)
    if not sets:
        return
    params.append(register_id)
    execute(f"UPDATE {SCHEMA}.registers SET {', '.join(sets)}, updated_at=NOW() WHERE id=%s", params)


def _targets_with_data(targets: List[dict]) -> List[str]:
    """Таргеты, чьи физические таблицы существуют и непустые — их метаданные через UI не удаляем."""
    out = []
    for t in targets:
        schema = t.get("target_schema") or "public"
        if _table_exists(schema, t["target_table"]) and _table_row_count(schema, t["target_table"]) > 0:
            out.append(f'{schema}.{t["target_table"]}')
    return out


def delete_register(register_id: int):
    # Регистр с загруженными таблицами — рабочая витрина: движок читает её конфиг каждый тик.
    # Каскадное удаление метаданных из UI запрещено; сначала явно удалить таргеты (или миграцией).
    busy = _targets_with_data(list_targets_for_register(register_id))
    if busy:
        raise ValueError(
            f"регистр {register_id} питает таблицы с данными: {', '.join(busy)}. "
            f"Удаление конфига рабочей витрины через UI запрещено — только миграцией."
        )
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


def _normalize_source_type(source_type: str, mssql_table: str) -> str:
    """
    Корректировка source_type по физическому имени таблицы 1С.
    Регистры накопления/сведений (`_AccumRg*`, `_InfoRg*`, `_AccRg*`) — это
    всегда standalone, независимо от того что пришло из формы / Discover.
    UI иногда ставит 'header' для этих таблиц по ошибке — фиксим централизованно.
    """
    if not mssql_table:
        return source_type
    t = mssql_table.lstrip("_").lower()
    if t.startswith(("accumrg", "inforg", "accrg")):
        return "standalone"
    return source_type


def default_period_column(source_type: str, mssql_table: str) -> Optional[str]:
    """
    Колонка периода для фильтра QueryBuilder, если пользователь не задал явно:
      _Document{N}  (шапка документа)   → _Date_Time   (не _Period! у документов её нет)
      detail / _VT                      → ''  (пусто: фильтр ставится на родителя)
      регистры _AccumRg/_InfoRg/_AccRg  → _Period
    ConfigLoader: NULL → '_Period' (legacy), '' → без фильтра — поэтому для ТЧ пишем ''.
    """
    t = (mssql_table or "").lstrip("_").lower()
    if source_type == "detail" or "_vt" in t:
        return ""
    if re.match(r"^document\d+$", t):
        return "_Date_Time"
    return "_Period"


def create_source(data: dict) -> int:
    fc = data.get("fields_cache")
    if fc and not isinstance(fc, str):
        fc = json.dumps(fc, ensure_ascii=False)
    # Нормализуем тип — для регистров накопления/сведений всегда standalone
    source_type = _normalize_source_type(
        data.get("source_type", "standalone"),
        data.get("mssql_table", ""),
    )
    period_column = data.get("period_column")
    if period_column is None:
        period_column = default_period_column(source_type, data.get("mssql_table", ""))
    sql = f"""
        INSERT INTO {SCHEMA}.register_sources
            (register_id, source_code, source_type, mssql_schema, mssql_table,
             onec_name, parent_source_id, join_type, join_key_source, join_key_parent,
             where_clause, priority, fields_cache, period_column)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """
    return insert_returning(sql, [
        data["register_id"], data["source_code"], source_type,
        data.get("mssql_schema", "dbo"), data["mssql_table"],
        data.get("onec_name") or None,
        data.get("parent_source_id") or None,
        data.get("join_type") or None,
        data.get("join_key_source") or None,
        data.get("join_key_parent") or None,
        data.get("where_clause") or None,
        data.get("priority", 0),
        fc,
        period_column,
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


def auto_create_mappings(source_id: int, fields: list, target_id: Optional[int] = None):
    """
    Auto-create column_mappings for known system fields from 1C API structure.
    fields = [{"field_name": "Период", "field_name_sql": "Period"}, ...]
    Skips fields that already have a mapping for this source.
    source_column stored as actual MSSQL name (with _ prefix).

    Args:
        target_id: если известно куда колонки этого source направлены (например
                   document_header source → dim target, document_detail → fact),
                   маппинги получат это значение. Если NULL — определится позже
                   через target.include_columns (legacy путь).

    For RecorderTRef: if register has recorder_type_map, uses recorder_type_lookup
    transform with the map as params (converts binary→document name).
    """
    existing = {m["source_column"].lower() for m in list_mappings_for_source(source_id)}
    created = 0

    # Check if register has recorder_type_map for lookup
    source = get_source(source_id)
    reg = get_register(source["register_id"]) if source else None
    type_map = None
    register_id = reg["id"] if reg else None
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
                # Новые поля
                "target_id": target_id,
                "register_id": register_id,
                "is_auto": True,
                "is_required": False,
                **{k: v for k, v in rule.items() if k != "source_column"},
            }
            # If RecorderTRef and we have a type_map, inject it as transform_params
            if key == "recordertref" and type_map:
                mapping_data["transform_params"] = json.dumps({"map": type_map}, ensure_ascii=False)

            create_mapping(mapping_data)
            created += 1
    return created


def update_source(source_id: int, data: dict):
    # Та же нормализация, что в create_source — фикс UI ставит 'header' для _AccumRg
    source_type = _normalize_source_type(
        data.get("source_type", "standalone"),
        data.get("mssql_table", ""),
    )
    # period_column меняем только если прислали (PATCH для этого поля; '' = без фильтра)
    pc_set = ", period_column=%s" if "period_column" in data else ""
    sql = f"""
        UPDATE {SCHEMA}.register_sources SET
            source_code=%s, source_type=%s, mssql_schema=%s, mssql_table=%s,
            onec_name=%s, parent_source_id=%s, join_type=%s,
            join_key_source=%s, join_key_parent=%s,
            where_clause=%s, priority=%s{pc_set}
        WHERE id=%s
    """
    params = [
        data["source_code"], source_type,
        data.get("mssql_schema", "dbo"), data["mssql_table"],
        data.get("onec_name") or None,
        data.get("parent_source_id") or None,
        data.get("join_type") or None,
        data.get("join_key_source") or None,
        data.get("join_key_parent") or None,
        data.get("where_clause") or None,
        data.get("priority", 0),
    ]
    if "period_column" in data:
        params.append(data["period_column"] if data["period_column"] is not None else "")
    params.append(source_id)
    execute(sql, params)


def delete_source(source_id: int):
    """
    Источник, который питает таргет (source_id), входит в union или является родителем
    других источников, удалить нельзя: раньше каскад молча удалял таргет (у document_with_vt —
    orders вместе с post_load_sql) и ломал JOIN'ы детей. Сначала перепривязать/удалить явно.
    """
    fed = query(f"SELECT target_schema, target_table FROM {SCHEMA}.register_targets WHERE source_id=%s", [source_id])
    members = query(f"SELECT u.union_code FROM {SCHEMA}.source_union_members m JOIN {SCHEMA}.source_unions u ON u.id=m.union_id WHERE m.source_id=%s", [source_id])
    children = query(f"SELECT source_code FROM {SCHEMA}.register_sources WHERE parent_source_id=%s", [source_id])
    problems = []
    if fed:
        problems.append("питает таргет(ы): " + ", ".join(f'{t["target_schema"]}.{t["target_table"]}' for t in fed))
    if members:
        problems.append("входит в union: " + ", ".join(m["union_code"] for m in members))
    if children:
        problems.append("родитель для: " + ", ".join(c["source_code"] for c in children))
    if problems:
        raise ValueError(f"источник {source_id} нельзя удалить — " + "; ".join(problems) + ". Сначала перепривяжите или удалите зависимости явно.")
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
             default_value, is_nullable, onec_name,
             target_id, register_id, is_required, is_auto)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s) RETURNING id
    """
    return insert_returning(sql, [
        data["source_id"], data["source_column"], data["target_column"],
        data.get("is_expression", False),
        data.get("target_type") or None,
        data.get("transform_type") or None, tp,
        data.get("default_value") or None,
        data.get("is_nullable", True),
        data.get("onec_name") or None,
        # Новые поля (NULLABLE / defaults)
        data.get("target_id"),     # NULL если не известно (определится через include_columns)
        data.get("register_id"),   # для быстрых запросов
        bool(data.get("is_required", False)),
        bool(data.get("is_auto", True)),
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
    """
    Удаляет union и его членов. Таргеты, которые им питаются, НЕ удаляются:
    раньше здесь стоял каскадный DELETE register_targets WHERE union_id — удаление
    union молча уносило таргет (у document_with_vt это order_positions вместе с
    post_load_sql и include_columns). Теперь такой union удалить нельзя, пока
    таргет не перепривязан или не удалён явно.
    """
    fed = query(
        f"SELECT id, target_schema, target_table FROM {SCHEMA}.register_targets WHERE union_id=%s",
        [union_id],
    )
    if fed:
        names = ", ".join(f'{t["target_schema"]}.{t["target_table"]} (target {t["id"]})' for t in fed)
        raise ValueError(
            f"union {union_id} питает таргет(ы): {names}. Сначала перепривяжите или удалите таргет явно."
        )
    execute(f"DELETE FROM {SCHEMA}.source_union_members WHERE union_id=%s", [union_id])
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
    ic = data.get("include_columns") or None
    if isinstance(ic, str):
        ic = [c.strip() for c in ic.split(",") if c.strip()]
    sql = f"""
        INSERT INTO {SCHEMA}.register_targets
            (register_id, target_schema, target_table,
             union_id, source_id, load_mode, upsert_keys,
             pre_load_sql, post_load_sql, include_columns, priority, target_role,
             parent_target_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """
    return insert_returning(sql, [
        data["register_id"],
        data.get("target_schema", "public"), data["target_table"],
        data.get("union_id") or None, data.get("source_id") or None,
        data.get("load_mode", "upsert"), uk,
        data.get("pre_load_sql") or None,
        data.get("post_load_sql") or None,
        ic,
        data.get("priority", 0),
        data.get("target_role") or None,
        data.get("parent_target_id"),
    ])


def update_target(target_id: int, data: dict):
    """
    PATCH-семантика: меняются ТОЛЬКО ключи, присутствующие в data.

    Раньше UPDATE перезаписывал все поля таргета значениями из формы, а формы
    targets/form.html не содержит include_columns / target_role / post_load_sql /
    priority / parent_target_id — одно нажатие Save обнуляло их (аудит 2026-09-04:
    для PROD-регистров это уничтожило бы post_load-резолвы *_id и роли dim/fact).
    Поля, которых нет в data, не трогаем.
    """
    old = get_target(target_id)
    if not old:
        return

    # Переименование/перенос физической таблицы — только если имя реально прислали.
    # Таблицу с данными через UI не переименовываем: на неё смотрят BI, post_load и DAG.
    if "target_table" in data:
        old_schema = old.get("target_schema") or "public"
        new_schema = data.get("target_schema", old_schema) or "public"
        if (old_schema, old.get("target_table")) != (new_schema, data["target_table"]):
            if _table_exists(old_schema, old.get("target_table")) and _table_row_count(old_schema, old.get("target_table")) > 0:
                raise ValueError(
                    f'таблица {old_schema}.{old.get("target_table")} содержит данные — '
                    f"переименование через UI запрещено, только миграцией."
                )
        _sync_real_table(old_schema, old.get("target_table"), new_schema, data["target_table"])

    def _list(v):
        if isinstance(v, str):
            return [c.strip() for c in v.split(",") if c.strip()]
        return list(v) if v else []

    sets, params = [], []
    simple = {
        "target_schema": lambda v: v or "public",
        "target_table":  lambda v: v,
        "union_id":      lambda v: v or None,
        "source_id":     lambda v: v or None,
        "load_mode":     lambda v: v or "upsert",
        "pre_load_sql":  lambda v: v or None,
        "post_load_sql": lambda v: v or None,
        "priority":      lambda v: v if v is not None else 0,
        "target_role":   lambda v: v or None,
        "parent_target_id": lambda v: v or None,
        "is_active":     lambda v: bool(v),
    }
    for key, norm in simple.items():
        if key in data:
            sets.append(f"{key}=%s"); params.append(norm(data[key]))
    if "upsert_keys" in data:
        sets.append("upsert_keys=%s"); params.append(_list(data["upsert_keys"]))
    if "include_columns" in data:
        ic = _list(data["include_columns"])
        sets.append("include_columns=%s"); params.append(ic or None)
    if not sets:
        return
    params.append(target_id)
    execute(f"UPDATE {SCHEMA}.register_targets SET {', '.join(sets)} WHERE id=%s", params)


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


def _collect_mappings_for_target(target: dict) -> List[dict]:
    """Маппинги, формирующие DDL target-таблицы.

    Если у target есть include_columns — собираем колонки со ВСЕХ источников
    register (main + header + detail), оставляем только перечисленные в
    include_columns. Это нужно чтобы header-поля (например doc_number) попали
    в DDL sales, а fact-поля (nomenklatura, kolichestvo) — в DDL sales_positions.

    upsert_keys считаются ИМПЛИЦИТНО включёнными в DDL: без них ON CONFLICT не
    имеет смысла. Если пользователь не добавил их в include_columns — DDL builder
    всё равно подтянет, иначе CREATE TABLE упадёт на UNIQUE(...) с ссылкой на
    несуществующую колонку. Маппинг для upsert_key ищется во всех источниках
    register; если ни в одном нет — колонка просто не появится в DDL, и ошибка
    будет более понятной (а валидация target должна предупреждать на этом этапе).

    Если include_columns не задан — старая логика: маппинги от target.source_id
    или union_id (обратная совместимость).
    """
    include = target.get("include_columns") or []
    register_id = target.get("register_id")
    source_id = target.get("source_id")
    union_id = target.get("union_id")
    upsert_keys = target.get("upsert_keys") or []

    mappings: List[dict] = []
    seen = set()

    def _is_keepable(cm: dict) -> bool:
        # Сразу отбрасываем неактивные и custom_python — иначе они «съедают» dedup-имя.
        # Мэппинги raw_refs.<key> — не колонки таблицы: движок пакует их в JSONB raw_refs.
        return (cm.get("is_active", True) and cm.get("transform_type") != "custom_python"
                and not str(cm.get("target_column", "")).startswith("raw_refs."))

    if include and register_id:
        # Implicit-include: upsert_keys ВСЕГДА должны быть в DDL,
        # даже если пользователь забыл добавить их в include_columns.
        include_set = set(include) | set(upsert_keys)
        for src in list_sources_for_register(register_id):
            for cm in list_mappings_for_source(src["id"]):
                if not _is_keepable(cm):
                    continue
                tc = cm["target_column"]
                if tc in include_set and tc not in seen:
                    mappings.append(cm)
                    seen.add(tc)
    elif union_id:
        # Как и QueryBuilder._build_union_member_query: член union — это detail
        # JOIN его parent (header), колонки родителя доступны каждому члену
        # (recorder, recorder_type у document_with_vt живут в шапке). Без parent
        # Sync терял бы их и предлагал DROP на заполненной таблице.
        for m in list_members_for_union(union_id):
            member_src = get_source(m["source_id"])
            chain = [m["source_id"]]
            if member_src and member_src.get("parent_source_id"):
                chain.append(member_src["parent_source_id"])
            for sid in chain:
                for cm in list_mappings_for_source(sid):
                    if not _is_keepable(cm):
                        continue
                    if cm["target_column"] not in seen:
                        mappings.append(cm)
                        seen.add(cm["target_column"])
    elif source_id:
        mappings = [m for m in list_mappings_for_source(source_id) if _is_keepable(m)]

    return mappings


def _target_uses_raw_refs(target: dict) -> bool:
    """
    Нужна ли таргету колонка raw_refs JSONB (стандарт ссылок): либо 'raw_refs'
    явно в include_columns, либо у источников таргета есть мэппинги raw_refs.<key>.
    """
    include = target.get("include_columns") or []
    if "raw_refs" in include or any(str(c).startswith("raw_refs.") for c in include):
        return True
    register_id = target.get("register_id")
    if not register_id:
        return False
    for src in list_sources_for_register(register_id):
        for cm in list_mappings_for_source(src["id"]):
            if cm.get("is_active", True) and str(cm.get("target_column", "")).startswith("raw_refs."):
                return True
    return False


def _target_source_ids(target: dict) -> List[int]:
    """Источники, из которых таргет реально берёт колонки: source_id (+parent) или члены union (+parents)."""
    ids: List[int] = []
    def _add(sid):
        if sid and sid not in ids:
            ids.append(sid)
            src = get_source(sid)
            if src and src.get("parent_source_id"):
                _add(src["parent_source_id"])
    if target.get("source_id"):
        _add(target["source_id"])
    elif target.get("union_id"):
        for m in list_members_for_union(target["union_id"]):
            _add(m["source_id"])
    return ids


def _parse_params(tp):
    if isinstance(tp, str):
        try:
            return json.loads(tp)
        except Exception:
            return {}
    return tp if isinstance(tp, dict) else {}


def raw_ref_dim_links(target: dict) -> List[dict]:
    """
    Ссылки таргета на справочники по стандарту raw_refs: для каждого мэппинга
    raw_refs.<key> (не полиморфного .uid/.type) — dim-таблица и FK-колонка <key>_id.
    Таблица справочника: transform_params.dim у мэппинга (явно, напр. {"dim": "dim_counterparty"}
    для gruzopoluchatel) либо public.dim_<key>, если существует. Нерезолвимые ключи
    (vid_operatsii, tip_cen …) остаются только в raw_refs — фейковых *_id не бывает.
    """
    include = set(target.get("include_columns") or [])
    keys: Dict[str, Optional[str]] = {}
    for sid in _target_source_ids(target):
        for cm in list_mappings_for_source(sid):
            if not cm.get("is_active", True):
                continue
            tc = str(cm.get("target_column") or "")
            if not tc.startswith("raw_refs.") or (include and tc not in include):
                continue
            parts = tc.split(".")
            if len(parts) != 2:
                continue  # raw_refs.<key>.uid/.type — ссылка на документ/регистр
            key = parts[1]
            dim = _parse_params(cm.get("transform_params")).get("dim")
            if key not in keys or dim:
                keys[key] = dim
    out = []
    for key, dim in keys.items():
        dim_table = dim or f"dim_{key}"
        if _table_exists("public", dim_table):
            out.append({"key": key, "dim_table": dim_table, "fk_col": f"{key}_id"})
    return out


def raw_ref_register_links(target: dict) -> List[dict]:
    """
    Полиморфные ссылки raw_refs.<key>.uid + .type на ДРУГОЙ регистр витрины, объявленные
    в мэппинге .uid через transform_params {"ref_target": "orders"}. Даёт <key>_id BIGINT → ref.id.
    Без декларации ссылка живёт только в raw_refs (как zakaz до появления orders).
    """
    include = set(target.get("include_columns") or [])
    out = {}
    for sid in _target_source_ids(target):
        for cm in list_mappings_for_source(sid):
            tc = str(cm.get("target_column") or "")
            if not cm.get("is_active", True) or not tc.startswith("raw_refs.") or not tc.endswith(".uid"):
                continue
            if include and tc not in include:
                continue
            ref = _parse_params(cm.get("transform_params")).get("ref_target")
            key = tc.split(".")[1]
            if ref and key not in out:
                out[key] = {"key": key, "ref_table": ref, "fk_col": f"{key}_id"}
    return list(out.values())


def generate_post_load_sql(target_id: int) -> dict:
    """
    Шаблон post_load_sql из metadata и настроек таргета (ничего не сохраняет):
      1. positions → header: <fk> = header.id по natural key (recorder, recorder_type)
      2. raw_refs.<key> → dim.guid → <key>_id (stub ON CONFLICT + UPDATE)
      3. raw_refs.<key>.{type,uid} → другой регистр (ref_target) → <key>_id
      4. fact: удаление строк, исчезнувших из документа при перепроведении (окно 60/30 мин)
      5. dimension: late-resolve — таргеты, объявившие ref_target на ЭТУ таблицу, дозаполняют <key>_id
    Имена таблиц/колонок не хардкодятся — берутся из etl_meta и физической схемы.
    """
    target = get_target(target_id)
    if not target:
        return {"error": f"target {target_id} not found"}
    schema = target.get("target_schema") or "public"
    table = target["target_table"]
    full = f'{schema}.{table}'
    role = (target.get("target_role") or "").lower()
    uk = list(target.get("upsert_keys") or [])
    nk = [k for k in ("recorder", "recorder_type") if k in uk] or ["recorder"]
    parts = [f"-- post_load {full}: сгенерировано конфигуратором из metadata (стандарт ссылок raw_refs → *_id BIGINT)."]
    summary = []

    parent = None
    if role == "fact":
        parent = get_target(target["parent_target_id"]) if target.get("parent_target_id") else _find_dim_sibling(target)
        if parent and parent["id"] != target["id"]:
            pschema = parent.get("target_schema") or "public"
            fk = _resolve_fk_column(schema, table, parent["target_table"])
            cond = " AND ".join(f"p.{k} = h.{k}" for k in nk)
            parts.append(f"""-- FK на шапку по natural key ({', '.join(nk)}).
-- 1) не привязанные строки — где угодно
UPDATE {full} p SET {fk} = h.id FROM {pschema}.{parent['target_table']} h
WHERE p.{fk} IS NULL AND {cond};
-- 2) «висячие» ссылки: шапку пересоздали с новым id после отката упавшего прогона
--    (аудит 2026-09-15). Скоуп — документы последних загрузок, не вся таблица.
UPDATE {full} p SET {fk} = h.id FROM {pschema}.{parent['target_table']} h
WHERE h.etl_updated_at >= timezone('Asia/Almaty', now()) - interval '60 minutes'
  AND {cond} AND p.{fk} IS NOT NULL AND p.{fk} <> h.id;""")
            summary.append(f"{fk} → {parent['target_table']}.id")

    for l in raw_ref_dim_links(target):
        k, dim, fk = l["key"], l["dim_table"], l["fk_col"]
        parts.append(f"""-- {k}: stub в {dim} по незнакомому guid (id постоянный) + резолв {fk}
INSERT INTO public.{dim} (guid) SELECT DISTINCT (raw_refs->>'{k}')::uuid FROM {full} WHERE {fk} IS NULL AND raw_refs ? '{k}' ON CONFLICT (guid) DO NOTHING;
UPDATE {full} x SET {fk} = d.id FROM public.{dim} d WHERE x.{fk} IS NULL AND d.guid = (x.raw_refs->>'{k}')::uuid;""")
        summary.append(f"{fk} → {dim}")

    for l in raw_ref_register_links(target):
        k, ref, fk = l["key"], l["ref_table"], l["fk_col"]
        parts.append(f"""-- {k}: ссылка на регистр {ref} по natural key (type, uid); вне истории {ref} → NULL, {{type,uid}} остаются в raw_refs
UPDATE {full} s SET {fk} = h.id FROM public.{ref} h
WHERE s.{fk} IS NULL AND s.raw_refs ? '{k}'
  AND h.recorder_type = (s.raw_refs->'{k}'->>'type')::int AND h.recorder = (s.raw_refs->'{k}'->>'uid')::uuid;""")
        summary.append(f"{fk} → {ref}.id")

    if role == "fact":
        parent_fk = None
        if parent and parent["id"] != target["id"]:
            parent_fk = (f'{parent.get("target_schema") or "public"}.{parent["target_table"]}',
                         _resolve_fk_column(schema, table, parent["target_table"]))
        if parent_fk:
            # ориентир — шапка: она перезаписывается при каждой загрузке документа, поэтому видит и заказы,
            # у которых удалили ВСЕ строки (правило по строкам документа их пропускало — аудит 2026-09-14)
            parts.append(f"""-- строки, исчезнувшие из документа при перепроведении: строка старше своей шапки на 30+ минут
DELETE FROM {full} p
USING {parent_fk[0]} o
WHERE o.id = p.{parent_fk[1]}
  AND o.etl_updated_at >= timezone('Asia/Almaty', now()) - interval '60 minutes'
  AND p.etl_updated_at < o.etl_updated_at - interval '30 minutes';""")
        else:
            grp = ", ".join(nk)
            cond = " AND ".join(f"p.{k} = m.{k}" for k in nk)
            parts.append(f"""-- строки, исчезнувшие из документа при перепроведении (по строкам того же документа)
DELETE FROM {full} p
USING (SELECT {grp}, MAX(etl_updated_at) AS last_ts FROM {full}
       WHERE etl_updated_at >= timezone('Asia/Almaty', now()) - interval '60 minutes' GROUP BY {grp}) m
WHERE {cond} AND p.etl_updated_at < m.last_ts - interval '30 minutes';""")
        summary.append("удаление исчезнувших строк")

    if role == "dimension":
        # late-resolve: кто объявил ссылку на ЭТУ таблицу
        rows = query(f"SELECT DISTINCT t.id FROM {SCHEMA}.register_targets t WHERE t.id <> %s", [target_id])
        for r in rows:
            other = get_target(r["id"])
            if not other:
                continue
            for l in raw_ref_register_links(other):
                if l["ref_table"] != table:
                    continue
                oschema = other.get("target_schema") or "public"
                k, fk = l["key"], l["fk_col"]
                parts.append(f"""-- late-resolve {oschema}.{other['target_table']}.{fk}: документ пришёл позже ссылающейся строки
UPDATE {oschema}.{other['target_table']} s SET {fk} = h.id FROM {full} h
WHERE s.{fk} IS NULL AND s.raw_refs ? '{k}'
  AND h.recorder_type = (s.raw_refs->'{k}'->>'type')::int AND h.recorder = (s.raw_refs->'{k}'->>'uid')::uuid;""")
                summary.append(f"late-resolve {other['target_table']}.{fk}")

    return {"sql": "\n\n".join(parts) + "\n", "summary": "; ".join(summary) or "ссылок для резолва не найдено",
            "dim_links": raw_ref_dim_links(target), "register_links": raw_ref_register_links(target)}


def _fk_candidates(dim_table: str) -> List[str]:
    """
    Имена FK-колонки fact → dim, в порядке предпочтения. То же правило, что в
    ETLEngine._validate_full_period_load (dags/core/etl_engine.py): {dim}_id,
    затем без «s» для таблиц во множественном числе (sales → sales_id,
    orders → order_id). Никаких таблиц-исключений.
    """
    cands = [f"{dim_table}_id"]
    if dim_table.endswith("s") and len(dim_table) > 1:
        cands.append(f"{dim_table[:-1]}_id")
    return cands


def _resolve_fk_column(fact_schema: str, fact_table: str, dim_table: str) -> str:
    """Первая из кандидатных FK-колонок, которая уже есть в fact-таблице; иначе первая по правилу."""
    cands = _fk_candidates(dim_table)
    if _table_exists(fact_schema, fact_table):
        existing = _existing_columns(fact_schema, fact_table)
        for c in cands:
            if c in existing:
                return c
    return cands[0]


def _table_row_count(schema: str, table: str) -> int:
    """COUNT(*) для таблицы или 0 если её нет."""
    try:
        r = query_one(f'SELECT COUNT(*) AS n FROM "{schema}"."{table}"')
        return int(r["n"]) if r else 0
    except Exception:
        return 0


def _table_exists(schema: str, table: str) -> bool:
    return query_one(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        [schema, table],
    ) is not None


def _existing_columns(schema: str, table: str) -> Dict[str, dict]:
    return {
        r["column_name"]: r
        for r in query(
            """SELECT column_name, data_type, character_maximum_length, numeric_precision,
                      numeric_scale, is_nullable
               FROM information_schema.columns
               WHERE table_schema=%s AND table_name=%s""",
            [schema, table],
        )
    }


def _existing_constraints(schema: str, table: str) -> List[dict]:
    """PK/UNIQUE/FK констрейнты таблицы: [{name, type('p'|'u'|'f'), cols:set}]."""
    rows = query("""
        SELECT con.conname AS name, con.contype AS type,
               ARRAY(SELECT att.attname
                     FROM unnest(con.conkey) k
                     JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k
               ) AS cols
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND con.contype IN ('p', 'u', 'f')
    """, [schema, table])
    return [{"name": r["name"], "type": r["type"], "cols": set(r["cols"] or [])} for r in rows]


def _duplicate_key_groups(schema: str, table: str, keys: List[str]) -> int:
    """Сколько групп дублей по ключу — преграда для UNIQUE на непустой таблице."""
    cols = ", ".join(f'"{k}"' for k in keys)
    try:
        r = query_one(
            f'SELECT COUNT(*) AS n FROM '
            f'(SELECT 1 FROM "{schema}"."{table}" GROUP BY {cols} HAVING COUNT(*) > 1) d'
        )
        return int(r["n"]) if r else 0
    except Exception:
        return 0


def _find_dim_sibling(target: dict) -> Optional[dict]:
    """Dimension-target того же регистра (для FK-колонки fact→dim)."""
    rows = query(f"""
        SELECT * FROM {SCHEMA}.register_targets
        WHERE register_id = %s AND target_role = 'dimension' AND is_active = TRUE
        ORDER BY priority, id
    """, [target["register_id"]])
    return rows[0] if rows else None


def _dim_pk_info(dim_target: dict) -> dict:
    """
    PK dim-таблицы: натуральный 'id' из маппингов (uuid и т.п.) или
    синтетический BIGSERIAL. Возвращает {natural: bool, pg_type: str}.
    pg_type — тип FK-колонки в fact (BIGSERIAL → BIGINT).
    """
    for m in _collect_mappings_for_target(dim_target):
        if m["target_column"] == "id":
            return {"natural": True, "pg_type": _resolve_pg_type(m.get("target_type"))}
    return {"natural": False, "pg_type": "BIGINT"}


def _target_ddl_extras(target: dict, mappings: List[dict]) -> dict:
    """
    Контрактные DDL-инварианты target-таблицы (этап 0.2):
      • pk_natural    : маппинги порождают 'id' → он и есть PK (иначе BIGSERIAL)
      • unique_keys   : UNIQUE(upsert_keys) для load_mode=upsert (если ключ ≠ PK id)
      • add_updated_at: у dimension — колонка updated_at (источник watermark).
                        ИСКЛЮЧЕНИЕ — справочники (pipeline_type='reference_dim'):
                        они не участвуют в инкременте и watermark не дают,
                        их свежесть — etl_updated_at, проставляемый загрузчиком
                        имён. Без исключения Sync каждый раз предлагал бы
                        добавить в dim_* мёртвую колонку updated_at.
      • fk            : у fact — колонка {dim_table}_id → dim(id)
    """
    _reg = query_one(
        f"SELECT pipeline_type, retail_table FROM {SCHEMA}.registers WHERE id = %s",
        [target.get("register_id")],
    ) if target.get("register_id") else None
    is_reference_dim = bool(_reg and _reg.get("pipeline_type") == "reference_dim")
    extras = {
        "pk_natural": any(m["target_column"] == "id" for m in mappings),
        "unique_keys": None,
        # legacy updated_at больше НЕ добавляем никому: в чистой fact-модели свежесть
        # строки — etl_updated_at / retail_snapshot_at / retail_updated_at (см.
        # system_cols ниже). Уже существующая updated_at остаётся под защитой
        # SYSTEM_COLS в compute_sync_plan — старый контур TEST не ломаем.
        "add_updated_at": False,
        "fk": None,
        "is_reference_dim": is_reference_dim,
        # Контрактные системные колонки новой модели: движок пишет их всегда
        # (etl_engine._process_target*), raw_refs пакуется из мэппингов raw_refs.<key>.
        "system_cols": _contract_system_columns(target, is_reference_dim, bool(_reg and _reg.get("retail_table"))),
    }

    upsert_keys = list(target.get("upsert_keys") or [])
    if (target.get("load_mode") or "upsert") == "upsert" and upsert_keys and upsert_keys != ["id"]:
        extras["unique_keys"] = upsert_keys

    if target.get("target_role") == "fact":
        # Родитель — parent_target_id, если задан; иначе dimension-sibling регистра
        dim = None
        if target.get("parent_target_id"):
            dim = get_target(target["parent_target_id"])
        if dim is None:
            dim = _find_dim_sibling(target)
        if dim and dim["id"] != target["id"]:
            pk = _dim_pk_info(dim)
            extras["fk"] = {
                "column": _resolve_fk_column(
                    target.get("target_schema") or "public", target["target_table"], dim["target_table"]
                ),
                "candidates": _fk_candidates(dim["target_table"]),
                "pg_type": pk["pg_type"],
                "ref_schema": dim.get("target_schema", "public"),
                "ref_table": dim["target_table"],
            }
    return extras


def _contract_system_columns(target: dict, is_reference_dim: bool, has_retail: bool) -> List[tuple]:
    """
    Системные колонки, которые движок заполняет сам и которые обязаны быть в DDL.
    Возвращает [(column, ddl_type_with_default, reason)].

    Регистры/факты (accumrg_with_documents, document_with_vt, …):
      etl_loaded_at, etl_updated_at, retail_snapshot_at, retail_updated_at, raw_refs JSONB
      (последняя — только если таргет пользуется стандартом ссылок raw_refs.*).
    Справочники (reference_dim): etl_loaded_at, etl_updated_at, is_stub;
      retail_updated_at — только у справочников с retail-привязкой.
    """
    if is_reference_dim:
        # Контракт dim-слоя (load_dim_from_config + 007_dim_layer): etl_updated_at, is_stub,
        # retail_updated_at у retail-привязанных. etl_loaded_at DIM-загрузчик не пишет.
        cols = [("etl_updated_at", "TIMESTAMP", "dim-слой: момент последнего изменения строки загрузчиком"),
                # DEFAULT TRUE — не косметика: post_load факта заводит строку одним
                # INSERT ... (guid), флаг берётся из DEFAULT колонки. С DEFAULT FALSE
                # новый справочник молча не дал бы ни одного stub, и stub-pass
                # reference_dim никогда бы его не дозаполнил (миграция 007 — TRUE).
                ("is_stub", "BOOLEAN NOT NULL DEFAULT TRUE",
                 "dim-слой: строка создана stub-резолвом, имя ещё не приехало")]
        if has_retail:
            cols.append(("retail_updated_at", "TIMESTAMP", "retail-метка справочника (watermark инкремента)"))
        return cols
    cols = [("etl_loaded_at", "TIMESTAMP DEFAULT NOW()", "ETL audit: момент первой загрузки строки"),
            ("etl_updated_at", "TIMESTAMP", "ETL audit: момент последнего изменения строки движком")]
    cols.append(("retail_snapshot_at", "TIMESTAMP", "retail-снимок full_period (fallback watermark)"))
    cols.append(("retail_updated_at", "TIMESTAMP", "per-row retail-метка инкремента (watermark)"))
    if _target_uses_raw_refs(target):
        cols.append(("raw_refs", "JSONB", "стандарт ссылок: исходные GUID 1С одной JSONB-колонкой"))
        # BIGINT-ссылки на справочники/регистры, которые post_load резолвит из raw_refs
        for l in raw_ref_dim_links(target):
            cols.append((l["fk_col"], "BIGINT", f"FK на {l['dim_table']} (raw_refs.{l['key']})"))
        for l in raw_ref_register_links(target):
            cols.append((l["fk_col"], "BIGINT", f"FK на регистр {l['ref_table']} (raw_refs.{l['key']}.uid/type)"))
    return cols


# Какие переходы типов считаются безопасным расширением (без потери данных)
_SAFE_TYPE_WIDENING = {
    ("integer", "BIGINT"): True,
    ("integer", "NUMERIC(18,4)"): True,
    ("smallint", "INTEGER"): True,
    ("smallint", "BIGINT"): True,
    # text/varchar взаимные — текстовые типы между собой
}


def _is_type_widening(from_data_type: str, to_pg_type: str, from_maxlen=None) -> bool:
    """
    Расширяющая смена типа (безопасная на данных)?
    - integer → bigint, integer → numeric, smallint → integer/bigint
    - varchar(N) → varchar(M) при M > N или → text
    - timestamp → timestamp without time zone (тождество)
    """
    if not from_data_type:
        return True
    from_data_type = from_data_type.lower()
    to_upper = to_pg_type.upper()

    if (from_data_type, to_upper) in _SAFE_TYPE_WIDENING:
        return True
    # тождество
    if to_upper.startswith("VARCHAR") and from_data_type in ("character varying", "varchar"):
        # varchar(N) → varchar(M) where M >= N OR → varchar без ограничения
        m = None
        try:
            inside = to_upper.split("(")[1].rstrip(")") if "(" in to_upper else None
            m = int(inside) if inside else None
        except Exception:
            m = None
        if from_maxlen is None or m is None or m >= int(from_maxlen):
            return True
        return False
    if to_upper == "TEXT" and from_data_type in ("character varying", "varchar", "text"):
        return True
    # одинаковые
    same_map = {
        "integer": "INTEGER", "bigint": "BIGINT", "smallint": "SMALLINT",
        "numeric": "NUMERIC(18,4)", "text": "TEXT", "boolean": "BOOLEAN",
        "uuid": "UUID", "date": "DATE",
        "timestamp without time zone": "TIMESTAMP",
        "jsonb": "JSONB", "bytea": "BYTEA",
    }
    if same_map.get(from_data_type, "").startswith(to_upper.split("(")[0]):
        return True
    return False


def compute_sync_plan(target_id: int) -> dict:
    """
    План синхронизации одной таблицы.

    Принципы:
      • Новые колонки на НЕПУСТОЙ таблице добавляются как NULLABLE.
      • NOT NULL ставится только при CREATE TABLE (пустая таблица).
      • DROP COLUMN и сужение типа на таблице с данными — destructive.
      • На пустой таблице любые правки безопасны (терять нечего).

    Returns:
      {
        "target_id": ..., "schema": ..., "table": ...,
        "exists": bool, "rows_in_table": int,
        "actions": [
          {"kind": "create_table"|"add_column"|"alter_type"|"drop_column"|"recreate",
           "col": ..., "ddl": ..., "destructive": bool, "reason": ""}
        ],
        "safe_count": int, "destructive_count": int,
        "applied": False,
      }
    """
    target = get_target(target_id)
    if not target:
        return {"error": f"target {target_id} not found"}

    schema = target.get("target_schema", "public")
    table = target["target_table"]
    mappings = _collect_mappings_for_target(target)

    plan = {
        "target_id": target_id,
        "schema": schema, "table": table,
        "full_table_name": f"{schema}.{table}",
        "exists": _table_exists(schema, table),
        "rows_in_table": 0,
        "actions": [],
        "applied": False,
    }
    if not mappings:
        plan["error"] = "no mappings for target"
        plan["safe_count"] = 0; plan["destructive_count"] = 0
        return plan

    extras = _target_ddl_extras(target, mappings)

    if not plan["exists"]:
        # CREATE TABLE: одна action с полным DDL, не destructive.
        # Контрактные инварианты: PK(id), UNIQUE(upsert_keys), updated_at у dim,
        # FK-колонка fact→dim — без них upsert/post_load_sql движка не работают.
        cols = []
        if not extras["pk_natural"]:
            cols.append('    "id" BIGSERIAL PRIMARY KEY')
        for m in mappings:
            pg_type = _resolve_pg_type(m.get("target_type"))
            nullable = "NULL" if m.get("is_nullable", True) else "NOT NULL"
            default = f"DEFAULT {m['default_value']}" if m.get("default_value") else ""
            pk = " PRIMARY KEY" if (extras["pk_natural"] and m["target_column"] == "id") else ""
            cols.append(f'    "{m["target_column"]}" {pg_type}{pk} {nullable} {default}'.rstrip())
        if extras["fk"]:
            fk = extras["fk"]
            # Без REFERENCES: стандарт витрины — nullable BIGINT-ссылка, резолв в post_load;
            # физических FK нет ни у sales_id, ни у doc_sale_id, ни у dim-FK.
            cols.append(f'    "{fk["column"]}" {fk["pg_type"]} NULL')
        # Контрактные системные колонки (raw_refs, audit) — вместо legacy updated_at/etl_hash
        mapped_names = {m["target_column"] for m in mappings}
        for col_name, col_type, _reason in extras["system_cols"]:
            if col_name not in mapped_names:
                cols.append(f'    "{col_name}" {col_type}')
        if extras["unique_keys"]:
            uk = ", ".join(f'"{k}"' for k in extras["unique_keys"])
            cols.append(f"    UNIQUE ({uk})")
        ddl = f'CREATE TABLE "{schema}"."{table}" (\n' + ",\n".join(cols) + "\n)"
        plan["actions"].append({
            "kind": "create_table",
            "col": None,
            "ddl": ddl,
            "destructive": False,
            "reason": "table does not exist — creating from scratch",
        })
        plan["safe_count"] = 1; plan["destructive_count"] = 0
        return plan

    # Таблица существует
    rows = _table_row_count(schema, table)
    plan["rows_in_table"] = rows
    is_empty = (rows == 0)

    existing_cols = _existing_columns(schema, table)
    expected_cols = {m["target_column"] for m in mappings}
    # системные колонки всегда оставляем
    # ('sales_id' — legacy-литерал до шаблонной FK-колонки, не дропаем старые таблицы)
    # Системные колонки таблицы, которые НЕ нужно дропать при sync даже если
    # их нет в маппингах. Включает legacy updated_at и три новые ETL-аудит-поля
    # (см. dags/core/migrations/006_etl_audit_columns.sql + docs/sales_load_modes.md).
    SYSTEM_COLS = {
        "id", "etl_loaded_at", "etl_hash", "sales_id",
        "updated_at",  # legacy — оставляем чтобы старый код продолжал работать
        "retail_snapshot_at", "retail_updated_at", "etl_updated_at",
        # служебный флаг dim-слоя: строка создана stub-резолвом и ещё не
        # обогащена именем из 1С. Источника в мэппингах нет и быть не может —
        # без этой защиты Sync предложил бы дропнуть колонку.
        "is_stub",
        # исходные ссылки 1С одной JSONB-колонкой (стандарт ссылок): заполняет движок
        "raw_refs",
    }
    if extras["fk"]:
        SYSTEM_COLS.update(extras["fk"].get("candidates") or [extras["fk"]["column"]])
    SYSTEM_COLS.update(c for c, _t, _r in extras["system_cols"])

    # 1. Найти лишние колонки → DROP
    for col_name in existing_cols:
        if col_name in expected_cols or col_name in SYSTEM_COLS:
            continue
        if col_name.endswith("_id"):
            # FK-колонки dim-слоя (nomenklatura_id и т.п.): заполняются
            # post_load_sql и в мэппингах отсутствуют по определению —
            # Sync их не трогает. См. reports/pilot_dim_nomenklatura_plan_2026-07-27.md
            continue
        if col_name == "category" or re.fullmatch(r"subcategory\d+", col_name):
            # Плоская иерархия справочника (category, subcategory1..N): считается
            # loader'ом из parent_guid рекурсивно, в мэппингах не живёт по той же
            # причине, что *_id. Число уровней задаёт DDL под фактическую глубину
            # дерева — Sync их не создаёт и не дропает.
            continue
        destructive = not is_empty
        plan["actions"].append({
            "kind": "drop_column",
            "col": col_name,
            "ddl": f'ALTER TABLE "{schema}"."{table}" DROP COLUMN "{col_name}"',
            "destructive": destructive,
            "reason": "" if is_empty else f"data loss: {rows} rows lose this column",
        })

    # 2. Найти недостающие колонки → ADD
    for m in mappings:
        col_name = m["target_column"]
        if col_name in existing_cols:
            continue
        pg_type = _resolve_pg_type(m.get("target_type"))
        # На непустой таблице — всегда NULLABLE; NOT NULL только при CREATE.
        if is_empty:
            nullable_sql = "" if m.get("is_nullable", True) else "NOT NULL"
        else:
            nullable_sql = ""  # NULLABLE на непустой
        default = f"DEFAULT {m['default_value']}" if m.get("default_value") else ""
        plan["actions"].append({
            "kind": "add_column",
            "col": col_name,
            "ddl": f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{col_name}" {pg_type} {nullable_sql} {default}'.rstrip(),
            "destructive": False,
            "reason": "" if is_empty else "added as NULLABLE (table has rows)",
        })

    # 3. Найти изменения типа → ALTER TYPE
    for m in mappings:
        col_name = m["target_column"]
        if col_name not in existing_cols:
            continue
        if not m.get("target_type"):
            continue
        pg_type = _resolve_pg_type(m.get("target_type"))
        ex = existing_cols[col_name]
        # сравнить грубо: data_type vs target_type
        current_type = ex.get("data_type", "")
        if _is_type_widening(current_type, pg_type, ex.get("character_maximum_length")):
            # либо тождество, либо безопасное расширение — пропускаем (не двигаем тип)
            continue
        # сужение или несовместимая смена
        destructive = not is_empty
        plan["actions"].append({
            "kind": "alter_type",
            "col": col_name,
            "ddl": f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN "{col_name}" TYPE {pg_type} USING "{col_name}"::{pg_type.split("(")[0]}',
            "destructive": destructive,
            "reason": "" if is_empty else f"narrowing type {current_type} → {pg_type} on {rows} rows",
        })

    # 4. Контрактные колонки: FK-колонка у fact (если ни один кандидат имени не
    #    существует) и системные колонки новой модели — добавляются NULLABLE, safe.
    #    legacy updated_at не добавляется (add_updated_at=False), существующая — под защитой.
    contract_cols = []
    if extras["fk"]:
        fk = extras["fk"]
        if not any(c in existing_cols for c in (fk.get("candidates") or [fk["column"]])):
            contract_cols.append((fk["column"], f'{fk["pg_type"]} NULL',
                                  f'FK-колонка fact → {fk["ref_schema"]}.{fk["ref_table"]}'))
    for col_name, col_type, reason in extras["system_cols"]:
        if col_name not in existing_cols and col_name not in expected_cols:
            contract_cols.append((col_name, col_type, reason))
    for col_name, pg_type, reason in contract_cols:
        plan["actions"].append({
            "kind": "add_column",
            "col": col_name,
            "ddl": f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{col_name}" {pg_type}',
            "destructive": False,
            "reason": reason,
        })

    # 5. Контрактные констрейнты: PK(id) и UNIQUE(upsert_keys).
    #    На непустой таблице — предварительная проверка дублей: если дубли есть,
    #    констрейнт невозможен без recreate → destructive (показывается в confirm).
    constraints = _existing_constraints(schema, table)
    has_pk = any(c["type"] == "p" for c in constraints)

    if not has_pk:
        if extras["pk_natural"] or "id" in existing_cols:
            # натуральный id (из маппингов либо уже есть в таблице) → PK на нём;
            # дубли проверяем только если колонка физически существует и есть строки
            dups = (_duplicate_key_groups(schema, table, ["id"])
                    if (not is_empty and "id" in existing_cols) else 0)
            plan["actions"].append({
                "kind": "add_pk",
                "col": "id",
                "ddl": f'ALTER TABLE "{schema}"."{table}" ADD PRIMARY KEY ("id")',
                "destructive": dups > 0,
                "reason": (f"{dups} групп дублей по id — PK требует recreate"
                           if dups else "контракт: PK по id"),
            })
        else:
            # нет натурального id — добавляем синтетический BIGSERIAL PK (safe)
            plan["actions"].append({
                "kind": "add_pk",
                "col": "id",
                "ddl": f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "id" BIGSERIAL PRIMARY KEY',
                "destructive": False,
                "reason": "контракт: суррогатный PK id",
            })

    if extras["unique_keys"]:
        uk = extras["unique_keys"]
        covered = any(c["type"] in ("p", "u") and c["cols"] == set(uk) for c in constraints)
        if not covered:
            dups = _duplicate_key_groups(schema, table, uk) if not is_empty else 0
            uk_sql = ", ".join(f'"{k}"' for k in uk)
            plan["actions"].append({
                "kind": "add_unique",
                "col": ",".join(uk),
                "ddl": f'ALTER TABLE "{schema}"."{table}" ADD UNIQUE ({uk_sql})',
                "destructive": dups > 0,
                "reason": (f"{dups} групп дублей по ({', '.join(uk)}) — UNIQUE требует recreate"
                           if dups else "контракт: upsert по ON CONFLICT требует UNIQUE"),
            })

    plan["safe_count"] = sum(1 for a in plan["actions"] if not a["destructive"])
    plan["destructive_count"] = sum(1 for a in plan["actions"] if a["destructive"])
    return plan


def _drop_dependents_cascade(schema: str, table: str) -> List[str]:
    """
    Найти таблицы которые ссылаются FK на {schema}.{table} и вернуть их
    в порядке зависимостей. Используется для recreate dim → fact:
    дропать fact (с CASCADE), потом dim.
    """
    deps = query("""
        SELECT tc.table_schema AS s, tc.table_name AS t
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
           AND ccu.constraint_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND ccu.table_schema = %s
          AND ccu.table_name = %s
    """, [schema, table])
    return [f'{r["s"]}.{r["t"]}' for r in deps]


def apply_sync_plan(plan: dict, confirm: bool = False) -> dict:
    """
    Применяет план. Возвращает копию plan с applied=True/False и executed_actions.

    Правила:
      - все safe actions применяются всегда
      - destructive actions — только при confirm=True
      - если рекомендуется recreate (несовместимый ADD NOT NULL или сужение типа
        + есть строки) — делается DROP TABLE CASCADE + CREATE TABLE заново
        в порядке: сначала dependents (через CASCADE), потом сама таблица
      - после recreate данные надо загружать заново через full_period
    """
    if "error" in plan:
        return {**plan, "applied": False, "executed": [], "errors": [plan["error"]]}

    safe = [a for a in plan["actions"] if not a["destructive"]]
    destructive = [a for a in plan["actions"] if a["destructive"]]

    executed: List[dict] = []
    errors: List[str] = []

    # Если есть destructive, но confirm не дали — применяем только safe и возвращаем
    if destructive and not confirm:
        for a in safe:
            try:
                execute(a["ddl"])
                executed.append(a)
            except Exception as e:
                errors.append(f'{a["kind"]} {a["col"]}: {e}')
        return {
            **plan,
            "applied": False,
            "requires_confirm": True,
            "executed": executed,
            "errors": errors,
        }

    # Если destructive подтверждены — пересоздаём целиком (проще и консистентнее)
    if destructive and confirm:
        schema = plan["schema"]; table = plan["table"]
        # 1. Найти таблицы которые ссылаются FK на нас (dependents — fact на dim)
        dependents = _drop_dependents_cascade(schema, table)
        # 2. DROP CASCADE — сама таблица (это снесёт и FK от dependents)
        try:
            execute(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')
            executed.append({
                "kind": "drop_table",
                "col": None,
                "ddl": f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE',
                "destructive": True,
                "reason": "recreate triggered by destructive change",
            })
        except Exception as e:
            errors.append(f"DROP TABLE: {e}")
        # 3. CREATE TABLE из новых маппингов
        new_plan = compute_sync_plan(plan["target_id"])
        create_action = next((a for a in new_plan["actions"] if a["kind"] == "create_table"), None)
        if create_action:
            try:
                execute(create_action["ddl"])
                executed.append(create_action)
            except Exception as e:
                errors.append(f"CREATE TABLE: {e}")
        # 4. dependents — их recreate должен инициироваться отдельно владельцем
        return {
            **plan,
            "applied": True,
            "executed": executed,
            "errors": errors,
            "recreated": True,
            "dependents_dropped": dependents,
            "note": "после recreate надо запустить full_period — данные потеряны",
        }

    # Нет destructive — просто применяем safe
    for a in safe:
        try:
            execute(a["ddl"])
            executed.append(a)
        except Exception as e:
            errors.append(f'{a["kind"]} {a["col"]}: {e}')
    return {**plan, "applied": True, "executed": executed, "errors": errors}


def sync_target_table(target_id: int):
    """
    Backward-compat обёртка: применяет план без подтверждения destructive.
    Возвращает план применения (с requires_confirm если нужен confirm).
    """
    plan = compute_sync_plan(target_id)
    return apply_sync_plan(plan, confirm=False)


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


def add_include_column(target_id: int, column: str):
    """
    Дописывает колонку в target.include_columns (идемпотентно).

    include_columns — то, по чему Sync собирает DDL (_collect_mappings_for_target).
    Если таргет им пользуется, а колонки там нет, новый мэппинг в таблицу не
    попадёт. У таргетов без include_columns список не заводим — там действует
    старая логика «все мэппинги источника».
    """
    target = get_target(target_id)
    if not target:
        return
    include = target.get("include_columns")
    if not include:          # NULL или [] — режим «все колонки источника»
        return
    if column in include:
        return
    execute(
        f"UPDATE {SCHEMA}.register_targets "
        f"SET include_columns = array_append(include_columns, %s) WHERE id=%s",
        [column, target_id],
    )


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
