"""
GOLDEN-ТЕСТ ЭТАПА 0: round-trip эталонного регистра sales через RegisterSpec.

Эталон — dags/core/migrations/_pending_sales_rebuild.sql (контракт платформы).
Живая etl_meta на момент создания теста ПУСТА, поэтому эталон применяется
в изолированную схему ETL-теста (etl_test_ref), а не в боевую.

Поток:
  1. Пересоздать схемы etl_test_ref и etl_test (структура = LIKE etl_meta.*).
  2. Применить эталонный SQL в etl_test_ref («живая» сторона).
  3. spec = read_spec('sales', etl_test_ref)         — конфиг → RegisterSpec
  4. validate_spec(spec) == []                       — валидатор контракта
  5. write_spec(spec, etl_test)                      — одна транзакция
  6. mirror_load(etl_test_ref) vs mirror_load(etl_test):
       RegisterConfig поле-в-поле (id-поля нормализуются в code-ссылки)
  7. QueryBuilder SQL (full_period + incremental) — строки идентичны.
  8. spec round-trip: read_spec(etl_test) == исходному spec.

Запуск:  python3 etl_config_app/tests/golden_sales_test.py [--schema-ref etl_meta]
Код выхода 0 = зелёный. Когда sales появится в боевой etl_meta,
тот же тест запускается с --schema-ref etl_meta (шаги 1-2 пропускаются).
"""

import argparse
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_APP_DIR = os.path.dirname(_HERE)
_ROOT = os.path.dirname(_APP_DIR)
for p in (_APP_DIR, _HERE):
    if p not in sys.path:
        sys.path.insert(0, p)

import dao  # noqa: E402
from register_spec import validate_spec  # noqa: E402
from spec_reader import read_spec  # noqa: E402
from spec_writer import write_spec  # noqa: E402
import mirror_loader  # noqa: E402  (тянет dags/core — Airflow)

from core.builder.query_builder import QueryBuilder  # noqa: E402

PENDING_SQL = os.path.join(_ROOT, "dags", "core", "migrations", "_pending_sales_rebuild.sql")

META_TABLES = [
    "registers", "register_sources", "column_mappings",
    "source_unions", "source_union_members", "register_targets", "load_history",
]

SAMPLE_UIDS = [
    "a1b2c3d4-e5f6-7788-99aa-bbccddeeff00",
    "00112233-4455-6677-8899-aabbccddeeff",
]

_failures = []


def check(name: str, ok: bool, detail: str = ""):
    mark = "✅" if ok else "❌"
    print(f"{mark} {name}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        _failures.append(name)


# ----------------------------------------------------------------------
# Подготовка схем
# ----------------------------------------------------------------------

def rebuild_schema(conn, schema: str):
    """DROP + CREATE схемы с таблицами-копиями структуры etl_meta."""
    with conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        cur.execute(f'CREATE SCHEMA "{schema}"')
        for t in META_TABLES:
            cur.execute(f'CREATE TABLE "{schema}".{t} (LIKE etl_meta.{t} INCLUDING DEFAULTS)')
    conn.commit()


def apply_etalon(conn, schema: str):
    """Применяет эталонный _pending_sales_rebuild.sql в указанную схему."""
    with open(PENDING_SQL, encoding="utf-8") as f:
        sql = f.read()
    sql = sql.replace("etl_meta.", f"{schema}.")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


# ----------------------------------------------------------------------
# Нормализация RegisterConfig: id-поля → code-ссылки
# ----------------------------------------------------------------------

def normalize_config(cfg) -> dict:
    src_code = {s.id: s.source_code for s in cfg.sources}
    un_code = {u.id: u.union_code for u in cfg.unions}

    def col(c):
        return {
            "source_column": c.source_column, "target_column": c.target_column,
            "is_expression": c.is_expression, "target_type": c.target_type,
            "transform_type": c.transform_type, "transform_params": c.transform_params,
            "default_value": c.default_value, "is_nullable": c.is_nullable,
        }

    return {
        "register": {
            "code": cfg.code, "name": cfg.name, "description": cfg.description,
            "default_mode": cfg.default_mode,
            "retail_table": cfg.retail_table, "retail_uid_column": cfg.retail_uid_column,
        },
        "sources": [{
            "source_code": s.source_code, "source_type": s.source_type,
            "mssql_schema": s.mssql_schema, "mssql_table": s.mssql_table,
            "parent": src_code.get(s.parent_source_id),
            "join_type": s.join_type, "join_key_source": s.join_key_source,
            "join_key_parent": s.join_key_parent,
            "where_clause": s.where_clause, "priority": s.priority,
            "period_column": s.period_column,
            "columns": [col(c) for c in s.columns],
        } for s in sorted(cfg.sources, key=lambda x: x.source_code)],
        "unions": [{
            "union_code": u.union_code, "description": u.description,
            "output_columns": u.output_columns,
            "members": [{
                "source": src_code.get(m.source_id),
                "priority": m.priority, "where_clause": m.where_clause,
            } for m in u.members],
        } for u in sorted(cfg.unions, key=lambda x: x.union_code)],
        "targets": [{
            "target_schema": t.target_schema, "target_table": t.target_table,
            "source": src_code.get(t.source_id), "union": un_code.get(t.union_id),
            "load_mode": t.load_mode, "upsert_keys": t.upsert_keys,
            "pre_load_sql": t.pre_load_sql, "post_load_sql": t.post_load_sql,
            "include_columns": t.include_columns, "priority": t.priority,
            "target_role": t.target_role, "is_active": t.is_active,
        } for t in sorted(cfg.targets, key=lambda x: (x.priority, x.target_table))],
    }


def deep_diff(a, b, path="$"):
    """Список отличий двух нормализованных структур."""
    diffs = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                diffs.append(f"{path}.{k}: только справа = {b[k]!r}")
            elif k not in b:
                diffs.append(f"{path}.{k}: только слева = {a[k]!r}")
            else:
                diffs += deep_diff(a[k], b[k], f"{path}.{k}")
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            diffs.append(f"{path}: длина {len(a)} != {len(b)}")
        for i, (x, y) in enumerate(zip(a, b)):
            diffs += deep_diff(x, y, f"{path}[{i}]")
    elif a != b:
        diffs.append(f"{path}: {a!r} != {b!r}")
    return diffs


# ----------------------------------------------------------------------
# SQL-снапшоты (key_column выводится как в ETLEngine._build_sql_for_target)
# ----------------------------------------------------------------------

def derive_key_column(cfg, target):
    primary = None
    if target.source_config:
        primary = target.source_config
    elif target.union_config and target.union_config.members:
        primary = next((m.source for m in target.union_config.members if m.source), None)
    elif cfg.sources:
        primary = cfg.sources[0]
    if not primary:
        return None
    if primary.source_type == "standalone":
        return "_RecorderRRef"
    if primary.source_type == "header":
        return "_IDRRef"
    if primary.source_type == "detail":
        parent = primary.parent_source
        return "_IDRRef" if parent and parent.source_type == "header" else "_RecorderRRef"
    return None


def build_sqls(cfg) -> dict:
    """{target_table: {'full_period': sql, 'incremental': sql}}"""
    qb = QueryBuilder(database="UPP_JAN")
    out = {}
    for t in sorted([t for t in cfg.targets if t.is_active], key=lambda x: x.priority):
        variants = {}
        for label, kwargs in [
            ("full_period", dict(period_start="4025-10-01", period_end="4025-11-01")),
            ("incremental", dict(key_column=derive_key_column(cfg, t), key_values=SAMPLE_UIDS)),
        ]:
            if t.union_config:
                sql = qb.build_union_query(union=t.union_config, register=cfg, **kwargs)
            elif t.source_config:
                sql = qb.build_source_query(source=t.source_config, **kwargs)
            else:
                sql = None
            variants[label] = sql
        out[t.target_table] = variants
    return out


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Golden-тест round-trip регистра sales")
    ap.add_argument("--schema-ref", default="etl_test_ref",
                    help="схема с эталоном (etl_test_ref = применить pending SQL; "
                         "etl_meta = живой конфиг, шаги подготовки пропускаются)")
    ap.add_argument("--schema-out", default="etl_test", help="схема для round-trip записи")
    args = ap.parse_args()

    conn = dao.get_conn()
    try:
        # 1-2. Подготовка эталона (только для изолированной схемы)
        if args.schema_ref != "etl_meta":
            rebuild_schema(conn, args.schema_ref)
            apply_etalon(conn, args.schema_ref)
            print(f"Эталон _pending_sales_rebuild.sql применён в {args.schema_ref}")
        rebuild_schema(conn, args.schema_out)

        # 3. Конфиг → RegisterSpec
        spec = read_spec("sales", schema=args.schema_ref, conn=conn)
        check("read_spec: эталон прочитан в RegisterSpec",
              spec.code == "sales" and len(spec.sources) == 1 and len(spec.targets) == 2)

        # 4. Валидатор контракта
        errors = validate_spec(spec)
        check("validate_spec: эталон проходит валидатор", not errors, "; ".join(errors))

        # 5. Запись одной транзакцией
        result = write_spec(spec, schema=args.schema_out, conn=conn, replace=True)
        check("write_spec: записано в " + args.schema_out,
              result["mapping_count"] == 13 and len(result["target_ids"]) == 2,
              json.dumps(result, ensure_ascii=False, default=str))

        # 6. RegisterConfig поле-в-поле (движковые dataclass'ы, зеркало ConfigLoader)
        cfg_ref = mirror_loader.load_register(conn, "sales", schema=args.schema_ref)
        cfg_new = mirror_loader.load_register(conn, "sales", schema=args.schema_out)
        diffs = deep_diff(normalize_config(cfg_ref), normalize_config(cfg_new))
        check("RegisterConfig: поле-в-поле идентичен эталону", not diffs)
        for d in diffs[:20]:
            print("    diff:", d)

        # 7. SQL-снапшоты QueryBuilder
        sql_ref, sql_new = build_sqls(cfg_ref), build_sqls(cfg_new)
        check("QueryBuilder SQL: идентичен эталону (full_period + incremental)",
              sql_ref == sql_new)
        if sql_ref != sql_new:
            for tbl in sql_ref:
                for mode in sql_ref[tbl]:
                    if sql_ref[tbl].get(mode) != (sql_new.get(tbl) or {}).get(mode):
                        print(f"    SQL diff: {tbl}/{mode}")

        # 8. Spec round-trip (etl_test → RegisterSpec == исходному)
        spec2 = read_spec("sales", schema=args.schema_out, conn=conn)
        check("RegisterSpec: round-trip без потерь",
              spec.model_dump() == spec2.model_dump())

        # Контрольный отпечаток SQL — для глаз
        print("\n--- SQL dim-target (full_period), первые 400 символов ---")
        first = next(iter(sql_ref.values()))["full_period"]
        print(first[:400])

    finally:
        conn.close()

    print()
    if _failures:
        print(f"FAILED: {len(_failures)} провалов: {_failures}")
        sys.exit(1)
    print("GOLDEN TEST PASSED ✅  (sales round-trip без потерь, SQL идентичен)")


if __name__ == "__main__":
    main()
