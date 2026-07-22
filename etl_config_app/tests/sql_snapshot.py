"""
SQL-снапшот регистра sales — регресс-гейт фазы 0.2.

Baseline снимается ДО патча движка (--save), после каждой правки запускается
сравнение (без аргументов). Любое расхождение SQL sales = правка сломала
текущее поведение → стоп/откат.

Требует, чтобы схема etl_test_ref была заполнена (golden_sales_test.py
запускается первым и пересоздаёт её).

Запуск:
    python3 etl_config_app/tests/sql_snapshot.py --save   # baseline
    python3 etl_config_app/tests/sql_snapshot.py          # сравнение
"""

import argparse
import difflib
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_APP_DIR = os.path.dirname(_HERE)
for p in (_APP_DIR, _HERE):
    if p not in sys.path:
        sys.path.insert(0, p)

import dao  # noqa: E402
import mirror_loader  # noqa: E402
from golden_sales_test import build_sqls  # noqa: E402

SNAPSHOT_FILE = os.path.join(_HERE, "snapshots", "sales_sql.json")


def generate(schema: str = "etl_test_ref") -> dict:
    conn = dao.get_conn()
    try:
        cfg = mirror_loader.load_register(conn, "sales", schema=schema)
        return build_sqls(cfg)
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--save", action="store_true", help="сохранить baseline")
    ap.add_argument("--schema", default="etl_test_ref")
    args = ap.parse_args()

    current = generate(args.schema)

    if args.save:
        os.makedirs(os.path.dirname(SNAPSHOT_FILE), exist_ok=True)
        with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
            json.dump(current, f, ensure_ascii=False, indent=1)
        print(f"Baseline сохранён: {SNAPSHOT_FILE}")
        return

    with open(SNAPSHOT_FILE, encoding="utf-8") as f:
        baseline = json.load(f)

    if current == baseline:
        print("SQL SNAPSHOT OK ✅  (sales SQL идентичен baseline)")
        return

    print("SQL SNAPSHOT FAILED ❌ — расхождения с baseline:")
    for tbl in sorted(set(baseline) | set(current)):
        for mode in sorted(set(baseline.get(tbl, {})) | set(current.get(tbl, {}))):
            old = (baseline.get(tbl) or {}).get(mode) or ""
            new = (current.get(tbl) or {}).get(mode) or ""
            if old != new:
                print(f"\n--- {tbl}/{mode} ---")
                for line in difflib.unified_diff(old.splitlines(), new.splitlines(),
                                                 "baseline", "current", lineterm=""):
                    print(line)
    sys.exit(1)


if __name__ == "__main__":
    main()
