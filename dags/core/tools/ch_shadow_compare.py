"""
Сравнение старого пути с прямым: две цели ClickHouse, один и тот же набор колонок.

    PYTHONPATH=dags python -m core.tools.ch_shadow_compare --pair fact_sales_positions:fact_sales_positions_direct
    ... --partition 202606        одна партиция
    ... --all-partitions          все партиции, где есть данные в shadow

Отпечаток сверяется по колонкам ИСХОДНОЙ конфигурации, поэтому колонки, которых в
старой цели нет (id шапки), в сравнение не попадают и проверяются отдельно.
Служебные метки времени (etl_updated_at, retail_updated_at) не сравниваются: это время
загрузки, оно обязано отличаться.

Если отпечатки разошлись — разбор до строк: чего нет в одной из целей и какие
колонки различаются у совпавших ключей.
"""

import argparse
import sys

sys.path.insert(0, __file__.rsplit("/core/", 1)[0])

from core.clickhouse import reconcile as rec            # noqa: E402
from core.clickhouse.config import load_spec            # noqa: E402
from core.clickhouse.target import ClickHouse           # noqa: E402

AUDIT = {"etl_updated_at", "retail_updated_at", "etl_loaded_at"}


def compare_partition(ch, old, new, p, detail=True):
    w = f" WHERE {old.partition_expr} = {int(p)}"
    a = ch.row(rec.fingerprint_sql(old, "clickhouse") + f" FROM {old.fqn}{w}")
    b = ch.row(rec.fingerprint_sql(old, "clickhouse") + f" FROM {new.fqn}{w}")
    diff = rec.compare(old, a, b)
    out = {"partition": p, "rows_old": int(a[0]) if a else 0, "rows_new": int(b[0]) if b else 0,
           "diff": diff, "rows": []}
    if diff and detail:
        bk = old.business_key
        on = " AND ".join(f"o.{k} = n.{k}" for k in bk)
        keys = ", ".join(f"toString(o.{k})" for k in bk)
        miss = ch.query(f"SELECT {keys} FROM {old.fqn} o LEFT ANTI JOIN {new.fqn} n ON {on} "
                        f"WHERE {old.partition_expr.replace('period', 'o.period')} = {int(p)} LIMIT 5")
        extra = ch.query(f"SELECT {keys.replace('o.', 'n.')} FROM {new.fqn} n LEFT ANTI JOIN {old.fqn} o ON {on} "
                         f"WHERE {old.partition_expr.replace('period', 'n.period')} = {int(p)} LIMIT 5")
        out["missing_in_new"] = [x for x in miss.split("\n") if x]
        out["extra_in_new"] = [x for x in extra.split("\n") if x]
        for c in old.target_columns:
            if c in bk or c in AUDIT:
                continue
            n = ch.scalar(f"SELECT count() FROM {old.fqn} o INNER JOIN {new.fqn} n ON {on} "
                          f"WHERE {old.partition_expr.replace('period', 'o.period')} = {int(p)} "
                          f"AND o.{c} != n.{c}")
            if int(n or 0):
                ex = ch.query(f"SELECT {keys}, toString(o.{c}), toString(n.{c}) FROM {old.fqn} o "
                              f"INNER JOIN {new.fqn} n ON {on} WHERE "
                              f"{old.partition_expr.replace('period', 'o.period')} = {int(p)} "
                              f"AND o.{c} != n.{c} LIMIT 3")
                out["rows"].append((c, int(n), [x for x in ex.split("\n") if x]))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Старый путь против прямого в ClickHouse")
    ap.add_argument("--pair", action="append", required=True, help="old_code:new_code")
    ap.add_argument("--partition", action="append")
    ap.add_argument("--all-partitions", action="store_true")
    ap.add_argument("--ch-conn", default="clickhouse_etl")
    args = ap.parse_args()
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id="etl_prod")
    ch = ClickHouse(args.ch_conn)
    bad = 0
    for pair in args.pair:
        oc, nc = pair.split(":")
        old, new = load_spec(pg, oc), load_spec(pg, nc)
        if args.all_partitions:
            parts = [x for x in ch.query(f"SELECT DISTINCT {new.partition_expr} AS p FROM {new.fqn} ORDER BY p")
                     .split("\n") if x]
        else:
            parts = args.partition or []
        print(f"\n=== {oc}  против  {nc} ===")
        for p in parts:
            r = compare_partition(ch, old, new, p)
            ok = not r["diff"]
            bad += not ok
            print(f"   {p}  строк старый {r['rows_old']:>8,}  прямой {r['rows_new']:>8,}   "
                  f"{'Δ=0' if ok else 'РАСХОЖДЕНИЕ'}")
            for n, x, y in r["diff"]:
                print(f"      {n}: старый {x}  прямой {y}")
            if r.get("missing_in_new"):
                print(f"      нет в прямом (пример): {r['missing_in_new'][:3]}")
            if r.get("extra_in_new"):
                print(f"      лишние в прямом (пример): {r['extra_in_new'][:3]}")
            for c, n, ex in r["rows"]:
                print(f"      колонка {c}: различаются {n} строк; пример {ex[:2]}")
    print("\nИТОГ:", "PASS — прямой путь совпал со старым" if not bad else f"FAIL — партиций с расхождением {bad}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
