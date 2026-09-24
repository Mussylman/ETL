"""
Итоговая сверка и отчёт по аналитическому слою ClickHouse.

Идёт по активным конфигурациям etl_meta.ch_sync: для каждой считает отпечаток
источника и цели целиком и сравнивает. Список объектов не зашит — он в конфиге.

    PYTHONPATH=dags python -m core.tools.ch_report
    ... --ch-admin <admin.xml>   добавить размеры на диске (нужен доступ к system.parts)
"""

import argparse
import subprocess
import sys

sys.path.insert(0, __file__.rsplit("/core/", 1)[0])

from core.clickhouse import reconcile as rec              # noqa: E402
from core.clickhouse.config import load_active_specs      # noqa: E402
from core.clickhouse.source import open_source            # noqa: E402
from core.clickhouse.target import ClickHouse             # noqa: E402


def sizes(admin_cfg):
    """Размеры и партиции из system.parts. etl_writer туда не пускают — нужен ch_admin."""
    if not admin_cfg:
        return {}
    r = subprocess.run(["clickhouse-client", "--config-file", admin_cfg, "-q",
        "SELECT table, uniqExact(partition), count(), sum(rows), sum(bytes_on_disk), "
        "round(sum(data_uncompressed_bytes)/sum(bytes_on_disk),2) "
        "FROM system.parts WHERE database='analytics_poc' AND active GROUP BY table FORMAT TSV"],
        capture_output=True, text=True)
    out = {}
    for line in r.stdout.strip().split("\n"):
        if not line.strip():
            continue
        f = line.split("\t")
        out[f[0]] = {"partitions": int(f[1]), "parts": int(f[2]), "rows": int(f[3]),
                     "bytes": int(f[4]), "ratio": f[5]}
    return out


def human(n):
    for u in ("B", "KiB", "MiB", "GiB"):
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TiB"


def main() -> int:
    ap = argparse.ArgumentParser(description="Итоговая сверка аналитического слоя ClickHouse")
    ap.add_argument("--config-conn", default="etl_prod")
    ap.add_argument("--ch-conn", default="clickhouse_etl")
    ap.add_argument("--ch-admin", help="config-file ch_admin для размеров")
    args = ap.parse_args()

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=args.config_conn)
    ch = ClickHouse(args.ch_conn)
    sz = sizes(args.ch_admin)

    print(f"{'объект':<24} {'строк источник':>15} {'строк ClickHouse':>17} {'парт.':>6} "
          f"{'сжато':>10} {'сверка':<10} {'инкремент':<14} статус")
    print("-" * 122)
    all_ok = True
    for spec in load_active_specs(pg):
        if not ch.table_exists(spec.fqn):
            print(f"{spec.code:<24} {'—':>15} {'—':>17} {'—':>6} {'—':>10} "
                  f"{'нет таблицы':<10} {'—':<14} FAIL")
            all_ok = False
            continue
        src = open_source(spec)
        a = src.conn.get_first(rec.fingerprint_sql(spec, spec.source_type)
                               + f" FROM {src.projected()}")
        b = ch.row(rec.fingerprint_sql(spec, "clickhouse") + f" FROM {spec.fqn}")
        diff = rec.compare(spec, a, b)
        # по бакетам: на больших фактах GROUP BY целиком не влезает в лимит памяти
        B = 16
        dup = 0
        if spec.business_key:
            for bucket in range(B):
                dup += int(ch.scalar(
                    rec.duplicates_sql(spec, spec.fqn, bucket=bucket, buckets=B)) or 0)
        ok = not diff and not dup
        all_ok &= ok
        m = sz.get(spec.target_table, {})
        inc = ("партиция месяца" if spec.is_partitioned else "полная замена")
        print(f"{spec.code:<24} {int(a[0]):>15,} {int(b[0]):>17,} "
              f"{m.get('partitions','—'):>6} {human(m['bytes']) if m else '—':>10} "
              f"{'Δ=0' if not diff else f'Δ≠0 ({len(diff)})':<10} {inc:<14} "
              f"{'PASS' if ok else 'FAIL'}")
        if diff:
            for n, x, y in diff[:4]:
                print(f"{'':<24} расхождение {n}: источник {x} против ClickHouse {y}")
        if dup:
            print(f"{'':<24} дублей business key: {dup}")
    print("-" * 122)
    if sz:
        tot = sum(v["bytes"] for k, v in sz.items() if not k.endswith("_stage"))
        rows = sum(v["rows"] for k, v in sz.items() if not k.endswith("_stage"))
        print(f"итого в analytics_poc: строк {rows:,}, на диске {human(tot)}")
    print("\nCLICKHOUSE CORE ANALYTICS: " + ("PASS" if all_ok else "FAIL"))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
