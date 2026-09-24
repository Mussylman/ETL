"""
Синхронизация источников в ClickHouse по конфигурации из etl_meta.ch_sync.

Ни одной таблицы в коде: что грузить, откуда, как партиционировать и как сверять —
всё в PostgreSQL control plane. Новый объект подключается записью конфигурации.

Использование:
    PYTHONPATH=dags python -m core.tools.ch_sync --code fact_sales_positions --plan
    PYTHONPATH=dags python -m core.tools.ch_sync --code fact_sales_positions --apply
    ... --all                все активные конфигурации по priority
    ... --sweep              принудительно сверить отпечатки всей истории
    ... --create-table       создать целевую таблицу и staging из конфигурации
    ... --partition 202608   только одна партиция
"""

import argparse
import sys
from datetime import datetime

sys.path.insert(0, __file__.rsplit("/core/", 1)[0])

from core.clickhouse import engine as eng          # noqa: E402
from core.clickhouse.config import load_active_specs, load_spec   # noqa: E402
from core.clickhouse.source import open_source     # noqa: E402
from core.clickhouse.target import ClickHouse      # noqa: E402


def ensure_tables(ch: ClickHouse, spec) -> None:
    """Целевая таблица и staging создаются из конфигурации, а не руками."""
    for t in (spec.fqn, spec.stage_fqn):
        if not ch.table_exists(t):
            ch.execute(spec.ddl(t))
            print(f"   создана {t}")
        else:
            print(f"   уже есть {t}")


def run_spec(pg, ch, spec, args) -> int:
    print(f"\n=== {spec.code} — {spec.description or spec.target_table} ===")
    print(f"   источник: {spec.source_conn_id} ({spec.source_type}) → {spec.fqn}")
    if args.create_table:
        ensure_tables(ch, spec)
    if not ch.table_exists(spec.fqn):
        print(f"   ✗ {spec.fqn} не существует — запустите с --create-table")
        return 1

    src = open_source(spec)
    if args.partition:
        keys, reason = [args.partition], {args.partition: "указана явно"}
        orphan = []
    else:
        plan = eng.affected_partitions(pg, ch, spec, src, force_sweep=args.sweep)
        keys, reason, orphan = plan["keys"], plan["reason"], plan.get("orphan", [])
        print(f"   партиций в источнике: {plan.get('src_total', '—')}, "
              f"к загрузке: {len(keys)}{', sweep выполнен' if plan.get('swept') else ''}")
    if orphan:
        print(f"   ⚠ есть в ClickHouse, нет в источнике: {orphan} — молча не удаляю, "
              f"это отдельное решение")
    if not keys:
        print("   изменений нет")
        return 0

    print(f"\n   {'партиция':>10} {'причина':<38} {'строк ист.':>12} {'строк цель':>12}  статус")
    print("   " + "-" * 92)
    failed = 0
    for k in keys:
        r = eng.sync_partition(pg, ch, spec, src, k, apply=args.apply)
        print(f"   {k:>10} {reason.get(k,''):<38} {r.get('rows_source',0):>12,} "
              f"{r.get('rows_target',0):>12,}  {r['status']}"
              + (f" — {r['note']}" if r.get("note") else ""))
        if r.get("t_total"):
            print(f"   {'':>10} поток {r['t_stream']}s | сверка {r['t_verify']}s | "
                  f"{r.get('method','')} {r['t_publish']}s | всего {r['t_total']}s")
        if r["status"] == "failed":
            failed += 1
            print("   ОСТАНОВ: партиция не опубликована, цель не изменена, "
                  "следующие партиции не грузятся")
            break
    return 1 if failed else 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Синхронизация в ClickHouse по конфигурации etl_meta")
    ap.add_argument("--code", help="code из etl_meta.ch_sync")
    ap.add_argument("--group", help="группа конфигураций (sync_group) — тот же runner, что вызывает DAG")
    ap.add_argument("--mode", default="patch", choices=["patch", "rebuild"],
                    help="для регистров 1С: патч документов или пересборка --partition")
    ap.add_argument("--include-inactive", action="store_true", help="включить неактивные (shadow)")
    ap.add_argument("--all", action="store_true", help="все активные конфигурации")
    ap.add_argument("--partition", help="только одна партиция, например 202608")
    ap.add_argument("--sweep", action="store_true", help="принудительная сверка отпечатков всей истории")
    ap.add_argument("--create-table", action="store_true", help="создать цель и staging из конфигурации")
    ap.add_argument("--ch-conn", default="clickhouse_etl", help="Airflow conn_id ClickHouse")
    ap.add_argument("--config-conn", default="etl_prod", help="Airflow conn_id control plane")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if args.group:
        if not args.apply:
            ap.error("--group выполняет загрузку: нужен --apply")
        from core.clickhouse.runner import run_group
        rep = run_group(args.group, mode=args.mode,
                        partitions=[args.partition] if args.partition else None,
                        include_inactive=args.include_inactive, ch_conn_id=args.ch_conn,
                        config_conn_id=args.config_conn)
        print(f"группа {args.group}: объектов {rep['objects']}, сбоев {len(rep['failed'])}")
        for r in rep.get("results", []):
            print("   ", {k: v for k, v in r.items() if k != "published"})
        return 0
    if not args.code and not args.all:
        ap.error("нужен --code, --group либо --all")

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=args.config_conn)
    ch = ClickHouse(args.ch_conn)

    specs = load_active_specs(pg) if args.all else [load_spec(pg, args.code)]
    print(f"конфигураций: {len(specs)} | режим: {'apply' if args.apply else 'plan'} "
          f"| старт {datetime.now():%H:%M:%S}")
    rc = 0
    for spec in specs:
        rc |= run_spec(pg, ch, spec, args)
        if rc:
            print("\nОСТАНОВ: дальнейшие конфигурации не обрабатываются")
            break
    return rc


if __name__ == "__main__":
    sys.exit(main())
