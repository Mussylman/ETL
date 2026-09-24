"""
Создание целевых таблиц ClickHouse из конфигурации etl_meta.ch_sync.

Отдельно от загрузки сознательно: CREATE TABLE и GRANT выполняет ch_admin,
а он в обычном ETL не участвует и в Airflow не заводится. Загрузчик etl_writer
создавать таблицы не может и не должен.

    PYTHONPATH=dags python -m core.tools.ch_ddl --code dim_sklad --ch-config <admin.xml> --apply
    ... --all        все активные конфигурации
    ... --plan       только показать DDL и гранты
"""

import argparse
import os
import subprocess
import sys

sys.path.insert(0, __file__.rsplit("/core/", 1)[0])

# admin-доступ только для DDL; ETL работает под etl_writer, ch_admin в Airflow не заведён
ADMIN_CFG = os.path.expanduser("~/.config/clickhouse/ch_admin.xml")

from core.clickhouse.config import load_active_specs, load_spec   # noqa: E402


def run(cfg: str, sql: str) -> None:
    r = subprocess.run(["clickhouse-client", "--config-file", cfg], input=sql,
                       capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(r.stderr.strip()[:300])


def grants(spec) -> str:
    """
    Ровно те права, что нужны движку, и ни одним больше.
      TRUNCATE + ALTER MOVE PARTITION — на staging (MOVE проверяет ИСТОЧНИК)
      ALTER DELETE                    — на цели, этого требует REPLACE PARTITION
    CREATE/DROP TABLE не выдаются никогда.
    """
    g = (f"GRANT TRUNCATE, ALTER MOVE PARTITION ON {spec.stage_fqn} TO etl_writer;\n"
         f"GRANT ALTER DELETE ON {spec.fqn} TO etl_writer;")
    if spec.needs_raw:
        g += f"\nGRANT TRUNCATE ON {spec.raw_fqn} TO etl_writer;"
    return g


def main() -> int:
    ap = argparse.ArgumentParser(description="DDL целевых таблиц ClickHouse из конфигурации")
    ap.add_argument("--code")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--ch-config", default=ADMIN_CFG,
                    help=f"config-file clickhouse-client под ch_admin (по умолчанию {ADMIN_CFG}, права 600, вне репозитория)")
    ap.add_argument("--config-conn", default="etl_prod")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=args.config_conn)
    specs = load_active_specs(pg) if args.all else [load_spec(pg, args.code)]

    for spec in specs:
        print(f"\n=== {spec.code} → {spec.fqn} ===")
        tables = [(spec.fqn, spec.ddl(spec.fqn)), (spec.stage_fqn, spec.ddl(spec.stage_fqn))]
        if spec.needs_raw:
            tables.append((spec.raw_fqn, spec.ddl_raw()))
        for table, ddl in tables:
            if args.apply:
                run(args.ch_config, ddl)
                print(f"   создана {table}")
            else:
                print(ddl + ";\n")
        if args.apply:
            run(args.ch_config, grants(spec))
            print("   права etl_writer выданы")
        else:
            print(grants(spec))
    return 0


if __name__ == "__main__":
    sys.exit(main())
