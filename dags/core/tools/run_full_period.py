"""
CLI для запуска ETL в режиме full_period.

Использование:
    cd /home/dev/airflow
    python -u -m dags.core.tools.run_full_period \
        --register sales \
        --start 4026-06-14 \
        --end   4026-06-18

Или через PYTHONPATH:
    PYTHONPATH=/home/dev/airflow/dags python -u -m core.tools.run_full_period ...

Параметры:
    --register   код регистра из etl_meta.registers.code (default: sales)
    --start      нижняя граница _Period в формате 1С (YYYY-MM-DD с offset +2000)
    --end        верхняя граница _Period (включительно)
    --targets    необязательно: список target_table через запятую
    --log-dir    куда писать лог (default: /tmp)
    --truncate   очистить целевые таблицы перед загрузкой (default: False)
"""

import argparse
import logging
import os
import sys
import time
import warnings
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description="Запуск ETL full_period")
    parser.add_argument("--register", default="sales", help="код регистра (default: sales)")
    parser.add_argument("--start", required=True, help="start_date в формате 1С, напр. 4026-06-14")
    parser.add_argument("--end", required=True, help="end_date в формате 1С, напр. 4026-06-18")
    parser.add_argument("--targets", default=None, help="target_table через запятую, опционально")
    parser.add_argument("--log-dir", default="/tmp", help="каталог для лога")
    parser.add_argument("--truncate", action="store_true", help="TRUNCATE целевых таблиц перед загрузкой")
    parser.add_argument("--skip-names", action="store_true",
                        help="не догружать имена справочников после загрузки "
                             "(по умолчанию догружаются — иначе витрина остаётся "
                             "со stub-строками без имён)")
    args = parser.parse_args()

    warnings.filterwarnings("ignore")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        stream=sys.stdout,
    )

    # tee в файл
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(args.log_dir, f"etl_{args.register}_{args.start}_{args.end}_{ts}.log")

    class Tee:
        def __init__(self, *streams):
            self.streams = streams
        def write(self, s):
            for st in self.streams:
                st.write(s); st.flush()
        def flush(self):
            for st in self.streams:
                st.flush()

    log_file = open(log_path, "w")
    sys.stdout = Tee(sys.__stdout__, log_file)
    sys.stderr = Tee(sys.__stderr__, log_file)

    print("=" * 70)
    print(f"  ETL FULL_PERIOD")
    print(f"  register = {args.register}")
    print(f"  period   = {args.start} .. {args.end}")
    print(f"  log      = {log_path}")
    print("=" * 70)

    # Optional truncate
    if args.truncate:
        print("\n[truncate] очищаю target-таблицы…")
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from core.etl_engine import ETLEngine
        # Используем конфиг через ETLEngine, чтобы получить список targets без дублирования логики
        etl_probe = ETLEngine(register_code=args.register, mode="full_period",
                              start_date=args.start, end_date=args.end)
        pg = PostgresHook(postgres_conn_id=etl_probe.dst_conn_id)
        tables = [t.target_table for t in etl_probe.config.targets if t.is_active]
        # fact (priority desc) → dim — иначе FK не даст
        tables_in_order = [t.target_table for t in sorted(etl_probe.config.targets, key=lambda x: -x.priority) if t.is_active]
        sql = ", ".join(f'public.{t}' for t in tables_in_order)
        if sql:
            pg.run(f"TRUNCATE TABLE {sql} RESTART IDENTITY;")
            print(f"[truncate] выполнено: {sql}")

    # Run
    from core.etl_engine import ETLEngine
    kwargs = dict(register_code=args.register, mode="full_period",
                  start_date=args.start, end_date=args.end)
    if args.targets:
        kwargs["target_tables"] = [t.strip() for t in args.targets.split(",") if t.strip()]

    t0 = time.time()
    etl = ETLEngine(**kwargs)
    result = etl.run()
    elapsed = time.time() - t0

    # Имена справочников: post_load создаёт stub-строки с id, но без имени —
    # без этого шага витрина остаётся безымянной до ручного прогона.
    if not args.skip_names:
        print("\n[dim-names] догружаю имена справочников…")
        try:
            from core.tools.load_dim_names import enrich_all
            totals = enrich_all(only_stub=True, raise_on_error=False)
            print(f"[dim-names] {totals}")
        except Exception as e:
            print(f"[dim-names] ПРОПУЩЕНО из-за ошибки: {str(e)[:200]}")

    print("\n" + "=" * 70)
    print(f"  RESULT  ({elapsed:.1f}s)")
    for tbl, rows in (result or {}).items():
        print(f"    {tbl:<25s} {rows:>10d} rows")
    print("=" * 70)
    print(f"  log: {log_path}")


if __name__ == "__main__":
    main()
