"""
retail-метка в справочниках: dim.retail_updated_at ← retail.updated_at.

ЕДИНЫЙ механизм для всех retail-таблиц, ТРИ режима — отличаются ТОЛЬКО тем,
какая дата попадает в строку:

    initial  (первичная заливка) — ОДНА MAX(updated_at) из retail-таблицы
                                   присваивается ВСЕМ строкам dim. Не построчно.
    reload   (перезаливка)       — то же самое, что initial.
    incremental                  — ПОСТРОЧНО: каждая изменённая строка получает
                                   свою реальную retail-дату по guid.

Per-row дата бывает ТОЛЬКО в режиме incremental. Первичная и перезаливка всегда
кладут одно значение на всю таблицу — это снимок «справочник соответствует
состоянию retail на этот момент», а не история изменений каждой строки.

Тот же принцип уже работает в фактах (docs/sales_load_modes.md): full_period
ставит один retail_snapshot_at всем строкам, incremental — per-row
retail_updated_at. Здесь обе роли живёт в одной колонке.

Роль скрипта — только метка. Он НЕ вставляет строки, НЕ пишет name/code,
НЕ снимает is_stub: строки создают факты (stub-резолв post_load), имена
приходят из 1С (load_dim_names.py). Из retail читаются два поля: uid и updated_at.

Использование:
    source venv/bin/activate
    PYTHONPATH=dags python3 -m core.tools.set_dim_retail_marks --mode initial --dry-run
    PYTHONPATH=dags python3 -m core.tools.set_dim_retail_marks --mode initial
    PYTHONPATH=dags python3 -m core.tools.set_dim_retail_marks --mode incremental
    PYTHONPATH=dags python3 -m core.tools.set_dim_retail_marks --dim dim_sklad --mode reload
"""

import argparse
import sys
import warnings
from typing import Dict, List, Optional, Tuple

MAPPING: Dict[str, str] = {
    "dim_nomenklatura":  "products",
    "dim_sklad":         "warehouses",
    "dim_podrazdelenie": "departments",
    "dim_kachestvo":     "qualities",
}

UUID_RE = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
SNAPSHOT_MODES = ("initial", "reload")


def _has_column(hook, table: str, column: str, schema: str = "public") -> bool:
    return bool(hook.get_first(
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s AND column_name=%s",
        parameters=(schema, table, column))[0])


def _live_filter(rt, retail_table: str) -> str:
    """soft-delete есть не у всех справочников — фильтр только если колонка есть."""
    return "deleted_at IS NULL" if _has_column(rt, retail_table, "deleted_at") else "TRUE"


def retail_max(rt, retail_table: str):
    """ОДНА метка на всю таблицу: MAX(updated_at) из retail (UTC→Almaty)."""
    row = rt.get_first(f"""
        SELECT MAX((updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp)
        FROM   public.{retail_table}
        WHERE  {_live_filter(rt, retail_table)}
    """)
    return row[0] if row else None


def apply_snapshot(pg, dim_table: str, mark, dry_run: bool) -> int:
    """
    Режимы initial / reload: одно значение ВСЕМ строкам справочника.

    IS DISTINCT FROM — повторный прогон с той же меткой не переписывает строки.
    """
    sql = f"""
        UPDATE public.{dim_table}
        SET    retail_updated_at = %s
        WHERE  guid IS NOT NULL
          AND  retail_updated_at IS DISTINCT FROM %s::timestamp
    """
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (mark, mark))
            n = cur.rowcount
        conn.rollback() if dry_run else conn.commit()
        return n
    finally:
        conn.close()


def fetch_changed(rt, retail_table: str, since) -> List[Tuple[str, object]]:
    """Режим incremental: (guid, updated_at) только тех строк, что новее метки."""
    where = [_live_filter(rt, retail_table), "uid IS NOT NULL",
             f"uid ~ '{UUID_RE}'", "updated_at IS NOT NULL"]
    params = ()
    if since is not None:
        where.append("(updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp > %s")
        params = (since,)
    sql = f"""
        SELECT lower(uid),
               (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp
        FROM   public.{retail_table}
        WHERE  {' AND '.join(where)}
    """
    return [(r[0], r[1]) for r in rt.get_records(sql, parameters=params or None)]


def apply_per_row(pg, dim_table: str, marks: List[Tuple[str, object]],
                  batch: int, dry_run: bool) -> int:
    """Режим incremental: каждой строке — своя дата по guid."""
    if not marks:
        return 0
    from psycopg2.extras import execute_values
    sql = f"""
        UPDATE public.{dim_table} d
        SET    retail_updated_at = v.retail_updated_at
        FROM  (VALUES %s) AS v(guid, retail_updated_at)
        WHERE  d.guid = v.guid::uuid
          AND  d.retail_updated_at IS DISTINCT FROM v.retail_updated_at::timestamp
    """
    conn = pg.get_conn()
    total = 0
    try:
        for i in range(0, len(marks), batch):
            with conn.cursor() as cur:
                execute_values(cur, sql, marks[i:i + batch], page_size=batch)
                total += max(cur.rowcount, 0)
            conn.rollback() if dry_run else conn.commit()
        return total
    finally:
        conn.close()


def process(dim_table: str, retail_table: str, pg, rt, mode: str,
            batch: int, dry_run: bool) -> Optional[dict]:
    if not _has_column(pg, dim_table, "retail_updated_at"):
        print(f"  ✗ {dim_table}: нет колонки retail_updated_at — примените миграции 009/010")
        return None

    rows, with_mark, watermark = pg.get_first(
        f"SELECT count(*), count(retail_updated_at), max(retail_updated_at) "
        f"FROM public.{dim_table}")

    if mode in SNAPSHOT_MODES:
        mark = retail_max(rt, retail_table)
        if mark is None:
            print(f"  ⚠ {dim_table:20s} ← {retail_table:12s} | в retail нет ни одного "
                  f"updated_at — метку поставить не из чего, пропускаю")
            return {"dim": dim_table, "rows": rows, "touched": 0,
                    "mark": None, "mode": mode}
        touched = apply_snapshot(pg, dim_table, mark, dry_run)
        print(f"  {dim_table:20s} ← {retail_table:12s} | строк {rows:>6} | "
              f"{'проставилась бы' if dry_run else 'проставлена'} ОДНА метка {mark} | "
              f"затронуто {touched:>6}")
        return {"dim": dim_table, "rows": rows, "touched": touched, "mark": mark, "mode": mode}

    # incremental — per-row от текущего watermark справочника
    changed = fetch_changed(rt, retail_table, watermark)
    touched = apply_per_row(pg, dim_table, changed, batch, dry_run)
    print(f"  {dim_table:20s} ← {retail_table:12s} | строк {rows:>6} | "
          f"watermark {watermark} | изменений в retail {len(changed):>6} | "
          f"{'обновилось бы' if dry_run else 'обновлено'} {touched:>6}")
    return {"dim": dim_table, "rows": rows, "touched": touched,
            "mark": None, "mode": mode, "changed": len(changed)}


def main() -> None:
    ap = argparse.ArgumentParser(description="retail-метка в справочники")
    ap.add_argument("--mode", choices=["initial", "reload", "incremental"], default="initial",
                    help="initial/reload — ОДНА MAX-дата всем строкам; "
                         "incremental — построчно по guid")
    ap.add_argument("--dim", nargs="*", default=None, help=f"по умолчанию все: {list(MAPPING)}")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--pg-conn", required=True)
    ap.add_argument("--retail-conn", default="bd_retail")
    args = ap.parse_args()
    warnings.filterwarnings("ignore")

    targets = args.dim or list(MAPPING)
    unknown = [t for t in targets if t not in MAPPING]
    if unknown:
        print(f"неизвестные dim: {unknown}; доступны: {list(MAPPING)}")
        sys.exit(2)

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=args.pg_conn)
    rt = PostgresHook(postgres_conn_id=args.retail_conn)

    kind = ("ОДНА MAX-дата всем строкам" if args.mode in SNAPSHOT_MODES
            else "построчно по guid")
    print("=" * 104)
    print(f"  RETAIL-МЕТКА, режим '{args.mode}' — {kind}{'  [DRY-RUN]' if args.dry_run else ''}")
    print("=" * 104)

    results, failed = [], []
    for dim_table in targets:
        try:
            r = process(dim_table, MAPPING[dim_table], pg, rt, args.mode, args.batch, args.dry_run)
            (results if r else failed).append(r or dim_table)
        except Exception as e:
            print(f"  ✗ {dim_table}: {str(e)[:180]}")
            failed.append(dim_table)

    print("-" * 104)
    if results:
        print(f"  ИТОГО {'затронулось бы' if args.dry_run else 'затронуто'}: "
              f"{sum(r['touched'] for r in results)} строк")
    no_mark = [r["dim"] for r in results if r.get("mark") is None
               and r["mode"] in SNAPSHOT_MODES and r["rows"] > 0]
    if no_mark:
        print(f"  ⚠ без метки {'останутся' if args.dry_run else 'остались'}: {no_mark} — "
              f"в retail у этих объектов updated_at пуст, watermark по ним не построить")
    if failed:
        print(f"  С ОШИБКОЙ: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
