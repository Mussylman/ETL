"""
Проверка расхождений по всем активным таблицам ClickHouse — READ ONLY, ничего не меняет.

Объединяет существующие проверки в один отчёт по каждой активной конфигурации etl_meta.ch_sync:
  обобщённые источники (postgres / mssql: справочники, cost_daily) — полный отпечаток источник ↔
    ClickHouse (строки, контрольная сумма, меры) и дубли бизнес-ключа — как ch_report;
  регистры 1С (sales, orders, stock …) — помесячно 1С ↔ ClickHouse (onec_reconcile):
    закрытые партиции — строго 1:1; операционно открытые (--open-window, текущая + N−1 предыдущих)
    — ClickHouse ⊆ 1С: существующие документы совпадают построчно, допустимы только новые документы
    1С, ещё не дошедшие патчем (они показываются как pending, не как ошибка);
  целостность — дубли бизнес-ключа, not_zero, строки без шапки, recorder → id = etl_meta.doc_key.

    PYTHONPATH=dags python3 -m core.tools.ch_verify --config-conn etl_prod
    ... --months 3                 только последние 3 партиции регистров 1С (быстро)
    ... --code fact_stock_positions  одна конфигурация (code или target_table)
    ... --no-doc-key               без сверки id с doc_key (самая тяжёлая по памяти часть)
    ... --json reports/verify.json   результат ещё и в файл

Статусы: PASS · WAIT (открытый месяц изменён после последней hot-пересборки — догонит в течение часа) · FAIL.
Код выхода: 0 — расхождений нет (PASS/WAIT), 1 — есть расхождения, 2 — ошибка запуска.
Запросы в 1С — с NOLOCK; в ClickHouse — агрегаты по партициям и бакетам, без выгрузки данных.
"""
import argparse
import json
import sys
import time
from datetime import datetime

sys.path.insert(0, __file__.rsplit("/core/", 1)[0])
from core.conn import require_conn                          # noqa: E402
from core.clickhouse import reconcile as rec                 # noqa: E402
from core.clickhouse import onec_reconcile as orc            # noqa: E402
from core.clickhouse.config import load_active_specs, load_group_specs   # noqa: E402
from core.clickhouse.runner import hot_partitions                     # noqa: E402
from core.clickhouse.source import open_source               # noqa: E402
from core.clickhouse.target import ClickHouse                # noqa: E402
from core.tools import ch_promote as cp                      # noqa: E402

BUCKETS = 16


def check_generic(ch, spec) -> dict:
    """Справочники / cost_daily: полный отпечаток и дубли — та же логика, что у ch_report."""
    r = {"code": spec.code, "table": spec.target_table, "kind": "source ↔ ClickHouse", "issues": []}
    if not ch.table_exists(spec.fqn):
        r["issues"].append(f"нет таблицы {spec.fqn}")
        return r
    src = open_source(spec)
    a = src.conn.get_first(rec.fingerprint_sql(spec, spec.source_type) + f" FROM {src.projected()}")
    b = ch.row(rec.fingerprint_sql(spec, "clickhouse") + f" FROM {spec.fqn}")
    r["source_rows"], r["ch_rows"] = int(a[0]), int(b[0])
    for name, x, y in rec.compare(spec, a, b):
        r["issues"].append(f"{name}: источник {x} ≠ ClickHouse {y}")
    if spec.business_key:
        dup = sum(int(ch.scalar(rec.duplicates_sql(spec, spec.fqn, bucket=b_, buckets=BUCKETS)) or 0)
                  for b_ in range(BUCKETS))
        if dup:
            r["issues"].append(f"дублей бизнес-ключа: {dup}")
    return r


def check_register(pg, ch, ms_factory, specs, months: int, open_window: int, doc_key: bool) -> list:
    """Регистр 1С: помесячная сверка каждой сверяемой цели + целостность всех целей регистра."""
    out = []
    pairs = [cp.Pair(s, s, s.source_params) for s in specs]
    # целостность (дубли, not_zero, сироты, doc_key) — по всем целям регистра
    if doc_key:
        bad, info = cp.integrity_gate(ch, pg, pairs)
    else:   # integrity_gate без doc_key: подменяем область, которой нет
        saved = [(p.shadow, dict(p.shadow.source_params or {})) for p in pairs]
        for p in pairs:
            p.shadow.source_params = {k: v for k, v in (p.shadow.source_params or {}).items() if k != "own_id"}
        bad, info = cp.integrity_gate(ch, pg, pairs)
        for s, sp in saved:
            s.source_params = sp
    for s in specs:
        r = {"code": s.code, "table": s.target_table, "kind": "1С ↔ ClickHouse", "issues": [],
             "ch_rows": info.get(s.code, {}).get("rows"), "integrity": info.get(s.code, {}),
             "partitions": {"checked": 0, "exact": 0, "open_ok": 0, "pending_docs": 0}}
        r["issues"] += [b for b in bad if b.startswith(s.code + ":")]
        try:
            plan = orc.plan(pg, s)
        except RuntimeError:
            r["kind"] = "целостность · шапка"
            out.append(r)
            continue
        ms = ms_factory(s.source_conn_id)
        parts = sorted(x for x in ch.query(f"SELECT DISTINCT {s.partition_expr} FROM {s.fqn}").split() if x)
        parts = [p for p in parts if not orc.prehistory(plan, p)]
        if months:
            parts = parts[-months:]
        opened = set(cp.open_partitions(ch, s, parts, open_window))
        hot_window = set(hot_partitions())
        for part in parts:
            r["partitions"]["checked"] += 1
            if part in opened:
                d = cp.open_partition_diff(ms, ch, s, plan, part)
                r["partitions"]["pending_docs"] += d["source_new"]
                hot = (s.source_params or {}).get("hot_rebuild") is not False and part in hot_window
                if d["ok"]:
                    r["partitions"]["open_ok"] += 1
                elif hot and not d["deleted"] and not d["shadow_extra"]:
                    # правки без сигнала retail (B2B-заказы и т.п.) догоняет ежечасная пересборка
                    r["partitions"]["open_ok"] += 1
                    r.setdefault("waits", []).append(f"{part}: изменено {d['changed']} док. после последней hot-пересборки "
                                                     f"— догонит hot в течение часа (например {d['examples']['changed'][:1]})")
                else:
                    r["issues"].append(f"{part} (открытая): изменено {d['changed']}, исчезло из 1С {d['deleted']}, "
                                       f"лишних {d['shadow_extra']}; примеры {d['examples']}")
                continue
            diff = orc.compare(orc.fingerprint_1c(ms, plan, part), orc.fingerprint_ch(ch, s, plan, part), plan)
            if diff:
                r["issues"].append(f"{part}: " + "; ".join(f"{n} 1С {x} ≠ CH {y}" for n, x, y in diff[:3]))
            else:
                r["partitions"]["exact"] += 1
        out.append(r)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Проверка расхождений по всем активным таблицам ClickHouse (READ ONLY)")
    ap.add_argument("--config-conn", help="Airflow conn_id control plane (обязателен)")
    ap.add_argument("--ch-conn", default="clickhouse_etl")
    ap.add_argument("--code", help="только одна конфигурация: code или target_table")
    ap.add_argument("--months", type=int, default=0, help="регистры 1С: только последние N партиций (0 — вся история)")
    ap.add_argument("--open-window", type=int, default=1,
                    help="операционно открытые партиции: текущая + N−1 предыдущих (в начале месяца — 2)")
    ap.add_argument("--no-doc-key", action="store_true", help="не сверять recorder → id с etl_meta.doc_key")
    ap.add_argument("--json", help="записать результат в JSON-файл")
    a = ap.parse_args(argv)
    conn_id = require_conn("--config-conn", a.config_conn)

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    pg = PostgresHook(postgres_conn_id=conn_id)
    ch = ClickHouse(a.ch_conn)
    hooks: dict = {}
    ms_factory = lambda cid: hooks.setdefault(cid, MsSqlHook(mssql_conn_id=cid))   # noqa: E731
    want = lambda s: not a.code or a.code in (s.code, s.target_table)              # noqa: E731

    t0 = time.time()
    results = []
    for spec in load_active_specs(pg):
        if want(spec):
            results.append(check_generic(ch, spec))
    groups = [g for g, in pg.get_records(
        "SELECT DISTINCT sync_group FROM etl_meta.ch_sync WHERE is_active AND source_type = 'onec_register' ORDER BY 1")]
    for g in groups:
        by_reg: dict = {}
        for s in load_group_specs(pg, g):
            by_reg.setdefault(s.source_object, []).append(s)
        for reg, specs in by_reg.items():
            if a.code and not any(want(s) for s in specs):
                continue
            for r in check_register(pg, ch, ms_factory, specs, a.months, a.open_window, not a.no_doc_key):
                if want(next(s for s in specs if s.code == r["code"])):
                    results.append(r)

    print(f"{'таблица':<24} {'проверка':<26} {'строк CH':>12}  {'партиций':>16}  статус")
    print("-" * 96)
    for r in results:
        p = r.get("partitions")
        pt = (f"{p['exact']}+{p['open_ok']} из {p['checked']}" if p and p["checked"] else "—")
        print(f"{r['table']:<24} {r['kind'][:26]:<26} {r.get('ch_rows') or 0:>12,}  {pt:>16}  "
              f"{'FAIL' if r['issues'] else ('WAIT' if r.get('waits') else 'PASS')}")
        for w in r.get("waits", []):
            print(f"{'':<24}   ⧗ WAIT {w}")
        if p and p["pending_docs"]:
            print(f"{'':<24}   ожидают патча (новые документы 1С в открытых партициях): {p['pending_docs']}")
        for i in r["issues"][:8]:
            print(f"{'':<24}   ✗ {i}")
        if len(r["issues"]) > 8:
            print(f"{'':<24}   … ещё {len(r['issues']) - 8}")
    ok = all(not r["issues"] for r in results)
    print("-" * 96)
    print(f"таблиц: {len(results)} · с расхождениями: {sum(1 for r in results if r['issues'])} · "
          f"{time.time() - t0:.0f} с\nРАСХОЖДЕНИЯ: {'НЕТ — PASS' if ok else 'ЕСТЬ — FAIL'}")
    if a.json:
        with open(a.json, "w") as f:
            json.dump({"checked_at": datetime.now().isoformat(timespec="seconds"), "ok": ok, "results": results},
                      f, ensure_ascii=False, indent=1, default=str)
    return 0 if ok else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # noqa: BLE001
        print(f"ОШИБКА ЗАПУСКА: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(2)
