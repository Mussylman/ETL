"""
Статус сверки по месяцам текущего года → etl_meta.reconcile_status (страница «Сверка» конфигуратора).

READ ONLY по отношению к данным: читает 1С (NOLOCK), источники и ClickHouse, пишет только свою таблицу статуса.

    PYTHONPATH=dags python3 -m core.tools.reconcile_status --config-conn etl_prod --mode light   # ~1 мин
    PYTHONPATH=dags python3 -m core.tools.reconcile_status --config-conn etl_prod --mode full    # ~5 мин
    ... --year 2025      другой год (по умолчанию — текущий, в январе ещё и декабрь прошлого)

light — строки + SUM мер по месяцу (быстро, не видит подмен с теми же суммами);
full  — отпечаток onec_reconcile / reconcile (строки, хеш ключей и ссылок, суммы и квадраты мер),
        открытый месяц регистра — правилом «ClickHouse ⊆ 1С» (новые документы — не ошибка).

Статус ячейки — по факту, а не по проценту:
  PASS — совпало;
  WAIT — расхождение ещё должен исправить штатный механизм: текущий месяц (patch каждые 5 мин, hot),
         прошлый месяц с hot (ежечасно), остальное — ночной sweep; справочники/cost_daily — цикл DAG;
  FAIL — этот механизм уже отработал после того, как расхождение замечено, а оно осталось → реагировать.
"""
import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

sys.path.insert(0, __file__.rsplit("/core/", 1)[0])
from core.conn import require_conn                                   # noqa: E402
from core.clickhouse import onec_reconcile as orc                     # noqa: E402
from core.clickhouse import reconcile as rec                          # noqa: E402
from core.clickhouse.config import load_active_specs, load_group_specs   # noqa: E402
from core.clickhouse.runner import hot_partitions                     # noqa: E402
from core.clickhouse.source import open_source                        # noqa: E402
from core.clickhouse.target import ClickHouse                         # noqa: E402

ALMATY = ZoneInfo("Asia/Almaty")
MARGIN = timedelta(minutes=15)      # механизм «отработал после обнаружения» — с запасом на длительность прогона


def _d(x) -> Decimal:
    return Decimal(str(x if x is not None else 0))


def pct(src: dict, dst: dict) -> float:
    """max относительного отклонения по строкам и мерам, %."""
    worst = Decimal(0)
    for k in set(src) | set(dst):
        a, b = _d(src.get(k)), _d(dst.get(k))
        if a != b:
            worst = max(worst, abs(a - b) / max(abs(a), Decimal(1)) * 100)
    return float(round(worst, 4))


def months_of(year: int, current: str) -> list:
    last = min(int(current), year * 100 + 12)
    out = [f"{year}{m:02d}" for m in range(1, 13) if year * 100 + m <= last]
    if current.endswith("01") and int(current[:4]) == year:    # в январе — ещё и декабрь прошлого года
        out.insert(0, f"{year - 1}12")
    return out


# ------------------------------------------------------------------ значения
def light_register(ms, ch, spec, plan, part):
    y, m = int(part[:4]), int(part[4:])
    a = f"{y + 2000:04d}-{m:02d}-01"
    b = f"{y + 2000 + (m == 12):04d}-{(1 if m == 12 else m + 1):02d}-01"
    src = {"rows": Decimal(0), **{x: Decimal(0) for x in plan["measures"]}}
    for mb in plan["members"]:
        meas = "".join(f", ISNULL(SUM(CAST({e} AS decimal(38,8))),0)" for e in mb["measures"])
        cond = [f"{mb['period']} >= '{a}'", f"{mb['period']} < '{b}'"] + [f"({w})" for w in mb["where"]]
        row = ms.get_first(f"SELECT COUNT(*){meas} FROM {mb['from']} WHERE {' AND '.join(cond)}")
        src["rows"] += _d(row[0])
        for i, x in enumerate(plan["measures"]):
            src[x] += _d(row[i + 1])
    meas = "".join(f", sum(toDecimal128({x},8))" for x in plan["measures"])
    r = ch.row(f"SELECT count(){meas} FROM {spec.fqn} WHERE {spec.partition_expr} = {int(part)}")
    dst = {"rows": _d(r[0]), **{x: _d(r[i + 1]) for i, x in enumerate(plan["measures"])}}
    return src, dst


def light_header(ms, ch, spec, plan, part):
    """Шапка, собранная из строк регистра: число документов в 1С (по плану строк) против строк шапки."""
    y, m = int(part[:4]), int(part[4:])
    a = f"{y + 2000:04d}-{m:02d}-01"
    b = f"{y + 2000 + (m == 12):04d}-{(1 if m == 12 else m + 1):02d}-01"
    n = 0
    for mb in plan["members"]:
        cond = [f"{mb['period']} >= '{a}'", f"{mb['period']} < '{b}'"] + [f"({w})" for w in mb["where"]]
        n += int(ms.get_first(f"SELECT COUNT(*) FROM (SELECT DISTINCT {mb['keys'][0]} AS r, {mb['keys'][1]} AS t "
                              f"FROM {mb['from']} WHERE {' AND '.join(cond)}) x")[0])
    c = int(ch.scalar(f"SELECT count() FROM {spec.fqn} WHERE {spec.partition_expr} = {int(part)}"))
    return {"docs": Decimal(n)}, {"docs": Decimal(c)}


def generic_by_month(ch, spec, parts):
    """Обобщённый партиционированный источник (cost_daily): строки + SUM мер по месяцам одним запросом."""
    src = open_source(spec)
    col, ms = spec.partition_column, spec.measure_columns
    lo, hi = min(parts), max(parts)
    pk = src.partition_key_expr(col)
    sums = "".join(f", sum({x})" for x in ms)
    rows = src.conn.get_records(f"SELECT {pk} AS p, count(*){sums} FROM {src.projected(f' WHERE {pk} BETWEEN {lo} AND {hi}')} "
                                f"GROUP BY 1")
    a = {str(int(r[0])): {"rows": _d(r[1]), **{x: _d(r[i + 2]) for i, x in enumerate(ms)}} for r in rows}
    sums_ch = "".join(f", sum(toDecimal128({x},8))" for x in ms)
    b = {}
    for line in ch.query(f"SELECT {spec.partition_expr} AS p, count(){sums_ch} FROM {spec.fqn} "
                         f"WHERE p BETWEEN {lo} AND {hi} GROUP BY p").splitlines():
        v = line.split("\t")
        b[v[0]] = {"rows": _d(v[1]), **{x: _d(v[i + 2]) for i, x in enumerate(ms)}}
    return {p: (a.get(p, {"rows": Decimal(0)}), b.get(p, {"rows": Decimal(0)})) for p in parts}


def full_generic(ch, spec, key=None):
    src = open_source(spec)
    where = src.partition_filter(key) if key else ""
    a = src.conn.get_first(rec.fingerprint_sql(spec, spec.source_type) + f" FROM {src.projected(where)}")
    b = ch.row(rec.fingerprint_sql(spec, "clickhouse") + f" FROM {spec.fqn}"
               + (f" WHERE {spec.partition_expr} = {int(key)}" if key else ""))
    names = ["rows", "checksum"] + [x for m in spec.measure_columns for x in (m, m + "²")]
    return dict(zip(names, map(_d, a))), dict(zip(names, map(_d, b)))


# ------------------------------------------------------------------ классификация
def state_marks(pg) -> dict:
    """Когда последний раз отработали hot и sweep (ch_source_state, Almaty naive → UTC)."""
    out = {}
    for k, ts in pg.get_records("SELECT source_key, watermark FROM etl_meta.ch_source_state "
                                "WHERE source_key IN ('analytics_sync:hot', 'analytics_sync:sweep')"):
        out[k.rsplit(":", 1)[1]] = ts.replace(tzinfo=ALMATY).astimezone(timezone.utc) if ts else None
    return out


def last_success(pg, target) -> datetime:
    ts = pg.get_first("SELECT max(h.finished_at) FROM etl_meta.ch_sync_history h JOIN etl_meta.ch_sync s ON s.id = h.sync_id "
                      "WHERE s.target_table = %s AND s.is_active AND h.status = 'success'", parameters=(target,))[0]
    return ts.replace(tzinfo=timezone.utc) if ts else None


def classify(kind, part, current, hot_on, prev, now, marks, cycle_done):
    """→ (status, fixer, first_mismatch_at). prev — прошлое состояние ячейки."""
    first = prev["first_mismatch_at"] if prev and prev["status"] != "PASS" and prev["first_mismatch_at"] else now
    hot_window = set(hot_partitions())
    if kind == "generic":
        fixer, done = "следующий цикл analytics_sync (5 мин)", cycle_done
    elif part == current:
        return "WAIT", ("patch каждые 5 мин + hot ежечасно" if hot_on else "patch каждые 5 мин + ночной sweep"), first
    elif hot_on and part in hot_window:
        fixer, done = "hot (ежечасно)", marks.get("hot")
    else:
        fixer, done = "ночной sweep (после 03:00)", marks.get("sweep")
    return ("FAIL" if done and done > first + MARGIN else "WAIT"), fixer, first


# ------------------------------------------------------------------ прогон
def run(conn_id: str, mode: str, year: int | None = None, ch_conn: str = "clickhouse_etl") -> dict:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    pg = PostgresHook(postgres_conn_id=conn_id)
    ch = ClickHouse(ch_conn)
    now = datetime.now(timezone.utc)
    current = hot_partitions()[1]
    year = year or int(current[:4])
    parts = months_of(year, current)
    marks = state_marks(pg)
    prev = {(t, p): {"status": s, "first_mismatch_at": f, "checked_at": ca, "changed_keys": (cv or {}).get("changed_keys")}
            for t, p, s, f, ca, cv in pg.get_records(
        "SELECT target_table, partition_key, status, first_mismatch_at, checked_at, ch_values FROM etl_meta.reconcile_status "
        "WHERE check_kind = %s", parameters=(mode,))}
    rows = []

    def put(target, part, kind, src, dst, hot_on=False, note=None, cycle_done=None, override=None, ms=0):
        vs, vd = ({k: v for k, v in x.items() if not isinstance(v, list)} for x in (src, dst))
        mismatch = vs != vd
        if override:
            status, fixer = override
            first = prev.get((target, part), {}).get("first_mismatch_at") or now if status != "PASS" else None
        elif not mismatch:
            status, fixer, first = "PASS", None, None
        else:
            status, fixer, first = classify(kind, part, current, hot_on, prev.get((target, part)), now, marks, cycle_done)
        rows.append((target, part, mode, status, pct(vs, vd) if mismatch else 0,
                     json.dumps({k: (v if isinstance(v, list) else str(v)) for k, v in src.items()}),
                     json.dumps({k: (v if isinstance(v, list) else str(v)) for k, v in dst.items()}),
                     fixer, note, first, now, ms))

    # обобщённые источники: справочники — целиком, партиционированные (cost_daily) — по месяцам
    for spec in load_active_specs(pg):
        t0 = time.time()
        cyc = last_success(pg, spec.target_table)
        if not spec.is_partitioned:
            a, b = full_generic(ch, spec)
            put(spec.target_table, "all", "generic", a, b, cycle_done=cyc, ms=int((time.time() - t0) * 1000))
        elif mode == "light":
            for p, (a, b) in generic_by_month(ch, spec, parts).items():
                put(spec.target_table, p, "generic", a, b, cycle_done=cyc)
        else:
            for p in parts:
                a, b = full_generic(ch, spec, p)
                put(spec.target_table, p, "generic", a, b, cycle_done=cyc)

    # регистры 1С
    from core.tools.ch_promote import open_partition_diff
    hooks = {}
    for g in [r[0] for r in pg.get_records("SELECT DISTINCT sync_group FROM etl_meta.ch_sync WHERE is_active "
                                           "AND source_type = 'onec_register' ORDER BY 1")]:
        by_reg = {}
        for s in load_group_specs(pg, g):
            by_reg.setdefault(s.source_object, []).append(s)
        for reg, specs in by_reg.items():
            plans = {}
            for s in specs:
                try:
                    plans[s.code] = orc.plan(pg, s)
                except RuntimeError:
                    plans[s.code] = None
            child_plan = next((p for p in plans.values() if p), None)
            for s in specs:
                ms = hooks.setdefault(s.source_conn_id, MsSqlHook(mssql_conn_id=s.source_conn_id))
                hot_on = (s.source_params or {}).get("hot_rebuild") is not False
                plan = plans[s.code]
                have = set(x for x in ch.query(f"SELECT DISTINCT {s.partition_expr} FROM {s.fqn}").split() if x)
                for p in parts:
                    if p not in have:
                        continue
                    if plan and orc.prehistory(plan, p):
                        continue
                    t0 = time.time()
                    if plan is None:
                        if child_plan is None:
                            continue
                        a, b = light_header(ms, ch, s, child_plan, p)
                        put(s.target_table, p, "register", a, b, hot_on, note="документов: 1С против шапки",
                            ms=int((time.time() - t0) * 1000))
                        continue
                    if mode == "light":
                        a, b = light_register(ms, ch, s, plan, p)
                        put(s.target_table, p, "register", a, b, hot_on, ms=int((time.time() - t0) * 1000))
                        continue
                    if p == current:     # открытый месяц — «ClickHouse ⊆ 1С»
                        d = open_partition_diff(ms, ch, s, plan, p)
                        a = {"docs": Decimal(d["source_docs"])}
                        b = {"docs": Decimal(d["shadow_docs"]), "changed_keys": d["changed_keys"]}
                        if d["ok"]:
                            override = ("PASS", None) if not d["source_new"] else ("WAIT", "patch каждые 5 мин")
                        elif d["deleted"] or d["shadow_extra"]:
                            override = ("FAIL", "документ исчез из 1С / лишний — проверить вручную")
                        else:
                            # правки без сигнала: исправит hot (если включён) или ночной sweep. FAIL — только если
                            # те же документы остались изменёнными после того, как этот механизм уже прошёл.
                            fixer = "hot (ежечасно)" if hot_on else "ночной sweep (после 03:00)"
                            done = marks.get("hot" if hot_on else "sweep")
                            pv = prev.get((s.target_table, p)) or {}
                            same = set(d["changed_keys"]) & set(pv.get("changed_keys") or [])
                            stale = done and pv.get("checked_at") and done > pv["checked_at"] + MARGIN
                            override = ("FAIL", fixer + " прошёл, документы те же") if same and stale else ("WAIT", fixer)
                        put(s.target_table, p, "register", a, b, hot_on, override=override,
                            note=f"новых {d['source_new']}, изменено {d['changed']}, исчезло {d['deleted']}, лишних {d['shadow_extra']}",
                            ms=int((time.time() - t0) * 1000))
                        continue
                    fa, fb = orc.fingerprint_1c(ms, plan, p), orc.fingerprint_ch(ch, s, plan, p)
                    names = ["rows", "checksum"] + [x for m in plan["measures"] for x in (m, m + "²")]
                    put(s.target_table, p, "register", dict(zip(names, map(_d, fa))), dict(zip(names, map(_d, fb))),
                        hot_on, ms=int((time.time() - t0) * 1000))

    c = pg.get_conn()
    try:
        cur = c.cursor()
        cur.executemany("""INSERT INTO etl_meta.reconcile_status (target_table, partition_key, check_kind, status, diff_pct,
                             source_values, ch_values, fixer, note, first_mismatch_at, checked_at, duration_ms)
                           VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s)
                           ON CONFLICT (target_table, partition_key, check_kind) DO UPDATE SET
                             status = EXCLUDED.status, diff_pct = EXCLUDED.diff_pct, source_values = EXCLUDED.source_values,
                             ch_values = EXCLUDED.ch_values, fixer = EXCLUDED.fixer, note = EXCLUDED.note,
                             first_mismatch_at = EXCLUDED.first_mismatch_at, checked_at = EXCLUDED.checked_at,
                             duration_ms = EXCLUDED.duration_ms""", rows)
        c.commit()
    finally:
        c.close()
    summary = {s: sum(1 for r in rows if r[3] == s) for s in ("PASS", "WAIT", "FAIL")}
    return {"mode": mode, "year": year, "cells": len(rows), **summary}


def due_full(pg) -> bool:
    """Полная — раз в сутки после 05:00 Алматы (ночной sweep уже прошёл)."""
    now = datetime.now(ALMATY)
    if now.hour < 5:
        return False
    since = now.replace(hour=5, minute=0, second=0, microsecond=0)
    last = pg.get_first("SELECT max(checked_at) FROM etl_meta.reconcile_status WHERE check_kind = 'full'")[0]
    return last is None or last < since


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Статус сверки по месяцам → etl_meta.reconcile_status")
    ap.add_argument("--config-conn", help="Airflow conn_id control plane (обязателен)")
    ap.add_argument("--mode", choices=["light", "full", "auto"], default="light")
    ap.add_argument("--year", type=int)
    a = ap.parse_args(argv)
    conn = require_conn("--config-conn", a.config_conn)
    modes = [a.mode]
    if a.mode == "auto":
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        modes = ["light"] + (["full"] if due_full(PostgresHook(postgres_conn_id=conn)) else [])
    for m in modes:
        t0 = time.time()
        print(m, run(conn, m, a.year), f"{time.time() - t0:.0f} с", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
