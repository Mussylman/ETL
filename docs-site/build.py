"""
Сборка внутреннего handover-портала Data Platform.

    venv/bin/python3 docs-site/build.py            # snapshot из production (READ ONLY) + сборка dist/
    venv/bin/python3 docs-site/build.py --offline  # без обращения к production: последний snapshot

Live snapshot — только чтение метаданных: etl_meta (конфиг, история), system.parts / system.tables
ClickHouse, метаданные Airflow. Никаких полных сканов фактов. Если production недоступен, сборка
не падает: берётся последний content/generated/snapshot.json и в шапке показывается «Live config
unavailable». Секреты в snapshot и в страницы не попадают: запросы только к метаданным.
"""
import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent
GEN = ROOT / "content" / "generated" / "snapshot.json"
DIST = ROOT / "dist"
ALMATY = ZoneInfo("Asia/Almaty")
CH_ADMIN = os.path.expanduser("~/.config/clickhouse/ch_admin.xml")
CH_DB = "analytics_poc"

DOMAINS = [   # домен → таблицы ClickHouse (каталог портала; конфиг — из etl_meta)
    ("Sales", ["fact_sales", "fact_sales_positions"]),
    ("Orders", ["fact_orders", "fact_order_positions"]),
    ("Stock", ["fact_stock", "fact_stock_positions"]),
    ("Cost Daily", ["cost_daily"]),
]


def now_almaty() -> str:
    return datetime.now(ALMATY).strftime("%Y-%m-%d %H:%M")


def to_almaty(ts) -> str | None:
    """Журналы control plane пишут UTC (naive) — показываем по Алматы."""
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(ALMATY).strftime("%Y-%m-%d %H:%M")


# ------------------------------------------------------------------ snapshot (READ ONLY)
def ch_query(sql: str) -> list[list[str]]:
    cmd = ["clickhouse-client", "--readonly", "1", "--max_execution_time", "30"]
    cmd += ["--config-file", CH_ADMIN] if os.path.exists(CH_ADMIN) else []
    r = subprocess.run(cmd, input=sql, capture_output=True, text=True, timeout=60)
    if r.returncode:
        raise RuntimeError(r.stderr.strip()[:300])
    return [l.split("\t") for l in r.stdout.splitlines() if l]


def collect() -> dict:
    sys.path.insert(0, str(REPO / "dags"))
    os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id="etl_prod")
    conn = pg.get_conn()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    def q(sql, params=None):
        cur.execute(sql, params)
        return cur.fetchall()

    snap: dict = {"captured_at": now_almaty(), "live": True}
    snap["postgres"] = {
        "etl_prod_bytes": q("SELECT pg_database_size('etl_prod')")[0][0],
        "cluster_bytes": q("SELECT sum(pg_database_size(datname)) FROM pg_database")[0][0],
        "databases": [{"name": n, "bytes": b} for n, b in q(
            "SELECT datname, pg_database_size(datname) FROM pg_database WHERE NOT datistemplate ORDER BY 2 DESC")],
        "public_tables": [{"name": n, "rows": int(r), "bytes": b} for n, r, b in q(
            "SELECT c.relname, greatest(c.reltuples, 0)::bigint, pg_total_relation_size(c.oid) FROM pg_class c "
            "WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'r' ORDER BY 3 DESC")],
        "fact_tables_in_pg": [r[0] for r in q(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename IN "
            "('sales','sales_positions','orders','order_positions','stock','stock_positions')")],
    }
    snap["groups"] = [{"group": g, "dag": d, "position": p, "runner": rn, "active": a} for g, d, p, rn, a in q(
        "SELECT sync_group, dag_id, position, runner, is_active FROM etl_meta.ch_sync_group ORDER BY position")]
    snap["ch_sync"] = [
        {"code": c, "group": g, "source_type": st, "source_object": so, "source_conn": sc, "target": tt,
         "load_mode": lm, "partition": pe, "hot_rebuild": (sp or {}).get("hot_rebuild"),
         "signal_sources": [s.get("table") for s in (sp or {}).get("signal_sources") or []],
         "lookup": (lk or {}).get("table"), "business_key": bk, "columns": nc}
        for c, g, st, so, sc, tt, lm, pe, sp, lk, bk, nc in q(
            "SELECT s.code, s.sync_group, s.source_type, s.source_object, s.source_conn_id, s.target_table, s.load_mode, "
            "s.partition_expr, s.source_params, s.lookup, s.business_key, "
            "(SELECT count(*) FROM etl_meta.ch_sync_columns c WHERE c.sync_id = s.id) "
            "FROM etl_meta.ch_sync s WHERE s.is_active ORDER BY s.sync_group, s.priority")]
    snap["retired_ch_sync"] = q("SELECT count(*) FROM etl_meta.ch_sync WHERE NOT is_active AND description LIKE '[RETIRED%%'")[0][0]
    snap["registers"] = [{"code": c, "pipeline": p, "retail_table": rt, "pg_fact_write": w, "dim_key_source": ks}
                         for c, p, rt, w, ks in q(
        "SELECT code, pipeline_type, retail_table, pg_fact_write, dim_key_source FROM etl_meta.registers "
        "WHERE is_active ORDER BY pipeline_type, code")]
    snap["register_targets"] = [{"register": r, "target": t, "role": ro} for r, t, ro in q(
        "SELECT r.code, t.target_table, t.target_role FROM etl_meta.register_targets t "
        "JOIN etl_meta.registers r ON r.id = t.register_id WHERE t.is_active AND r.is_active ORDER BY r.code, t.priority")]
    scopes = []
    for doc_table, seq, issuer in q("SELECT doc_table, sequence_name, issuer FROM etl_meta.doc_key_scope ORDER BY 1"):
        last = q(f"SELECT last_value FROM {seq}")[0][0] if q("SELECT to_regclass(%s)", (seq,))[0][0] else None
        mx = q("SELECT max(id) FROM etl_meta.doc_key WHERE doc_table = %s", (doc_table,))[0][0]
        scopes.append({"scope": doc_table, "sequence": seq, "issuer": issuer, "sequence_last": last, "max_id": mx})
    snap["doc_key_scopes"] = scopes
    snap["doc_key_rows_est"] = int(q("SELECT reltuples FROM pg_class WHERE oid = 'etl_meta.doc_key'::regclass")[0][0])
    # последняя успешная публикация по целям (история control plane, UTC → Алматы)
    snap["last_success"] = {c: to_almaty(ts) for c, ts in q(
        "SELECT s.target_table, max(h.finished_at) FROM etl_meta.ch_sync s JOIN etl_meta.ch_sync_history h "
        "ON h.sync_id = s.id AND h.status = 'success' WHERE s.is_active GROUP BY 1")}
    snap["dim_registry_last_success"] = to_almaty(q(
        "SELECT max(h.finished_at) FROM etl_meta.load_history h JOIN etl_meta.registers r ON r.id = h.register_id "
        "WHERE r.pipeline_type = 'reference_dim' AND h.status = 'success'")[0][0])
    snap["cost_daily_days"] = [{"day": str(d), "time": to_almaty(t)} for d, t in q(
        "SELECT date(h.started_at), max(h.finished_at) FROM etl_meta.ch_sync_history h JOIN etl_meta.ch_sync s "
        "ON s.id = h.sync_id WHERE s.target_table = 'cost_daily' AND h.status = 'success' "
        "AND h.started_at > now() - interval '7 days' GROUP BY 1 ORDER BY 1")]
    conn.close()

    # ClickHouse — только system.tables / system.parts
    parts = {t: {"rows": int(r), "bytes": int(b), "compressed": int(c), "uncompressed": int(u), "max_partition": mp,
                 "max_time": mt, "max_date": md}
             for t, r, b, c, u, mp, mt, md in ch_query(
        f"SELECT table, sum(rows), sum(bytes_on_disk), sum(data_compressed_bytes), sum(data_uncompressed_bytes), "
        f"max(partition), toString(max(max_time)), toString(max(max_date)) FROM system.parts "
        f"WHERE active AND database = '{CH_DB}' GROUP BY table")}
    tables = []
    for name, engine, rows in ch_query(f"SELECT name, engine, toString(total_rows) FROM system.tables "
                                       f"WHERE database = '{CH_DB}' ORDER BY name"):
        p = parts.get(name, {})
        tables.append({"name": name, "engine": engine, "rows": p.get("rows", int(rows) if rows.isdigit() else 0),
                       "bytes": p.get("bytes", 0), "compressed": p.get("compressed", 0),
                       "uncompressed": p.get("uncompressed", 0), "max_partition": p.get("max_partition"),
                       "max_time": p.get("max_time"), "max_date": p.get("max_date")})
    cols: dict = {}
    for table, name, typ in ch_query(f"SELECT table, name, type FROM system.columns WHERE database = '{CH_DB}' "
                                     f"AND table NOT LIKE '%\\_stage' AND table NOT LIKE '%\\_raw' ORDER BY table, position"):
        cols.setdefault(table, []).append({"name": name, "type": typ})
    for t in tables:
        t["columns"] = cols.get(t["name"], [])
    snap["clickhouse"] = {"database": CH_DB, "tables": tables, "bytes": sum(t["bytes"] for t in tables)}

    # Airflow — метаданные DAG
    try:
        from airflow.models import DagModel, DagRun
        from airflow.settings import Session
        s = Session()
        dm = s.query(DagModel).filter(DagModel.dag_id == "analytics_sync").one()
        last_ok = (s.query(DagRun).filter(DagRun.dag_id == "analytics_sync", DagRun.state == "success")
                   .order_by(DagRun.end_date.desc()).first())
        snap["airflow"] = {"schedule": getattr(dm, "timetable_summary", None),
                           "schedule_text": getattr(dm, "timetable_description", None), "paused": dm.is_paused,
                           "last_success": to_almaty(last_ok.end_date) if last_ok else None}
    except Exception as e:  # noqa: BLE001
        snap["airflow"] = {"error": str(e)[:200]}
    from core.clickhouse.runner import SWEEP_HOUR
    snap["sweep_hour_almaty"] = SWEEP_HOUR
    return snap


def load_snapshot(offline: bool) -> dict:
    if not offline:
        try:
            snap = collect()
            GEN.parent.mkdir(parents=True, exist_ok=True)
            GEN.write_text(json.dumps(snap, ensure_ascii=False, indent=1, default=str))
            return snap
        except Exception as e:  # noqa: BLE001
            print(f"live snapshot недоступен: {type(e).__name__}: {str(e)[:200]}", file=sys.stderr)
    if GEN.exists():
        snap = json.loads(GEN.read_text())
        snap["live"] = False
        return snap
    raise SystemExit("нет ни live-доступа, ни сохранённого snapshot — сборка невозможна")


# ------------------------------------------------------------------ производные данные
def human(b) -> str:
    b = float(b or 0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if b < 1024 or unit == "TiB":
            return f"{b:.0f} {unit}" if unit == "B" else f"{b:.2f} {unit}"
        b /= 1024


def num(n) -> str:
    return f"{int(n or 0):,}".replace(",", " ")


def derive(snap: dict) -> dict:
    t = {x["name"]: x for x in snap["clickhouse"]["tables"]}
    for x in t.values():
        x["fks"] = [c["name"] for c in x.get("columns", []) if c["name"].endswith("_id") and c["name"] != "id"]
    sync_by_target = {s["target"]: s for s in snap["ch_sync"]}
    groups = {g["group"]: g for g in snap["groups"]}
    sweep = snap.get("sweep_hour_almaty")
    every = (snap.get("airflow") or {}).get("schedule_text") or "Every 5 minutes"

    def freq(target: str) -> str:
        s = sync_by_target.get(target)
        if not s:
            return "—"
        if s["load_mode"] == "document_patch":
            hot = "hot OFF" if s["hot_rebuild"] is False else "hot rebuild ежечасно"
            return f"patch каждые 5 мин · {hot} · sweep после {sweep:02d}:00"
        if target == "cost_daily":
            return "раз в сутки (источник обновляется ежедневно)"
        return "каждый цикл, полная замена при изменении"

    domains = []
    for name, tables in DOMAINS:
        rows = [t.get(x, {}) for x in tables]
        last = max((snap["last_success"].get(x) or "" for x in tables), default="") or None
        domains.append({"name": name, "tables": tables, "rows": sum(r.get("rows", 0) for r in rows),
                        "rows_by": {x: t.get(x, {}).get("rows", 0) for x in tables},
                        "bytes": sum(r.get("bytes", 0) for r in rows), "freq": freq(tables[0]), "last": last,
                        "group": (sync_by_target.get(tables[0]) or {}).get("group"),
                        "active": all(x in sync_by_target for x in tables)})
    dims = [x for x in snap["clickhouse"]["tables"] if x["name"].startswith("dim_") and not x["name"].endswith("_stage")]
    dim_last = max((snap["last_success"].get(x["name"]) or "" for x in dims), default="") or None
    domains.append({"name": "DIM", "tables": [d["name"] for d in dims], "rows": sum(d["rows"] for d in dims),
                    "rows_by": {d["name"]: d["rows"] for d in dims}, "bytes": sum(d["bytes"] for d in dims),
                    "freq": "реестр — каждые 5 мин (dim_registry); реплика — полная замена при изменении",
                    "last": max(filter(None, [dim_last, snap.get("dim_registry_last_success")]), default=None),
                    "group": "dim_registry → core_pg_to_ch", "active": bool(dims)})
    fact_rows = sum(d["rows"] for d in domains if d["name"] in ("Sales", "Orders", "Stock", "Cost Daily"))
    last_update = max(filter(None, (d["last"] for d in domains)), default=None)
    prod = [x for x in snap["clickhouse"]["tables"] if not x["name"].endswith(("_stage", "_raw"))]
    tech = [x for x in snap["clickhouse"]["tables"] if x["name"].endswith(("_stage", "_raw"))]
    regs = {r["code"]: r for r in snap["registers"]}
    group_last: dict = {}
    for s in snap["ch_sync"]:
        ts = snap["last_success"].get(s["target"])
        if ts and ts > group_last.get(s["group"], ""):
            group_last[s["group"]] = ts
    return {"domains": domains, "fact_rows": fact_rows, "last_update": last_update, "tables": t, "regs": regs, "group_last": group_last,
            "prod_tables": prod, "tech_tables": tech, "sync_by_target": sync_by_target, "groups_by": groups,
            "dims": dims}


# ------------------------------------------------------------------ сборка
def svg(name: str) -> str:
    return (ROOT / "static" / "svg" / f"{name}.svg").read_text()


PAGES = [("index.html", "index.html", "/", "01", "Как устроено"),
         ("operations.html", "operations/index.html", "/operations", "02", "Как сопровождать"),
         ("analytics.html", "analytics/index.html", "/analytics", "03", "Данные и отчёты")]


def build(snap: dict) -> None:
    from jinja2 import Environment, FileSystemLoader
    env = Environment(loader=FileSystemLoader(str(ROOT / "templates")),
                      trim_blocks=True, lstrip_blocks=True, autoescape=True)
    env.filters.update(human=human, num=num)
    env.globals.update(svg=svg)
    content = {p.stem: json.loads(p.read_text()) for p in (ROOT / "content").glob("*.json")}
    ctx = {"snap": snap, "d": derive(snap), "c": content, "built_at": now_almaty(),
           "nav": [{"href": h, "num": n, "title": t} for _, _, h, n, t in PAGES]}
    if DIST.exists():
        subprocess.run(["rm", "-rf", str(DIST)], check=True)
    index = []
    for tpl, out, href, n, title in PAGES:
        html = env.get_template(tpl).render(**ctx, current=href, page_title=title, page_num=n)
        dest = DIST / out
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(html)
        # индекс поиска: заголовки разделов (h2/h3 с id) всех страниц
        for tag, hid, text in re.findall(r'<(h[23]) id="([^"]+)"[^>]*>(.*?)</\1>', html, re.S):
            clean = re.sub(r"<[^>]+>", "", text).strip()
            index.append({"t": clean, "u": f"{href}#{hid}", "p": f"{n} {title}", "k": hid.replace("-", " ")})
    (DIST / "static").mkdir(parents=True)
    for f in ("site.css", "site.js"):
        (DIST / "static" / f).write_text((ROOT / "static" / f).read_text())
    (DIST / "static" / "search-index.js").write_text("window.SEARCH_INDEX = " + json.dumps(index, ensure_ascii=False) + ";\n")
    print(f"собрано: {', '.join(o for _, o, *_ in PAGES)} | snapshot {snap['captured_at']} "
          f"({'live' if snap.get('live') else 'FALLBACK'})")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--offline", action="store_true", help="не обращаться к production, взять последний snapshot")
    a = ap.parse_args()
    build(load_snapshot(a.offline))
    return 0


if __name__ == "__main__":
    sys.exit(main())
