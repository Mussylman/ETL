"""
Историческая загрузка склада в shadow ClickHouse — помесячно, с независимой сверкой 1С ↔ ClickHouse.

Для каждого месяца (от свежего к старому):
  1. rebuild группы shadow_stock (fact_stock_shadow + fact_stock_positions_shadow);
  2. сверка onec_reconcile (отпечаток строк, ссылки, меры);
  3. независимые проверки: документы и их множество, строки по виду движения, набор
     recorder+nomenklatura+sklad+kachestvo+movement_type+quantity+cost_amount, дубли,
     recorder ↔ id (doc_key), сироты, неразрешённые ссылки;
  4. повторный rebuild — партиция и реестр id не меняются (идемпотентность).
FAIL месяца — останов; успешные месяцы не переделываются (--resume продолжает после последнего PASS).

Запуск:
  PYTHONPATH=dags venv/bin/python3 ops/stock_history_load.py --from 202608 --to 201202 --out reports/stock_history_load.jsonl
"""
import argparse
import json
import sys
import time
from datetime import datetime

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from core.clickhouse import onec_reconcile as orc
from core.clickhouse.config import load_spec
from core.clickhouse.runner import run_group
from core.clickhouse.target import ClickHouse

GROUP = "shadow_stock"
HDR, POS = "analytics_poc.fact_stock_shadow", "analytics_poc.fact_stock_positions_shadow"
UTF8 = orc.UTF8
ZERO = "00000000-0000-0000-0000-000000000000"
REF_DIMS = [("nomenklatura", "dim_product"), ("sklad", "dim_warehouse"),
            ("kachestvo", "dim_quality"), ("kontragent", "dim_counterparty")]


def months(frm: str, to: str):
    y, m = int(frm[:4]), int(frm[4:])
    while f"{y:04d}{m:02d}" >= to:
        yield f"{y:04d}{m:02d}"
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)


def bounds(p: str):
    y, m = int(p[:4]), int(p[4:])
    a = f"{y + 2000:04d}-{m:02d}-01"
    b = f"{y + 2000 + (m == 12):04d}-{(1 if m == 12 else m + 1):02d}-01"
    return a, b


def h1c(canon: str) -> str:
    return (f"CONVERT(bigint, CONVERT(binary(4), SUBSTRING(HASHBYTES('MD5', "
            f"CAST(({canon}) COLLATE {UTF8} AS varchar(4000))), 1, 4)))")


def hch(canon: str) -> str:
    return f"sum(reinterpretAsUInt32(reverse(unhex(substring(lower(hex(MD5({canon}))),1,8)))))"


def u1c(col: str) -> str:
    return orc._uuid_1c(f"ISNULL({col}, 0x00000000000000000000000000000000)")


# строка регистра в каноническом виде: одинаково в 1С и ClickHouse
ROW_1C = ("CONCAT_WS('|', " + ", ".join([
    u1c("s._RecorderRRef"), "CAST(CAST(s._RecorderTRef AS bigint) AS varchar(20))",
    "CAST(CAST(s._LineNo AS bigint) AS varchar(20))",
    u1c("s._Fld17577RRef"), u1c("s._Fld17580RRef"), u1c("s._Fld17585RRef"),
    "CAST(CAST(s._RecordKind AS bigint) AS varchar(20))",
    "CAST(CAST(ROUND(s._Fld17586 * 10000, 0) AS bigint) AS varchar(30))",
    "CAST(CAST(ROUND(s._Fld17587 * 10000, 0) AS bigint) AS varchar(30))"]) + ")")
ROW_CH = ("concat(" + ", '|', ".join([
    "lower(toString(recorder))", "toString(recorder_type)", "toString(line_no)",
    "lower(toString(nomenklatura_guid))", "lower(toString(sklad_guid))", "lower(toString(kachestvo_guid))",
    "toString(movement_type)",
    "toString(toInt64(quantity * 10000))", "toString(toInt64(cost_amount * 10000))"]) + ")")


def stats_1c(ms, p):
    a, b = bounds(p)
    w = f"s._Period >= '{a}' AND s._Period < '{b}' AND s._Active = 0x01"
    rows = ms.get_records(f"""SELECT s._RecordKind, COUNT(*), ISNULL(SUM(CAST(s._Fld17586 AS decimal(38,4))),0),
        ISNULL(SUM(CAST(s._Fld17587 AS decimal(38,4))),0) FROM _AccumRg17576 s WITH (NOLOCK) WHERE {w}
        GROUP BY s._RecordKind""")
    by_mt = {int(r[0]): [int(r[1]), str(r[2]), str(r[3])] for r in rows}
    rh = ms.get_first(f"SELECT ISNULL(SUM(h),0) FROM (SELECT {h1c(ROW_1C)} h FROM _AccumRg17576 s WITH (NOLOCK) WHERE {w}) x")[0]
    docs = ms.get_first(f"""SELECT COUNT(*), ISNULL(SUM(h),0) FROM (SELECT {h1c(
        "CONCAT_WS('|', " + u1c('d.r') + ", CAST(CAST(d.t AS bigint) AS varchar(20)))")} h FROM
        (SELECT DISTINCT s._RecorderRRef r, s._RecorderTRef t FROM _AccumRg17576 s WITH (NOLOCK) WHERE {w}) d) x""")
    return {"by_movement_type": by_mt, "row_hash": int(rh), "docs": int(docs[0]), "doc_hash": int(docs[1])}


def stats_ch(ch, p):
    wp = f"toYYYYMM(period) = {int(p)}"
    by_mt = {}
    for line in ch.query(f"SELECT movement_type, count(), sum(toDecimal128(quantity,4)), sum(toDecimal128(cost_amount,4)) "
                         f"FROM {POS} WHERE {wp} GROUP BY movement_type").splitlines():
        mt, n, q, c = line.split("\t")
        by_mt[int(mt)] = [int(n), q, c]
    rh = int(ch.scalar(f"SELECT {hch(ROW_CH)} FROM {POS} WHERE {wp}"))
    doc_canon = "concat(lower(toString(recorder)), '|', toString(recorder_type))"
    d = ch.row(f"SELECT count(), {hch(doc_canon)} FROM {HDR} WHERE {wp}")
    return {"by_movement_type": by_mt, "row_hash": rh, "docs": int(d[0]), "doc_hash": int(d[1])}


def norm_mt(by_mt):
    from decimal import Decimal
    return {k: [v[0], format(Decimal(v[1]).normalize(), "f"), format(Decimal(v[2]).normalize(), "f")]
            for k, v in sorted(by_mt.items())}


def integrity(ch, pg, p):
    wp = f"toYYYYMM(period) = {int(p)}"
    r = {}
    r["dup_header_recorder"] = int(ch.scalar(f"SELECT count() FROM (SELECT recorder FROM {HDR} WHERE {wp} GROUP BY recorder HAVING count()>1)"))
    r["dup_header_id"] = int(ch.scalar(f"SELECT count() FROM (SELECT id FROM {HDR} GROUP BY id HAVING uniqExact(recorder, recorder_type)>1 AND has(groupArray(toYYYYMM(period)), {int(p)}))"))
    r["dup_positions_key"] = int(ch.scalar(f"SELECT count() FROM (SELECT recorder, recorder_type, line_no FROM {POS} WHERE {wp} GROUP BY 1,2,3 HAVING count()>1)"))
    r["orphan_stock_id"] = int(ch.scalar(f"SELECT count() FROM {POS} WHERE {wp} AND stock_id NOT IN (SELECT id FROM {HDR} WHERE {wp})"))
    r["stock_id_recorder_mismatch"] = int(ch.scalar(
        f"SELECT count() FROM {POS} p INNER JOIN (SELECT id, recorder, recorder_type FROM {HDR} WHERE {wp}) h ON h.id = p.stock_id "
        f"WHERE toYYYYMM(p.period) = {int(p)} AND (h.recorder != p.recorder OR h.recorder_type != p.recorder_type)"))
    r["headers_without_positions"] = int(ch.scalar(f"SELECT count() FROM {HDR} WHERE {wp} AND id NOT IN (SELECT stock_id FROM {POS} WHERE {wp})"))
    r["zero_id"] = int(ch.scalar(f"SELECT count() FROM {HDR} WHERE {wp} AND id = 0"))
    # recorder → id: ровно то, что в реестре doc_key (stock)
    hdr = [l.split("\t") for l in ch.query(f"SELECT lower(toString(recorder)), recorder_type, id FROM {HDR} WHERE {wp}").splitlines() if l]
    bad = 0
    for i in range(0, len(hdr), 20000):
        chunk = hdr[i:i + 20000]
        reg = {(str(a), int(t)): int(x) for a, t, x in pg.get_records(
            "SELECT recorder::text, recorder_type, id FROM etl_meta.doc_key WHERE doc_table='stock' AND recorder = ANY(%s::uuid[])",
            parameters=([c[0] for c in chunk],))}
        bad += sum(1 for a, t, x in chunk if reg.get((a, int(t))) != int(x))
    r["doc_key_mismatch"] = bad
    # ссылки: guid есть, id не разрешён (0) / id не существует в реестре справочника
    unres = {}
    for col, dim in REF_DIMS:
        n0 = int(ch.scalar(f"SELECT count() FROM {POS} WHERE {wp} AND {col}_id = 0 AND {col}_guid != toUUID('{ZERO}')"))
        ids = [int(x) for x in ch.query(f"SELECT DISTINCT {col}_id FROM {POS} WHERE {wp} AND {col}_id != 0").split()]
        have = {x for (x,) in pg.get_records(f"SELECT id FROM public.{dim} WHERE id = ANY(%s)", parameters=(ids,))} if ids else set()
        miss = len(set(ids) - have)
        stubs = int(pg.get_first(f"SELECT count(*) FROM public.{dim} WHERE is_stub AND id = ANY(%s)", parameters=(ids,))[0]) if ids else 0
        unres[col] = {"guid_without_id": n0, "id_not_in_registry": miss, "stub_ids": stubs}
    r["unresolved_refs"] = unres
    r["type_26908_docs"] = int(ch.scalar(f"SELECT count() FROM {HDR} WHERE {wp} AND recorder_type = 26908"))
    r["type_26908_basis_null"] = int(ch.scalar(f"SELECT count() FROM {HDR} WHERE {wp} AND recorder_type = 26908 AND document_basis_guid = toUUID('{ZERO}')"))
    return r


def content_hash(ch, p):
    """Всё содержимое партиции без технических меток времени — для идемпотентности."""
    wp = f"toYYYYMM(period) = {int(p)}"
    h = ch.row(f"SELECT count(), sum(cityHash64(id, recorder, recorder_type, period, document_date, document_basis_guid)) FROM {HDR} WHERE {wp}")
    cols = [l.split("\t")[0] for l in ch.query(f"DESCRIBE {POS}").splitlines()]
    cols = [c for c in cols if c not in ("source_updated_at", "loaded_at")]
    q = ch.row(f"SELECT count(), sum(cityHash64({', '.join(cols)})) FROM {POS} WHERE {wp}")
    return [h, q]


def stock_keys(pg):
    return pg.get_first("SELECT count(*), max(id) FROM etl_meta.doc_key WHERE doc_table='stock'")


def rebuild(p):
    t = time.time()
    rep = run_group(GROUP, mode="rebuild", partitions=[p], include_inactive=True, config_conn_id="etl_prod")
    return round(time.time() - t, 1), rep


def check_month(pg, ch, ms, pos_plan, pos_spec, p):
    res = {"partition": p, "started": datetime.now().isoformat(timespec="seconds")}
    fails = []
    try:
        res["rebuild_s"], rep = rebuild(p)
    except Exception as e:
        res["fail"] = [f"rebuild: {str(e)[:2000]}"]
        return res
    res["rebuild_report"] = {k: v for k, v in (rep.get("results") or [{}])[0].items() if k in ("outside_partition_docs", "failed")}

    d = orc.compare(orc.fingerprint_1c(ms, pos_plan, p), orc.fingerprint_ch(ch, pos_spec, pos_plan, p), pos_plan)
    res["onec_reconcile"] = "PASS" if not d else d
    if d:
        fails.append(f"onec_reconcile: {d[:5]}")

    s1, s2 = stats_1c(ms, p), stats_ch(ch, p)
    res["1c"] = {"docs": s1["docs"], "rows": sum(v[0] for v in s1["by_movement_type"].values()), "by_movement_type": norm_mt(s1["by_movement_type"])}
    res["ch"] = {"docs": s2["docs"], "rows": sum(v[0] for v in s2["by_movement_type"].values()), "by_movement_type": norm_mt(s2["by_movement_type"])}
    for k in ("docs", "rows", "by_movement_type"):
        if res["1c"][k] != res["ch"][k]:
            fails.append(f"{k}: 1С {res['1c'][k]} ≠ CH {res['ch'][k]}")
    if s1["doc_hash"] != s2["doc_hash"]:
        fails.append("множество recorder (recorder+recorder_type) 1С ≠ CH")
    if s1["row_hash"] != s2["row_hash"]:
        fails.append("набор recorder+nomenklatura+sklad+kachestvo+movement_type+quantity+cost_amount 1С ≠ CH")
    res["set_checks"] = {"recorder_set": s1["doc_hash"] == s2["doc_hash"], "row_set": s1["row_hash"] == s2["row_hash"]}

    integ = integrity(ch, pg, p)
    res["integrity"] = integ
    for k in ("dup_header_recorder", "dup_header_id", "dup_positions_key", "orphan_stock_id",
              "stock_id_recorder_mismatch", "headers_without_positions", "zero_id", "doc_key_mismatch"):
        if integ[k]:
            fails.append(f"{k} = {integ[k]}")
    for col, u in integ["unresolved_refs"].items():
        if u["guid_without_id"] or u["id_not_in_registry"]:
            fails.append(f"unresolved {col}: {u}")

    # идемпотентность: второй rebuild не меняет ни партицию, ни реестр id
    before, keys_before = content_hash(ch, p), stock_keys(pg)
    try:
        res["rebuild2_s"], _ = rebuild(p)
    except Exception as e:
        fails.append(f"rebuild2: {str(e)[:1000]}")
    else:
        after, keys_after = content_hash(ch, p), stock_keys(pg)
        res["idempotent"] = before == after and keys_before == keys_after
        if not res["idempotent"]:
            fails.append(f"идемпотентность: {before}/{keys_before} → {after}/{keys_after}")
    res["fail"] = fails
    res["finished"] = datetime.now().isoformat(timespec="seconds")
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="frm", required=True)
    ap.add_argument("--to", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    pg = PostgresHook(postgres_conn_id="etl_prod")
    ch = ClickHouse("clickhouse_etl")
    ms = MsSqlHook(mssql_conn_id="mssql_1c_conn")
    pos_spec = load_spec(pg, "fact_stock_positions_shadow")
    if (pos_spec.source_params or {}).get("history_from"):
        sys.exit("history_from ещё задан — историческая загрузка запрещена")
    pos_plan = orc.plan(pg, pos_spec)

    done = set()
    try:
        with open(args.out) as f:
            done = {json.loads(l)["partition"] for l in f if l.strip() and not json.loads(l).get("fail")}
    except FileNotFoundError:
        pass

    for p in months(args.frm, args.to):
        if p in done:
            continue
        res = check_month(pg, ch, ms, pos_plan, pos_spec, p)
        with open(args.out, "a") as f:
            f.write(json.dumps(res, ensure_ascii=False, default=str) + "\n")
        print(f"{p}: {'FAIL' if res['fail'] else 'PASS'} docs={res.get('ch', {}).get('docs')} "
              f"rows={res.get('ch', {}).get('rows')} {res.get('rebuild_s')}s+{res.get('rebuild2_s')}s", flush=True)
        if res["fail"]:
            print("ОСТАНОВ:", *res["fail"], sep="\n  ", flush=True)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
