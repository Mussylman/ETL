"""
Перевод shadow-группы ClickHouse в боевую — по конфигурации, без зашитых регистров.

Что делает cutover (группа shadow-конфигураций → боевая группа):
  1. проверки (gates) на shadow: состав и имена, схема таблиц = конфигурация, дубли бизнес-ключа,
     not_zero, сироты родитель → потомок, recorder → id = реестр doc_key, сверка 1С ↔ ClickHouse;
  2. ClickHouse (ch_admin): боевые таблицы (цель, staging, raw) из той же конфигурации, партиции
     переносятся из shadow через REPLACE PARTITION ... FROM — жёсткие ссылки, без перезагрузки и
     без копирования через PostgreSQL; содержимое сверяется с shadow; права etl_writer;
  3. control plane (одна транзакция): боевые ch_sync + колонки — копия shadow с боевыми именами,
     shadow=false, свой state_key, отметка источника, боевая группа включена.
Shadow-таблицы и shadow-конфигурации не меняются — это точка отката.

rollback: боевая группа и её конфигурации выключаются; таблицы, история, реестр id не трогаются.

Повтор безопасен: уже перенесённое (боевые конфигурации есть) повторно не копируется, совпадающее
не переписывается. Без --apply — только план и проверки.

    PYTHONPATH=dags python3 -m core.tools.ch_promote --conn etl_prod cutover --group shadow_x \
        --live-group onec_x --dag analytics_sync --position 16 --watermark '2026-10-06 12:00:00' [--param hot_rebuild=false] [--apply]
    PYTHONPATH=dags python3 -m core.tools.ch_promote --conn etl_prod rollback --live-group onec_x [--apply]
"""
import argparse
import dataclasses
import json
import os
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, __file__.rsplit("/core/", 1)[0])
from core.conn import require_conn                       # noqa: E402

ADMIN_CFG = os.path.expanduser("~/.config/clickhouse/ch_admin.xml")
BUCKETS = 16
TECH_COLUMNS = ("loaded_at",)          # метка загрузки — единственное, что может отличаться между копиями


class Stop(Exception):
    """Gate не пройден — ничего не изменено."""


def ch_admin(cfg: str, sql: str) -> str:
    r = subprocess.run(["clickhouse-client", "--config-file", cfg, "--multiquery"], input=sql,
                       capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"ClickHouse: {r.stderr.strip()[:500]}")
    return r.stdout


def live_name(name: str, suffix: str) -> str:
    if not name.endswith(suffix):
        raise Stop(f"{name!r} не оканчивается на {suffix!r} — боевое имя не выводится, задайте конфигурации явно")
    return name[: -len(suffix)]


# ------------------------------------------------------------------ план
@dataclasses.dataclass
class Pair:
    shadow: object          # SyncSpec
    live: object            # SyncSpec с боевыми именами (для DDL)
    live_params: Dict


def build_pairs(pg, group: str, suffix: str, params: Dict, live_state_key: Optional[str]) -> List[Pair]:
    from core.clickhouse.config import load_group_specs
    specs = load_group_specs(pg, group, include_inactive=True)
    if not specs:
        raise Stop(f"в группе {group!r} нет конфигураций")
    out = []
    for s in specs:
        p = dict(s.source_params or {})
        if not p.get("shadow"):
            raise Stop(f"{s.code}: source_params.shadow не true — это не shadow-конфигурация")
        lp = dict(p)
        lp["shadow"] = False
        if p.get("state_key"):
            lp["state_key"] = live_state_key or (p["state_key"][: -len(":shadow")] if p["state_key"].endswith(":shadow")
                                                 else p["state_key"])
        lp.update(params)
        live = dataclasses.replace(s, code=live_name(s.code, suffix), target_table=live_name(s.target_table, suffix),
                                   source_params=lp)
        out.append(Pair(s, live, lp))
    keys = {p.live_params.get("state_key") for p in out}
    if len(keys) > 1:
        raise Stop(f"у конфигураций группы разные state_key: {keys}")
    return out


# ------------------------------------------------------------------ gates
def describe(ch, fqn: str) -> List[Tuple[str, str]]:
    return [tuple(l.split("\t")[:2]) for l in ch.query(f"DESCRIBE TABLE {fqn}").splitlines() if l]


def schema_gate(ch, pairs: List[Pair]) -> List[str]:
    bad = []
    for p in pairs:
        s = p.shadow
        if not ch.table_exists(s.fqn):
            bad.append(f"{s.code}: нет shadow-таблицы {s.fqn}")
            continue
        phys = describe(ch, s.fqn)
        conf = [(c.target_column, c.target_type) for c in s.columns]
        norm = lambda t: t.replace(" ", "")
        if [(n, norm(t)) for n, t in phys] != [(n, norm(t)) for n, t in conf]:
            bad.append(f"{s.code}: схема {s.fqn} ≠ конфигурации: {sorted(set(phys) ^ set(conf))[:6]}")
    return bad


def _bucketed(ch, sql_tpl: str) -> int:
    return sum(int(ch.scalar(sql_tpl.format(b=b, n=BUCKETS))) for b in range(BUCKETS))


def integrity_gate(ch, pg, pairs: List[Pair]) -> Tuple[List[str], Dict]:
    bad, info = [], {}
    by_target = {p.shadow.source_params.get("target"): p.shadow for p in pairs}
    for p in pairs:
        s = p.shadow
        info[s.code] = {"rows": int(ch.scalar(f"SELECT count() FROM {s.fqn}"))}
        if s.business_key:
            k = ", ".join(s.business_key)
            d = _bucketed(ch, f"SELECT count() FROM (SELECT {k} FROM {s.fqn} WHERE cityHash64({k}) % {{n}} = {{b}} "
                              f"GROUP BY {k} HAVING count() > 1)")
            info[s.code]["dup_business_key"] = d
            if d:
                bad.append(f"{s.code}: дублей бизнес-ключа {d}")
        for c in (s.reconcile_metrics or {}).get("not_zero", []):
            z = int(ch.scalar(f"SELECT count() FROM {s.fqn} WHERE {c} = 0"))
            info[s.code][f"zero_{c}"] = z
            if z:
                bad.append(f"{s.code}: {c} = 0 у {z} строк")
        parent = by_target.get((s.source_params or {}).get("parent"))
        if parent:
            fk = next((c.target_column for c in s.columns if c.source_expr == "hdr_id"), None)
            pk = next((c.target_column for c in parent.columns if c.source_expr == "id"), None)
            if fk and pk:
                o = _bucketed(ch, f"SELECT count() FROM {s.fqn} WHERE {fk} % {{n}} = {{b}} AND {fk} NOT IN "
                                  f"(SELECT {pk} FROM {parent.fqn} WHERE {pk} % {{n}} = {{b}})")
                info[s.code]["orphans"] = o
                if o:
                    bad.append(f"{s.code}: строк без шапки {o}")
        if (s.source_params or {}).get("own_id"):
            scope = s.source_params.get("target")
            idc = next((c.target_column for c in s.columns if c.source_expr == "id"), None)
            if idc and pg.get_first("SELECT 1 FROM etl_meta.doc_key_scope WHERE doc_table = %s", parameters=(scope,)):
                reg = {(str(a), int(t)): int(i) for a, t, i in pg.get_records(
                    "SELECT recorder::text, recorder_type, id FROM etl_meta.doc_key WHERE doc_table = %s", parameters=(scope,))}
                mism = 0
                rows = ch.query(f"SELECT lower(toString(recorder)), recorder_type, {idc} FROM {s.fqn}").splitlines()
                for l in rows:
                    a, t, i = l.split("\t")
                    mism += reg.get((a, int(t))) != int(i)
                info[s.code]["doc_key_mismatch"] = mism
                if mism:
                    bad.append(f"{s.code}: id ≠ реестру doc_key у {mism} документов")
    return bad, info


def open_partition(ch, spec) -> Optional[str]:
    """Текущая (открытая) партиция по бизнес-времени — partition_expr конфигурации, вычисленный на «сейчас»."""
    if not spec.partition_column or spec.partition_expr in ("tuple()", ""):
        return None
    now = "now('Asia/Almaty')"           # бизнес-время (CLAUDE.md: period — дата Almaty)
    return ch.scalar(f"SELECT toString({spec.partition_expr.replace(spec.partition_column, now)})")


def _num(v) -> str:
    from decimal import Decimal
    d = Decimal(str(v if v is not None else 0)).normalize()
    return format(abs(d) if d == 0 else d, "f")


def doc_fingerprints_1c(ms, p: Dict, partition: str, doc_idx: List[int]) -> Dict[tuple, tuple]:
    """Отпечаток каждого документа партиции в 1С — те же выражения, что у onec_reconcile, сгруппированные по документу."""
    from core.clickhouse import onec_reconcile as orc
    y, m = int(partition[:4]), int(partition[4:])
    a = f"{y + 2000:04d}-{m:02d}-01"
    b = f"{y + 2000 + (m == 12):04d}-{(1 if m == 12 else m + 1):02d}-01"
    out: Dict[tuple, list] = {}
    for mb in p["members"]:
        canon = "CONCAT_WS('|', " + ", ".join(mb["keys"] + mb["refs"]) + ")"
        inner = [f"{mb['keys'][i]} AS k{j}" for j, i in enumerate(doc_idx)]
        inner += [f"CONVERT(bigint, CONVERT(binary(4), SUBSTRING(HASHBYTES('MD5', "
                  f"CAST(({canon}) COLLATE {orc.UTF8} AS varchar(4000))), 1, 4))) AS h"]
        inner += [f"CAST({e} AS decimal(38,8)) AS m{i}" for i, e in enumerate(mb["measures"])]
        ks = ", ".join(f"k{j}" for j in range(len(doc_idx)))
        meas = "".join(f", ISNULL(SUM(m{i}),0), ISNULL(SUM(m{i}*m{i}),0)" for i in range(len(mb["measures"])))
        cond = [f"{mb['period']} >= '{a}'", f"{mb['period']} < '{b}'"] + [f"({w})" for w in mb["where"]]
        sql = (f"SELECT {ks}, COUNT(*), ISNULL(SUM(h),0){meas} FROM (SELECT {', '.join(inner)} FROM {mb['from']} "
               f"WHERE {' AND '.join(cond)}) x GROUP BY {ks}")
        for r in ms.get_records(sql):
            key, vals = tuple(str(x) for x in r[:len(doc_idx)]), [x for x in r[len(doc_idx):]]
            prev = out.get(key)
            out[key] = vals if prev is None else [u + v for u, v in zip(prev, vals)]
    return {k: tuple(_num(x) for x in v) for k, v in out.items()}


def doc_fingerprints_ch(ch, spec, p: Dict, partition: str, doc_keys: List[str]) -> Dict[tuple, tuple]:
    parts, joins = [], []
    for k in p["keys"]:
        parts.append(f"lower(toString(f.{k}))" if k == "recorder" else f"toString(f.{k})")
    for i, r in enumerate(p["refs"]):
        joins.append(f"LEFT JOIN {spec.target_database}.{r['dim']} d{i} ON d{i}.id = f.{r['col']}")
        parts.append(f"lower(toString(d{i}.guid))")
    canon = "concat(" + ", '|', ".join(parts) + ")"
    ks = ", ".join(f"lower(toString(f.{k}))" if k == "recorder" else f"toString(f.{k})" for k in doc_keys)
    meas = "".join(f", sum(f.{m}), sum(toDecimal128(f.{m},8)*toDecimal128(f.{m},8))" for m in p["measures"])
    sql = (f"SELECT {ks}, count(), sum(reinterpretAsUInt32(reverse(unhex(substring(lower(hex(MD5({canon}))),1,8))))){meas} "
           f"FROM {spec.fqn} f {' '.join(joins)} WHERE {spec.partition_expr.replace(spec.partition_column, 'f.' + spec.partition_column)} "
           f"= {int(partition)} GROUP BY {ks}")
    out = {}
    for line in ch.query(sql).splitlines():
        v = line.split("\t")
        out[tuple(v[:len(doc_keys)])] = tuple(_num(x) for x in v[len(doc_keys):])
    return out


def open_partition_diff(ms, ch, spec, p: Dict, partition: str) -> Dict:
    """
    Открытая партиция: shadow — подмножество актуального источника. Документы shadow совпадают с
    источником построчно; разница допустима только из новых документов источника (появились после
    пересборки). Исчезнувший, изменённый или лишний документ shadow — FAIL.
    """
    from core.clickhouse import onec_reconcile as orc
    doc_keys = [k for k in orc.KEY_ORDER[:2] if k in p["keys"]]
    idx = [p["keys"].index(k) for k in doc_keys]
    src = doc_fingerprints_1c(ms, p, partition, idx)
    dst = doc_fingerprints_ch(ch, spec, p, partition, doc_keys)
    new = [k for k in src if k not in dst]
    missing = [k for k in dst if k not in src]
    changed = [k for k in dst if k in src and src[k] != dst[k]]
    moved = []
    if missing:
        # не в этой партиции источника: перенесён в другую (лишний в shadow) или исчез совсем
        mb = p["members"][0]
        for i in range(0, len(missing), 500):
            lst = ", ".join("'" + k[0] + "'" for k in missing[i:i + 500])
            moved += [(str(r[0]),) for r in ms.get_records(
                f"SELECT DISTINCT {mb['keys'][idx[0]]} FROM {mb['from']} WHERE {' AND '.join('(' + w + ')' for w in mb['where']) or '1=1'} "
                f"AND {mb['keys'][idx[0]]} IN ({lst})")]
    moved_set = {m[0] for m in moved}
    deleted = [k for k in missing if k[0] not in moved_set]
    extra = [k for k in missing if k[0] in moved_set]
    return {"partition": partition, "source_docs": len(src), "shadow_docs": len(dst),
            "source_new": len(new), "source_new_rows": sum(int(src[k][0]) for k in new),
            "changed": len(changed), "deleted": len(deleted), "shadow_extra": len(extra),
            "examples": {"changed": changed[:3], "deleted": deleted[:3], "shadow_extra": extra[:3]},
            "ok": not changed and not deleted and not extra}


def open_partitions(ch, spec, parts: List[str], window: int) -> List[str]:
    """
    Операционно открытые партиции: текущая по partition_expr и window-1 предыдущих по календарю
    (partition_granularity конфигурации), если они есть в таблице. В них источник ещё дописывает
    документы задним числом (начало месяца — прошлый месяц открыт).
    """
    if not spec.partition_column or spec.partition_expr in ("tuple()", "") or window < 1:
        return []
    unit = {"month": "MONTH", "day": "DAY"}.get(spec.partition_granularity or "")
    if unit is None:
        return []
    exprs = [spec.partition_expr.replace(spec.partition_column, f"now('Asia/Almaty') - INTERVAL {k} {unit}")
             for k in range(window)]
    cal = set(ch.query("SELECT arrayJoin([" + ", ".join(f"toString({e})" for e in exprs) + "])").split())
    return sorted(p for p in parts if p in cal)


def reconcile_gate(pg, ch, pairs: List[Pair], open_window: int = 1) -> Tuple[Dict, Dict]:
    """
    Сверка с источником всех партиций shadow: закрытые — строго 1:1 (отпечаток onec_reconcile),
    операционно открытые (последние open_window, см. open_partitions) — shadow ⊆ источника
    (open_partition_diff: существующие документы совпадают построчно, допустимы только новые).
    """
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    from core.clickhouse import onec_reconcile as orc
    closed, opened = {}, {}
    for s in [p.shadow for p in pairs if p.shadow.source_type == "onec_register"]:
        try:
            plan = orc.plan(pg, s)
        except RuntimeError:
            continue                      # шапка из строк регистра — сверяется через строки
        ms = MsSqlHook(mssql_conn_id=s.source_conn_id)
        parts = sorted(x for x in ch.query(f"SELECT DISTINCT {s.partition_expr} FROM {s.fqn}").split() if x)
        ops = set(open_partitions(ch, s, parts, open_window))
        for part in parts:
            if orc.prehistory(plan, part):
                continue
            if part in ops:
                opened[f"{s.code}/{part}"] = open_partition_diff(ms, ch, s, plan, part)
                continue
            d = orc.compare(orc.fingerprint_1c(ms, plan, part), orc.fingerprint_ch(ch, s, plan, part), plan)
            if d:
                closed.setdefault(part, []).append((s.code, d[:3]))
    return closed, opened


# ------------------------------------------------------------------ ClickHouse
def content_fp(cfg: str, fqn: str) -> str:
    cols = [l.split("\t")[0] for l in ch_admin(cfg, f"DESCRIBE TABLE {fqn}").splitlines() if l]
    cols = [c for c in cols if c not in TECH_COLUMNS]
    return ch_admin(cfg, f"SELECT count(), sum(cityHash64({', '.join(cols)})) FROM {fqn}").strip()


def table_keys(cfg: str, fqn: str) -> Tuple[str, str, str]:
    """(engine, partition_key, sorting_key) — то, что должно совпасть, чтобы REPLACE PARTITION был допустим."""
    db, t = fqn.split(".", 1)
    out = ch_admin(cfg, f"SELECT engine, partition_key, sorting_key FROM system.tables "
                        f"WHERE database = '{db}' AND name = '{t}' FORMAT TSV").strip()
    return tuple(out.split("\t")) if out else ("", "", "")


def partitions(cfg: str, fqn: str) -> List[str]:
    db, t = fqn.split(".", 1)
    return [x for x in ch_admin(cfg, f"SELECT DISTINCT partition_id FROM system.parts WHERE active AND "
                                     f"database = '{db}' AND table = '{t}' ORDER BY partition_id").split() if x]


def replace_gate(cfg: str, ch, pairs: List[Pair]) -> List[str]:
    """Shadow-таблица описана той же конфигурацией, что и боевая: ключи партиционирования и сортировки = конфиг."""
    bad = []
    for p in pairs:
        s = p.shadow
        eng, pk, sk = table_keys(cfg, s.fqn)
        want_sk = ", ".join(s.order_by)
        norm = lambda x: x.replace(" ", "")
        if eng != "MergeTree" or norm(pk) != norm(s.partition_expr) or norm(sk) != norm(want_sk):
            bad.append(f"{s.fqn}: engine/partition/order = ({eng}, {pk}, {sk}) ≠ конфигурации "
                       f"(MergeTree, {s.partition_expr}, {want_sk})")
    return bad


def promote_tables(cfg: str, ch, pairs: List[Pair], apply: bool) -> List[str]:
    """
    Боевые таблицы и перенос партиций. Повтор после сбоя на стадии control plane безопасен:
    боевая таблица, совпадающая с shadow по содержимому, повторно не переносится.
    """
    from core.tools.ch_ddl import grants
    steps = []
    for p in pairs:
        s, l = p.shadow, p.live
        tables = [(l.fqn, l.ddl(l.fqn)), (l.stage_fqn, l.ddl(l.stage_fqn))]
        if l.needs_raw:
            tables.append((l.raw_fqn, l.ddl_raw()))
        for fqn, ddl in tables:
            if not ch.table_exists(fqn):
                steps.append(f"CREATE TABLE {fqn} (DDL из конфигурации {s.code}, имя {l.target_table})")
                if apply:
                    ch_admin(cfg, ddl + ";")
        if ch.table_exists(l.fqn):
            if describe(ch, l.fqn) != describe(ch, s.fqn):
                raise Stop(f"{l.fqn}: колонки боевой таблицы ≠ shadow {s.fqn}")
            if table_keys(cfg, l.fqn) != table_keys(cfg, s.fqn):
                raise Stop(f"{l.fqn}: engine/partition/order ≠ shadow: {table_keys(cfg, l.fqn)} ≠ {table_keys(cfg, s.fqn)}")
        parts = partitions(cfg, s.fqn)
        live_rows = int(ch.scalar(f"SELECT count() FROM {l.fqn}")) if ch.table_exists(l.fqn) else 0
        if live_rows:
            a, b = content_fp(cfg, s.fqn), content_fp(cfg, l.fqn)
            if a != b:
                raise Stop(f"{l.fqn} уже содержит {live_rows} строк, отличных от shadow, а боевой конфигурации нет — "
                           f"перенос не выполняется (discard удалит неиспользуемые боевые таблицы)")
            steps.append(f"{l.fqn}: уже = shadow ({a.replace(chr(9), ' / ')}) — перенос не повторяется")
        else:
            steps.append(f"{s.fqn} → {l.fqn}: ALTER TABLE … REPLACE PARTITION ID … FROM × {len(parts)} (жёсткие ссылки)")
            if apply:
                for i in range(0, len(parts), 50):
                    ch_admin(cfg, ";\n".join(f"ALTER TABLE {l.fqn} REPLACE PARTITION ID '{x}' FROM {s.fqn}"
                                              for x in parts[i:i + 50]) + ";")
        if apply:
            a, b = content_fp(cfg, s.fqn), content_fp(cfg, l.fqn)
            pl = partitions(cfg, l.fqn)
            if a != b or pl != parts:
                raise Stop(f"{l.fqn}: после переноса ≠ shadow (строки/хеш {b} ≠ {a}, партиций {len(pl)} ≠ {len(parts)})")
            steps.append(f"{l.fqn}: = shadow — строк/хеш всех колонок с id {a.replace(chr(9), ' / ')}, партиций {len(pl)}")
            ch_admin(cfg, grants(l))
        steps.append(f"GRANT etl_writer: {l.stage_fqn} (TRUNCATE, MOVE PARTITION), {l.fqn} (ALTER DELETE)"
                     + (f", {l.raw_fqn} (TRUNCATE)" if l.needs_raw else ""))
    return steps


def rollback_plan(pairs: List[Pair], live_group: str) -> List[str]:
    return [f"ch_promote rollback --live-group {live_group} --apply:",
            f"  ch_sync_group {live_group}: is_active → false (DAG её больше не ведёт)"] + \
           [f"  ch_sync {p.live.code}: is_active → false ({p.live.fqn} остаётся)" for p in pairs] + \
           ["  shadow-группа, shadow-конфигурации и shadow-таблицы не менялись cutover'ом — остаются как были",
            "  doc_key, ch_sync_history, ch_source_state, остальные группы — не трогаются",
            "  полный возврат к shadow: после rollback — discard (удалить неиспользуемые боевые таблицы) по решению"]


# ------------------------------------------------------------------ control plane
def switch_config(conn_id: str, pairs: List[Pair], *, live_group: str, dag: str, position: int, description: str,
                  watermark: datetime, apply: bool) -> List[str]:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2.extras import Json
    c = PostgresHook(postgres_conn_id=conn_id).get_conn()
    c.autocommit = False
    cur = c.cursor()
    steps = []
    try:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('etl_meta.ch_config'))")
        cols = ("description, source_conn_id, source_type, source_schema, source_object, source_query, target_database, "
                "partition_expr, order_by, load_mode, partition_column, partition_granularity, watermark_column, business_key, "
                "batch_size, empty_partition_policy, hot_window, sweep_interval_min, checksum_columns, measure_columns, "
                "reconcile_metrics, priority, lookup")
        for p in pairs:
            cur.execute("SELECT id FROM etl_meta.ch_sync WHERE code = %s", (p.live.code,))
            if cur.fetchone():
                raise Stop(f"конфигурация {p.live.code} уже есть")
            cur.execute(f"""INSERT INTO etl_meta.ch_sync (code, target_table, source_params, sync_group, is_active, {cols})
                            SELECT %s, %s, %s, %s, true, {cols} FROM etl_meta.ch_sync WHERE code = %s RETURNING id""",
                        (p.live.code, p.live.target_table, Json(p.live_params), live_group, p.shadow.code))
            nid = cur.fetchone()[0]
            cur.execute("UPDATE etl_meta.ch_sync SET description = 'LIVE ' || regexp_replace(coalesce(description,''), "
                        "'^SHADOW\\s*', '') || ' [из ' || %s || ']' WHERE id = %s", (p.shadow.code, nid))
            cur.execute("""INSERT INTO etl_meta.ch_sync_columns (sync_id, ordinal, source_expr, target_column, target_type, codec)
                           SELECT %s, ordinal, source_expr, target_column, target_type, codec FROM etl_meta.ch_sync_columns
                           WHERE sync_id = %s""", (nid, p.shadow.id))
            steps.append(f"ch_sync +{p.live.code} → {p.live.fqn} (группа {live_group}, active, копия {p.shadow.code})")
        key = pairs[0].live_params.get("state_key")
        if key:
            cur.execute("""INSERT INTO etl_meta.ch_source_state (source_key, watermark, last_to_ts, updated_at, details)
                           VALUES (%s, %s, %s, now(), %s) ON CONFLICT (source_key) DO NOTHING RETURNING source_key""",
                        (key, watermark, watermark, Json({"from": "ch_promote", "shadow_group": pairs[0].shadow.source_params.get("state_key")})))
            steps.append(f"ch_source_state {key}: watermark {watermark}" if cur.fetchone() else
                         f"ch_source_state {key}: уже есть — не меняется")
        cur.execute("""INSERT INTO etl_meta.ch_sync_group (sync_group, dag_id, position, is_active, runner, description)
                       VALUES (%s, %s, %s, true, 'ch_sync', %s)
                       ON CONFLICT (sync_group) DO UPDATE SET dag_id = EXCLUDED.dag_id, position = EXCLUDED.position,
                              is_active = true, updated_at = now()""", (live_group, dag, position, description))
        steps.append(f"ch_sync_group {live_group}: dag {dag}, position {position}, active")
        if apply:
            c.commit()
        else:
            c.rollback()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    return steps


def live_state(pg, pairs: List[Pair]) -> str:
    """none — боевых конфигураций нет; done — все есть и включены; partial — иначе (стоп)."""
    rows = {r[0]: r[1] for r in pg.get_records("SELECT code, is_active FROM etl_meta.ch_sync WHERE code = ANY(%s)",
                                               parameters=([p.live.code for p in pairs],))}
    if not rows:
        return "none"
    if len(rows) == len(pairs) and all(rows.values()):
        return "done"
    return "partial"


# ------------------------------------------------------------------ команды
def cmd_cutover(a) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from core.clickhouse.target import ClickHouse
    pg = PostgresHook(postgres_conn_id=a.conn)
    ch = ClickHouse(a.ch_conn)
    params = {}
    for kv in a.param or []:
        k, v = kv.split("=", 1)
        params[k] = json.loads(v)
    pairs = build_pairs(pg, a.group, a.suffix, params, a.state_key)
    g = pg.get_first("SELECT is_active, dag_id FROM etl_meta.ch_sync_group WHERE sync_group = %s", parameters=(a.group,))
    if g and g[0] and g[1] != "none":
        raise Stop(f"shadow-группа {a.group} включена в DAG {g[1]} — сначала выключить (две записи в один источник)")
    print("план:")
    for p in pairs:
        print(f"   {p.shadow.code} ({p.shadow.fqn}) → {p.live.code} ({p.live.fqn}); source_params: "
              f"{json.dumps({k: p.live_params[k] for k in p.live_params if k in ('shadow', 'state_key', *params)}, ensure_ascii=False)}")

    state = live_state(pg, pairs)
    if state == "done":
        print("уже переключено: боевые конфигурации есть и включены — повтор ничего не делает")
        return 0
    if state == "partial":
        raise Stop("боевые конфигурации есть частично или выключены — разобрать вручную (rollback / ch_config)")

    if not os.path.exists(a.ch_config):
        raise Stop(f"нет конфигурации ch_admin {a.ch_config} (system.tables/parts, DDL, перенос партиций)")
    print("gates:")
    problems = schema_gate(ch, pairs)
    print(f"   колонки shadow = конфигурация: {'PASS' if not problems else problems}")
    rk = replace_gate(a.ch_config, ch, pairs)
    print(f"   engine/partition/order shadow = конфигурация (REPLACE PARTITION допустим): {'PASS' if not rk else rk}")
    problems += rk
    bad, info = integrity_gate(ch, pg, pairs)
    print(f"   целостность: {'PASS' if not bad else bad}  {json.dumps(info, ensure_ascii=False)}")
    problems += bad
    if not a.skip_reconcile:
        closed, opened = reconcile_gate(pg, ch, pairs, a.open_window)
        print(f"   сверка закрытых партиций с источником (строго 1:1; открытых — последние {a.open_window}): "
              f"{'PASS' if not closed else {k: v[0][1][:2] for k, v in closed.items()}}")
        problems += [f"закрытая партиция {k} ≠ источнику" for k in closed]
        for code, r in opened.items():
            print(f"   открытая партиция {code} (shadow ⊆ источника): {'PASS' if r['ok'] else 'FAIL'} "
                  f"changed={r['changed']} deleted={r['deleted']} shadow_extra={r['shadow_extra']} "
                  f"source_new={r['source_new']} (строк {r['source_new_rows']}); документов shadow {r['shadow_docs']}, "
                  f"источника {r['source_docs']}" + ("" if r["ok"] else f"; примеры {r['examples']}"))
            if not r["ok"]:
                problems.append(f"открытая партиция {r['partition']}: изменены/исчезли/лишние документы shadow")
    if problems:
        print("ОСТАНОВ: gates не пройдены — ничего не изменено")
        return 1

    wm = datetime.strptime(a.watermark, "%Y-%m-%d %H:%M:%S")
    mark = "✓" if a.apply else "[plan]"
    print("\nCUTOVER PLAN" + ("" if a.apply else " (dry-run: ClickHouse и control plane не меняются)"))
    print(" 1. ClickHouse (ch_admin):")
    for s in promote_tables(a.ch_config, ch, pairs, a.apply):
        print(f"    {mark} {s}")
    print(" 2. control plane (одна транзакция; без --apply выполняется и откатывается):")
    for s in switch_config(a.conn, pairs, live_group=a.live_group, dag=a.dag, position=a.position,
                           description=a.description or f"боевая группа из {a.group}", watermark=wm, apply=a.apply):
        print(f"    {mark} {s}")
    print(" 3. ROLLBACK PLAN:")
    for s in rollback_plan(pairs, a.live_group):
        print(f"    {s}")
    print("ПРИМЕНЕНО" if a.apply else "DRY-RUN: ничего не изменено (для выполнения --apply)")
    return 0


def cmd_discard(a) -> int:
    """Удалить боевые таблицы, которые не описаны ни одной конфигурацией (незавершённый или отменённый перенос)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from core.clickhouse.target import ClickHouse
    pg = PostgresHook(postgres_conn_id=a.conn)
    ch = ClickHouse(a.ch_conn)
    pairs = build_pairs(pg, a.group, a.suffix, {}, None)
    used = pg.get_records("SELECT code FROM etl_meta.ch_sync WHERE code = ANY(%s) OR "
                          "(target_database, target_table) IN (SELECT unnest(%s::text[]), unnest(%s::text[]))",
                          parameters=([p.live.code for p in pairs], [p.live.target_database for p in pairs],
                                      [p.live.target_table for p in pairs]))
    if used:
        raise Stop(f"боевые таблицы описаны конфигурациями {[u[0] for u in used]} — discard запрещён")
    drops = [f for p in pairs for f in (p.live.fqn, p.live.stage_fqn, p.live.raw_fqn) if ch.table_exists(f)]
    print("DISCARD:" if a.apply else "DISCARD (dry-run):")
    for f in drops:
        print(f"   DROP TABLE {f}")
        if a.apply:
            ch_admin(a.ch_config, f"DROP TABLE {f};")
    if drops:
        # ClickHouse не отзывает права при DROP TABLE — отзываем те же, что выдал cutover
        from core.tools.ch_ddl import grants
        revoke = "\n".join(g.replace("GRANT ", "REVOKE ", 1).replace(" TO etl_writer;", " FROM etl_writer;")
                           for p in pairs for g in grants(p.live).splitlines())
        print("   " + revoke.replace("\n", "\n   "))
        if a.apply:
            ch_admin(a.ch_config, revoke)
    else:
        print("   боевых таблиц нет — нечего удалять")
    print("ПРИМЕНЕНО" if a.apply else "DRY-RUN: ничего не изменено")
    return 0


def cmd_rollback(a) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    c = PostgresHook(postgres_conn_id=a.conn).get_conn()
    c.autocommit = False
    cur = c.cursor()
    try:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('etl_meta.ch_config'))")
        cur.execute("SELECT is_active, dag_id FROM etl_meta.ch_sync_group WHERE sync_group = %s FOR UPDATE", (a.live_group,))
        g = cur.fetchone()
        if not g:
            raise Stop(f"группы {a.live_group!r} нет")
        cur.execute("SELECT code, is_active, target_database || '.' || target_table FROM etl_meta.ch_sync "
                    "WHERE sync_group = %s ORDER BY priority FOR UPDATE", (a.live_group,))
        members = cur.fetchall()
        steps = []
        if g[0]:
            cur.execute("UPDATE etl_meta.ch_sync_group SET is_active = false, updated_at = now() WHERE sync_group = %s",
                        (a.live_group,))
            steps.append(f"ch_sync_group {a.live_group}: is_active → false (DAG {g[1]} её больше не ведёт)")
        for code, act, fqn in members:
            if act:
                cur.execute("UPDATE etl_meta.ch_sync SET is_active = false, updated_at = now() WHERE code = %s", (code,))
                steps.append(f"ch_sync {code}: is_active → false ({fqn} остаётся, данные не трогаются)")
        steps.append("не трогается: таблицы ClickHouse (боевые и shadow), shadow-конфигурации, doc_key, история, "
                     "ch_source_state, остальные группы")
        print("ROLLBACK:" if a.apply else "ROLLBACK (dry-run):")
        for s in steps:
            print("   •", s)
        if a.apply:
            c.commit()
            print("ПРИМЕНЕНО")
        else:
            c.rollback()
            print("DRY-RUN: выполнено и откачено")
        return 0
    finally:
        c.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="shadow → боевая группа ClickHouse по конфигурации")
    ap.add_argument("--conn", help="Airflow conn_id control plane (обязателен)")
    ap.add_argument("--ch-conn", default="clickhouse_etl")
    ap.add_argument("--ch-config", default=ADMIN_CFG, help="clickhouse-client config ch_admin (DDL, перенос партиций)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("cutover")
    p.add_argument("--group", required=True, help="shadow-группа")
    p.add_argument("--live-group", required=True)
    p.add_argument("--dag", required=True)
    p.add_argument("--position", type=int, required=True)
    p.add_argument("--watermark", required=True, help="отметка источника боевой группы, Almaty naive 'YYYY-MM-DD HH:MM:SS'")
    p.add_argument("--suffix", default="_shadow", help="суффикс shadow-имён (code, target_table)")
    p.add_argument("--state-key", help="боевой state_key (по умолчанию — shadow без ':shadow')")
    p.add_argument("--param", action="append", help="key=json — добавить/заменить в боевых source_params")
    p.add_argument("--description")
    p.add_argument("--skip-reconcile", action="store_true", help="не сверять все партиции с 1С (только если сверено отдельно)")
    p.add_argument("--open-window", type=int, default=1,
                   help="последние N партиций (не позже текущей) — операционно открытые: shadow ⊆ источника, "
                        "существующие документы совпадают построчно; остальные — строго 1:1")
    p.add_argument("--apply", action="store_true")
    p = sub.add_parser("rollback")
    p.add_argument("--live-group", required=True)
    p.add_argument("--apply", action="store_true")
    p = sub.add_parser("discard", help="удалить неиспользуемые боевые таблицы shadow-группы")
    p.add_argument("--group", required=True)
    p.add_argument("--suffix", default="_shadow")
    p.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)
    a.conn = require_conn("--conn", a.conn)
    try:
        return {"cutover": cmd_cutover, "rollback": cmd_rollback, "discard": cmd_discard}[a.cmd](a)
    except Stop as e:
        print(f"ОСТАНОВ: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"СБОЙ: {type(e).__name__}: {str(e)[:500]}\n"
              f"control plane — одна транзакция, откатан целиком. Таблицы ClickHouse, созданные до сбоя, ни одной "
              f"конфигурацией не используются: повтор cutover продолжит с них (совпадающие с shadow не переносятся), "
              f"discard — удалит.", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
