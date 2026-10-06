"""
Конфиг публикации в ClickHouse — без CONFIG_SEED-миграций.

Новый бизнес-объект заводится данными control plane, а не SQL-файлом:

    регистр / источники / маппинги / цели   — конфигуратор (как и раньше)
    ch_sync + ch_sync_columns                — sync     (спецификация JSON: scaffold → правка → sync)
    область реестра id документов            — scope
    группа оркестрации и порядок в ней       — group
    таблица ClickHouse + staging + права     — ch_ddl   (как и раньше)

Каждая команда — одна транзакция под общей advisory-блокировкой. Без --apply изменения
выполняются и откатываются (dry-run проверяет и ограничения БД), с --apply — фиксируются.
Повтор той же команды ничего не меняет: сравнение идёт до записи, при совпадении записи нет.
Действующую конфигурацию (is_active, активная группа DAG'а) команды не меняют без --allow-live.

    PYTHONPATH=dags python3 -m core.tools.ch_config --conn etl_prod scaffold --register R --target T --shadow --group G > spec.json
    PYTHONPATH=dags python3 -m core.tools.ch_config --conn etl_prod sync --spec spec.json [--apply]
    PYTHONPATH=dags python3 -m core.tools.ch_config --conn etl_prod export --code C
    PYTHONPATH=dags python3 -m core.tools.ch_config --conn etl_prod scope --name N [--apply]
    PYTHONPATH=dags python3 -m core.tools.ch_config --conn etl_prod group set --group G --dag D --position N [--active on|off] [--apply]
    PYTHONPATH=dags python3 -m core.tools.ch_config --conn etl_prod group add --group G --code C [--priority N] [--apply]
    PYTHONPATH=dags python3 -m core.tools.ch_config --conn etl_prod group order --group G --codes A,B [--apply]
    PYTHONPATH=dags python3 -m core.tools.ch_config --conn etl_prod group show [--group G]
"""
import argparse
import json
import re
import sys
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, __file__.rsplit("/core/", 1)[0])
from core.conn import require_conn                 # noqa: E402

LOCK_KEY = "etl_meta.ch_config"
NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
SEQ_RE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")

# поля ch_sync, которыми управляет спецификация (id, created_at, updated_at — нет)
SYNC_FIELDS = ["description", "source_conn_id", "source_type", "source_schema", "source_object", "source_query",
               "target_database", "target_table", "partition_expr", "order_by", "load_mode", "partition_column",
               "partition_granularity", "watermark_column", "business_key", "batch_size", "empty_partition_policy",
               "hot_window", "sweep_interval_min", "checksum_columns", "measure_columns", "reconcile_metrics",
               "lookup", "source_params", "priority", "is_active", "sync_group"]
ARRAY_FIELDS = {"order_by", "business_key", "checksum_columns", "measure_columns"}
JSON_FIELDS = {"reconcile_metrics", "lookup", "source_params"}
REQUIRED = ["code", "source_conn_id", "source_type", "target_table", "order_by", "load_mode", "columns"]
DEFAULTS = {"target_database": "analytics_poc", "partition_expr": "tuple()", "batch_size": 100000,
            "empty_partition_policy": "fail", "hot_window": 3, "sweep_interval_min": 60,
            "reconcile_metrics": {}, "priority": 100, "is_active": False}
COLUMN_FIELDS = ["source_expr", "target_column", "target_type", "codec"]
GROUP_FIELDS = ["dag_id", "position", "is_active", "runner", "description"]


class Refused(Exception):
    """Операция отклонена до записи — ничего не изменено."""


# ------------------------------------------------------------------ транзакция
class Tx:
    """Одна транзакция на команду; без apply — откат. Блокировка сериализует параллельные запуски."""

    def __init__(self, conn_id: str, apply: bool):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        self.conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        self.apply = apply
        self.changes: List[str] = []
        self.cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (LOCK_KEY,))

    def q(self, sql: str, params=None) -> List[tuple]:
        self.cur.execute(sql, params)
        return self.cur.fetchall() if self.cur.description else []

    def one(self, sql: str, params=None):
        r = self.q(sql, params)
        return r[0] if r else None

    def write(self, what: str, sql: str, params=None) -> List[tuple]:
        self.changes.append(what)
        return self.q(sql, params)

    def finish(self) -> int:
        if not self.changes:
            self.conn.rollback()
            print("БЕЗ ИЗМЕНЕНИЙ — конфигурация уже такая")
            return 0
        print("ИЗМЕНЕНИЯ:")
        for c in self.changes:
            print("  •", c)
        if self.apply:
            self.conn.commit()
            print("ПРИМЕНЕНО (COMMIT)")
        else:
            self.conn.rollback()
            print("DRY-RUN: выполнено и откачено (ROLLBACK) — для записи нужен --apply")
        return 0

    def abort(self, msg: str) -> int:
        self.conn.rollback()
        print(f"ОТКАЗ: {msg}\nничего не изменено (ROLLBACK)", file=sys.stderr)
        return 2


def _name(kind: str, v: str) -> str:
    if not v or not NAME_RE.match(v):
        raise Refused(f"{kind} {v!r}: допустимы a-z, 0-9, _ (начало — буква, до 63 символов)")
    return v


# ------------------------------------------------------------------ ch_sync
def load_sync(tx: Tx, code: str) -> Optional[Dict]:
    row = tx.one(f"SELECT id, {', '.join(SYNC_FIELDS)} FROM etl_meta.ch_sync WHERE code = %s FOR UPDATE", (code,))
    if not row:
        return None
    d = {"id": row[0], "code": code}
    d.update({f: row[i + 1] for i, f in enumerate(SYNC_FIELDS)})
    d["columns"] = [dict(zip(COLUMN_FIELDS, r)) for r in tx.q(
        "SELECT source_expr, target_column, target_type, codec FROM etl_meta.ch_sync_columns "
        "WHERE sync_id = %s ORDER BY ordinal", (row[0],))]
    return d


def normalize_spec(spec: Dict) -> Dict:
    s = {k: v for k, v in spec.items() if not k.startswith("_")}
    missing = [k for k in REQUIRED if s.get(k) in (None, "", [])]
    if missing:
        raise Refused(f"в спецификации нет обязательных полей: {missing}")
    unknown = set(s) - set(SYNC_FIELDS) - {"code", "columns"}
    if unknown:
        raise Refused(f"неизвестные поля спецификации: {sorted(unknown)}")
    for k, v in DEFAULTS.items():
        s.setdefault(k, v)
    for f in SYNC_FIELDS:
        s.setdefault(f, None)
    _name("code", s["code"])
    _name("target_table", s["target_table"])
    cols = []
    for i, c in enumerate(s["columns"], 1):
        bad = set(c) - set(COLUMN_FIELDS)
        if bad or not c.get("target_column") or not c.get("target_type"):
            raise Refused(f"колонка #{i}: нужны source_expr/target_column/target_type, лишние поля {sorted(bad)}")
        cols.append({"source_expr": c.get("source_expr") or c["target_column"], "target_column": c["target_column"],
                     "target_type": c["target_type"], "codec": c.get("codec")})
    s["columns"] = cols
    names = [c["target_column"] for c in cols]
    dup = sorted({n for n in names if names.count(n) > 1})
    if dup:
        raise Refused(f"повторяются колонки цели: {dup}")
    for f in ("order_by", "business_key", "checksum_columns", "measure_columns"):
        miss = [x for x in (s.get(f) or []) if x not in names]
        if miss:
            raise Refused(f"{f} ссылается на колонки, которых нет в columns: {miss}")
    return s


def validate_refs(tx: Tx, s: Dict) -> List[str]:
    """Ссылки спецификации на остальной конфиг. Ошибки — отказ, предупреждения — печать."""
    warn = []
    p = s.get("source_params") or {}
    if s["source_type"] == "onec_register":
        reg = tx.one("SELECT id FROM etl_meta.registers WHERE code = %s", (s["source_object"],))
        if not reg:
            raise Refused(f"регистр {s['source_object']!r} не найден в etl_meta.registers")
        targets = {r[0]: r[1] for r in tx.q(
            "SELECT target_table, target_role FROM etl_meta.register_targets WHERE register_id = %s AND is_active", (reg[0],))}
        for k in ("target", "parent"):
            if p.get(k) and p[k] not in targets:
                raise Refused(f"source_params.{k}={p[k]!r}: у регистра {s['source_object']} нет активной цели с таким именем "
                              f"(есть {sorted(targets)})")
        if not p.get("target"):
            raise Refused("onec_register: source_params.target обязателен — имя цели регистра")
        if not p.get("state_key"):
            raise Refused("onec_register: source_params.state_key обязателен — ключ водяного знака источника")
        if p.get("own_id") and not tx.one("SELECT 1 FROM etl_meta.doc_key_scope WHERE doc_table = %s", (p["target"],)):
            warn.append(f"own_id: области doc_key {p['target']!r} ещё нет — завести командой scope --name {p['target']}")
    if s.get("sync_group") and not tx.one("SELECT 1 FROM etl_meta.ch_sync_group WHERE sync_group = %s", (s["sync_group"],)):
        warn.append(f"группы {s['sync_group']!r} ещё нет — завести командой group set")
    other = tx.one("SELECT code FROM etl_meta.ch_sync WHERE target_database = %s AND target_table = %s AND code <> %s",
                   (s["target_database"], s["target_table"], s["code"]))
    if other:
        raise Refused(f"таблица {s['target_database']}.{s['target_table']} уже описана конфигурацией {other[0]!r}")
    return warn


def _group_live(tx: Tx, group: Optional[str]) -> bool:
    if not group:
        return False
    r = tx.one("SELECT is_active AND dag_id <> 'none' FROM etl_meta.ch_sync_group WHERE sync_group = %s", (group,))
    return bool(r and r[0])


def _diff(old: Dict, new: Dict) -> List[str]:
    out = []
    for f in SYNC_FIELDS:
        a, b = old.get(f), new.get(f)
        if f in ARRAY_FIELDS:
            a, b = list(a or []), list(b or [])
        if f in JSON_FIELDS:
            a, b = json.dumps(a, sort_keys=True), json.dumps(b, sort_keys=True)
        if a != b:
            out.append(f"{f}: {a!r} → {b!r}")
    if old["columns"] != new["columns"]:
        oc = {c["target_column"]: c for c in old["columns"]}
        nc = {c["target_column"]: c for c in new["columns"]}
        for n in nc:
            if n not in oc:
                out.append(f"колонка +{n} {nc[n]['target_type']}")
            elif oc[n] != nc[n]:
                out.append(f"колонка ~{n}: {oc[n]} → {nc[n]}")
        out += [f"колонка -{n}" for n in oc if n not in nc]
        if [c["target_column"] for c in old["columns"] if c["target_column"] in nc] != \
           [c["target_column"] for c in new["columns"] if c["target_column"] in oc]:
            out.append("порядок колонок изменён")
    return out


def _sql_value(f: str, v):
    from psycopg2.extras import Json
    return Json(v) if f in JSON_FIELDS and v is not None else v


def cmd_sync(tx: Tx, spec: Dict, allow_live: bool) -> int:
    s = normalize_spec(spec)
    for w in validate_refs(tx, s):
        print("ПРЕДУПРЕЖДЕНИЕ:", w)
    old = load_sync(tx, s["code"])
    live = bool(s["is_active"]) or _group_live(tx, s["sync_group"]) or bool(old and (old["is_active"] or _group_live(tx, old["sync_group"])))
    if old is None:
        if live and not allow_live:
            raise Refused(f"{s['code']}: новая конфигурация была бы действующей (is_active или активная группа) — нужен --allow-live")
        cols = ", ".join(SYNC_FIELDS)
        sid = tx.write(f"ch_sync +{s['code']} → {s['target_database']}.{s['target_table']} "
                       f"(группа {s['sync_group']}, active={s['is_active']})",
                       f"INSERT INTO etl_meta.ch_sync (code, {cols}) VALUES (%s, {', '.join(['%s'] * len(SYNC_FIELDS))}) RETURNING id",
                       [s["code"]] + [_sql_value(f, s[f]) for f in SYNC_FIELDS])[0][0]
        _write_columns(tx, sid, s["columns"], replace=False)
        return tx.finish()
    changes = _diff(old, s)
    if not changes:
        return tx.finish()
    if live and not allow_live:
        raise Refused(f"{s['code']}: действующая конфигурация (is_active или активная группа DAG'а) — "
                      f"изменения {changes[:5]} только с --allow-live")
    for c in changes:
        tx.changes.append(f"ch_sync ~{s['code']}: {c}")
    sets = ", ".join(f"{f} = %s" for f in SYNC_FIELDS)
    tx.q(f"UPDATE etl_meta.ch_sync SET {sets}, updated_at = now() WHERE id = %s",
         [_sql_value(f, s[f]) for f in SYNC_FIELDS] + [old["id"]])
    if old["columns"] != s["columns"]:
        _write_columns(tx, old["id"], s["columns"], replace=True)
    return tx.finish()


def _write_columns(tx: Tx, sync_id: int, cols: List[Dict], replace: bool) -> None:
    if replace:
        tx.q("DELETE FROM etl_meta.ch_sync_columns WHERE sync_id = %s", (sync_id,))
    for i, c in enumerate(cols, 1):
        tx.q("INSERT INTO etl_meta.ch_sync_columns (sync_id, ordinal, source_expr, target_column, target_type, codec) "
             "VALUES (%s, %s, %s, %s, %s, %s)", (sync_id, i, c["source_expr"], c["target_column"], c["target_type"], c["codec"]))
    if not replace:
        tx.changes.append(f"ch_sync_columns: {len(cols)} колонок")


def cmd_sync_delete(tx: Tx, code: str) -> int:
    old = load_sync(tx, code)
    if not old:
        return tx.finish()
    if old["is_active"] or _group_live(tx, old["sync_group"]):
        raise Refused(f"{code}: действующая конфигурация — удаление запрещено")
    hist = tx.one("SELECT count(*) FROM etl_meta.ch_sync_history WHERE sync_id = %s", (old["id"],))[0]
    if hist:
        raise Refused(f"{code}: есть история загрузок ({hist}) — это не черновик, удаление запрещено")
    tx.write(f"ch_sync -{code} (и {len(old['columns'])} колонок, состояние партиций)",
             "DELETE FROM etl_meta.ch_sync WHERE id = %s", (old["id"],))
    return tx.finish()


def export_spec(tx: Tx, code: str) -> Dict:
    old = load_sync(tx, code)
    if not old:
        raise Refused(f"конфигурации {code!r} нет")
    old.pop("id")
    return {"code": old.pop("code"), **{f: old[f] for f in SYNC_FIELDS}, "columns": old["columns"]}


# ------------------------------------------------------------------ scaffold
CH_TYPE = {"uuid": "UUID", "timestamp": "DateTime", "date": "Date", "numeric": "Decimal(18,4)",
           "boolean": "UInt8", "bigint": "Int64", "integer": "Int64"}
# технический хребет документа (CLAUDE.md): типы известны заранее
BACKBONE = {"recorder": "UUID", "recorder_type": "UInt16", "line_no": "UInt32", "period": "DateTime"}


def _codec(t: str) -> str:
    if t.startswith(("DateTime", "Date")):
        return "Delta, ZSTD(1)"
    if re.match(r"^U?Int\d+$", t):
        return "T64, ZSTD(1)"
    return "ZSTD(1)"


def scaffold(tx: Tx, register: str, target: str, *, shadow: bool, group: Optional[str], code: Optional[str],
             table: Optional[str], database: str, source_conn: str, priority: int) -> Dict:
    """Черновик спецификации из конфига регистра. Не пишет ничего; типы — проверить и сузить вручную."""
    from core.config import refs
    reg = tx.one("SELECT id, retail_table, retail_uid_column FROM etl_meta.registers WHERE code = %s", (register,))
    if not reg:
        raise Refused(f"регистр {register!r} не найден")
    t = tx.one("SELECT id, target_role, parent_target_id, include_columns, upsert_keys FROM etl_meta.register_targets "
               "WHERE register_id = %s AND target_table = %s AND is_active", (reg[0], target))
    if not t:
        raise Refused(f"у регистра {register} нет активной цели {target!r}")
    tid, role, parent_id, include, upsert = t
    parent = tx.one("SELECT target_table FROM etl_meta.register_targets WHERE id = %s", (parent_id,))[0] if parent_id else None
    if not include:
        # include_columns не задан — цель берёт все колонки своих маппингов
        include = [r[0] for r in tx.q(
            "SELECT target_column FROM etl_meta.column_mappings WHERE register_id = %s AND is_active "
            "AND (target_id IS NULL OR target_id = %s) GROUP BY target_column ORDER BY min(id)", (reg[0], tid))]
    types = {r[0]: r[1] for r in tx.q(
        "SELECT DISTINCT ON (target_column) target_column, target_type FROM etl_meta.column_mappings "
        "WHERE register_id = %s AND is_active AND target_type IS NOT NULL ORDER BY target_column, id", (reg[0],))}
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # refs читает через hook
    hook = PostgresHook(postgres_conn_id=tx.conn_id)
    dims = refs.dim_links(hook, tid)
    docs = refs.register_links(hook, tid)

    cols: List[Tuple[str, str, str]] = []
    own_id = parent is None and role == "dimension"
    if own_id:
        cols.append(("id", "id", "UInt64"))
    if parent:
        cols.append(("hdr_id", f"{parent}_id", "UInt64"))
    for c in include or []:
        if c == "raw_refs" or c.startswith("raw_refs."):
            continue
        typ = BACKBONE.get(c) or CH_TYPE.get((types.get(c) or "").lower(), "String")
        cols.append((c, c, typ))
    for d in dims:
        cols.append((d["fk_col"], d["fk_col"], "UInt32"))
    for d in docs:
        cols.append((d["fk_col"], d["fk_col"], "UInt64"))
    cols.append(("hdr_retail_updated_at" if parent else "retail_updated_at", "source_updated_at", "DateTime"))
    cols.append(("etl_updated_at", "loaded_at", "DateTime"))
    names = [c[1] for c in cols]

    bk = [k for k in (upsert or []) if k in names]
    order_by = [k for k in (["period", "recorder", "line_no"] if "line_no" in names else ["period", "recorder_type", "recorder"]) if k in names]
    id_cols = [c[1] for c in cols if c[1].endswith("_id") and c[1] != "id"]
    measures = [c[1] for c in cols if c[2].startswith("Decimal")]
    state_key = f"onec_register:{register}" + (":shadow" if shadow else "")
    params: Dict = {"target": target, "shadow": shadow, "doc_key": ["recorder"], "state_key": state_key}
    if own_id:
        params["own_id"] = True
        if reg[1]:
            params["signal_sources"] = [{"table": reg[1], "key_column": reg[2] or "document_uid", "updated_at_column": "updated_at"}]
    if parent:
        params.update(parent=parent, parent_key=["recorder", "recorder_type"], parent_prefix="hdr_")
    tbl = table or f"fact_{target}" + ("_shadow" if shadow else "")
    return {
        "_notes": ["черновик из etl_meta.register_targets/column_mappings — проверить типы (Int64/String по умолчанию), "
                   "order_by, measure_columns, signal_sources; поля с '_' в начале игнорируются"],
        "code": code or tbl, "description": f"{'SHADOW ' if shadow else ''}{register}/{target} → ClickHouse ({tbl})",
        "source_conn_id": source_conn, "source_type": "onec_register", "source_schema": None, "source_object": register,
        "source_query": None, "target_database": database, "target_table": tbl,
        "partition_expr": "toYYYYMM(period)" if "period" in names else "tuple()",
        "order_by": order_by, "load_mode": "document_patch",
        "partition_column": "period" if "period" in names else None,
        "partition_granularity": "month" if "period" in names else None, "watermark_column": None,
        "business_key": bk, "batch_size": 100000, "empty_partition_policy": "fail", "hot_window": 3, "sweep_interval_min": 60,
        "checksum_columns": bk + [c for c in id_cols if c not in bk], "measure_columns": measures,
        "reconcile_metrics": {"unique": [bk], "not_zero": ["id"] if own_id else ([f"{parent}_id"] if parent else [])},
        "lookup": None, "source_params": params, "priority": priority, "is_active": False, "sync_group": group,
        "columns": [{"source_expr": s, "target_column": n, "target_type": t, "codec": _codec(t)} for s, n, t in cols],
    }


# ------------------------------------------------------------------ doc_key scope
def cmd_scope(tx: Tx, name: str, sequence: Optional[str]) -> int:
    _name("scope", name)
    seq = sequence or f"etl_meta.doc_key_{name}_seq"
    if not SEQ_RE.match(seq):
        raise Refused(f"sequence {seq!r}: нужно schema.name в нижнем регистре")
    cur = tx.one("SELECT sequence_name, issuer FROM etl_meta.doc_key_scope WHERE doc_table = %s FOR UPDATE", (name,))
    if cur:
        if cur[0] != seq and sequence:
            raise Refused(f"область {name!r} уже есть с последовательностью {cur[0]} (запрошена {seq}) — "
                          f"последовательность выданных id не меняется")
        if not tx.one("SELECT to_regclass(%s)", (cur[0],))[0]:
            raise Refused(f"область {name!r} ссылается на несуществующую последовательность {cur[0]}")
        print(f"область {name!r}: последовательность {cur[0]}, issuer {cur[1]}")
        return tx.finish()
    if not tx.one("SELECT to_regclass(%s)", (seq,))[0]:
        schema, rel = seq.split(".")
        tx.write(f"CREATE SEQUENCE {seq}", f'CREATE SEQUENCE IF NOT EXISTS "{schema}"."{rel}"')
    else:
        used = tx.one("SELECT doc_table FROM etl_meta.doc_key_scope WHERE sequence_name = %s", (seq,))
        if used:
            raise Refused(f"последовательность {seq} уже выдаёт id области {used[0]!r} — общая последовательность запрещена")
        print(f"последовательность {seq} уже существует — используется")
    tx.write(f"doc_key_scope +{name} → {seq} (issuer registry)",
             "INSERT INTO etl_meta.doc_key_scope (doc_table, sequence_name, issuer) VALUES (%s, %s, 'registry') "
             "ON CONFLICT (doc_table) DO NOTHING", (name, seq))
    return tx.finish()


def cmd_scope_delete(tx: Tx, name: str) -> int:
    cur = tx.one("SELECT sequence_name FROM etl_meta.doc_key_scope WHERE doc_table = %s FOR UPDATE", (name,))
    if not cur:
        return tx.finish()
    n = tx.one("SELECT count(*) FROM etl_meta.doc_key WHERE doc_table = %s", (name,))[0]
    if n:
        raise Refused(f"область {name!r}: выдано {n} id — удаление запрещено (id не перевыдаются)")
    users = [r[0] for r in tx.q("SELECT code FROM etl_meta.ch_sync WHERE source_params->>'target' = %s "
                                "AND (source_params->>'own_id')::boolean", (name,))]
    if users:
        raise Refused(f"область {name!r} используется конфигурациями {users}")
    tx.write(f"doc_key_scope -{name}", "DELETE FROM etl_meta.doc_key_scope WHERE doc_table = %s", (name,))
    if cur[0] == f"etl_meta.doc_key_{name}_seq" and tx.one("SELECT to_regclass(%s)", (cur[0],))[0]:
        tx.write(f"DROP SEQUENCE {cur[0]}", f"DROP SEQUENCE {cur[0]}")
    return tx.finish()


# ------------------------------------------------------------------ ch_sync_group
def group_show(tx: Tx, group: Optional[str]) -> int:
    rows = tx.q("SELECT sync_group, dag_id, position, is_active, runner, coalesce(description,'') FROM etl_meta.ch_sync_group "
                + ("WHERE sync_group = %s " if group else "") + "ORDER BY dag_id, position, sync_group", (group,) if group else None)
    if group and not rows:
        print(f"группы {group!r} нет")
    for g, dag, pos, act, runner, desc in rows:
        print(f"{g:<24} dag={dag:<16} position={pos:<4} active={str(act):<5} runner={runner:<13} {desc}")
        for code, prio, sa, tbl in tx.q("SELECT code, priority, is_active, target_database||'.'||target_table FROM etl_meta.ch_sync "
                                        "WHERE sync_group = %s ORDER BY priority, code", (g,)):
            print(f"    {prio:>5}  {code:<36} active={str(sa):<5} → {tbl}")
    orphans = tx.q("SELECT DISTINCT sync_group FROM etl_meta.ch_sync s WHERE sync_group IS NOT NULL AND NOT EXISTS "
                   "(SELECT 1 FROM etl_meta.ch_sync_group g WHERE g.sync_group = s.sync_group)")
    if orphans and not group:
        print("ch_sync ссылается на незаведённые группы:", [r[0] for r in orphans])
    tx.conn.rollback()
    return 0


def group_set(tx: Tx, group: str, vals: Dict, allow_live: bool) -> int:
    _name("group", group)
    cur = tx.one(f"SELECT {', '.join(GROUP_FIELDS)} FROM etl_meta.ch_sync_group WHERE sync_group = %s FOR UPDATE", (group,))
    if cur is None:
        if vals.get("dag_id") is None or vals.get("position") is None:
            raise Refused("новая группа: нужны --dag и --position")
        new = {"is_active": False, "runner": "ch_sync", "description": None, **{k: v for k, v in vals.items() if v is not None}}
        if new["is_active"] and new["dag_id"] != "none" and not allow_live:
            raise Refused(f"группа {group!r} сразу активной в DAG {new['dag_id']} — нужен --allow-live")
        tx.write(f"ch_sync_group +{group}: {new}",
                 f"INSERT INTO etl_meta.ch_sync_group (sync_group, {', '.join(GROUP_FIELDS)}) VALUES (%s, %s, %s, %s, %s, %s)",
                 [group] + [new[f] for f in GROUP_FIELDS])
        return tx.finish()
    old = dict(zip(GROUP_FIELDS, cur))
    upd = {k: v for k, v in vals.items() if v is not None and old[k] != v}
    if not upd:
        return tx.finish()
    becomes_live = (upd.get("is_active", old["is_active"]) and upd.get("dag_id", old["dag_id"]) != "none")
    was_live = old["is_active"] and old["dag_id"] != "none"
    if (becomes_live or was_live) and not allow_live:
        raise Refused(f"группа {group!r} действующая или становится действующей — изменение {upd} только с --allow-live")
    tx.write(f"ch_sync_group ~{group}: " + ", ".join(f"{k}: {old[k]!r} → {v!r}" for k, v in upd.items()),
             f"UPDATE etl_meta.ch_sync_group SET {', '.join(f'{k} = %s' for k in upd)}, updated_at = now() WHERE sync_group = %s",
             list(upd.values()) + [group])
    return tx.finish()


def _members_guard(tx: Tx, group: str, codes: List[str], allow_live: bool) -> Dict[str, tuple]:
    if not tx.one("SELECT 1 FROM etl_meta.ch_sync_group WHERE sync_group = %s", (group,)):
        raise Refused(f"группы {group!r} нет — сначала group set")
    rows = {r[0]: r[1:] for r in tx.q("SELECT code, sync_group, priority, is_active FROM etl_meta.ch_sync "
                                      "WHERE code = ANY(%s) FOR UPDATE", (codes,))}
    missing = [c for c in codes if c not in rows]
    if missing:
        raise Refused(f"конфигураций нет: {missing}")
    if not allow_live:
        live = [c for c, (g, _, a) in rows.items() if a or _group_live(tx, g)] + ([group] if _group_live(tx, group) else [])
        if live:
            raise Refused(f"затрагивает действующие конфигурации/группу {live} — только с --allow-live")
    return rows


def group_add(tx: Tx, group: str, code: str, priority: Optional[int], allow_live: bool) -> int:
    rows = _members_guard(tx, group, [code], allow_live)
    g, prio, _ = rows[code]
    new_prio = priority if priority is not None else prio
    if g == group and prio == new_prio:
        return tx.finish()
    tx.write(f"ch_sync ~{code}: группа {g!r} → {group!r}, priority {prio} → {new_prio}",
             "UPDATE etl_meta.ch_sync SET sync_group = %s, priority = %s, updated_at = now() WHERE code = %s",
             (group, new_prio, code))
    return tx.finish()


def group_order(tx: Tx, group: str, codes: List[str], start: Optional[int], step: int, allow_live: bool) -> int:
    rows = _members_guard(tx, group, codes, allow_live)
    foreign = [c for c in codes if rows[c][0] != group]
    if foreign:
        raise Refused(f"не в группе {group!r}: {foreign} — сначала group add")
    base = start if start is not None else min(rows[c][1] for c in codes)
    for i, c in enumerate(codes):
        p = base + i * step
        if rows[c][1] != p:
            tx.write(f"ch_sync ~{c}: priority {rows[c][1]} → {p}",
                     "UPDATE etl_meta.ch_sync SET priority = %s, updated_at = now() WHERE code = %s", (p, c))
    return tx.finish()


def group_delete(tx: Tx, group: str) -> int:
    cur = tx.one("SELECT is_active, dag_id FROM etl_meta.ch_sync_group WHERE sync_group = %s FOR UPDATE", (group,))
    if not cur:
        return tx.finish()
    if cur[0] and cur[1] != "none":
        raise Refused(f"группа {group!r} действующая — удаление запрещено")
    members = [r[0] for r in tx.q("SELECT code FROM etl_meta.ch_sync WHERE sync_group = %s", (group,))]
    if members:
        raise Refused(f"в группе есть конфигурации {members} — сначала перенести или удалить их")
    tx.write(f"ch_sync_group -{group}", "DELETE FROM etl_meta.ch_sync_group WHERE sync_group = %s", (group,))
    return tx.finish()


# ------------------------------------------------------------------ CLI
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="конфиг публикации ClickHouse без миграций")
    ap.add_argument("--conn", help="Airflow conn_id control plane (обязателен, умолчания нет)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def flags(p, live=True):
        p.add_argument("--apply", action="store_true", help="зафиксировать (без него — dry-run с откатом)")
        if live:
            p.add_argument("--allow-live", action="store_true", help="разрешить изменение действующей конфигурации")

    p = sub.add_parser("scaffold", help="черновик спецификации ch_sync из цели регистра (только чтение)")
    p.add_argument("--register", required=True); p.add_argument("--target", required=True)
    p.add_argument("--shadow", action="store_true"); p.add_argument("--group")
    p.add_argument("--code"); p.add_argument("--table"); p.add_argument("--database", default="analytics_poc")
    p.add_argument("--source-conn", default="mssql_1c_conn"); p.add_argument("--priority", type=int, default=100)
    p = sub.add_parser("export", help="спецификация существующей ch_sync (только чтение)")
    p.add_argument("--code", required=True)
    p = sub.add_parser("sync", help="создать/обновить ch_sync + ch_sync_columns по спецификации")
    p.add_argument("--spec", required=True, help="JSON-файл спецификации ('-' — stdin)"); flags(p)
    p = sub.add_parser("sync-delete", help="удалить черновую ch_sync (неактивную, без истории)")
    p.add_argument("--code", required=True); flags(p, live=False)
    p = sub.add_parser("scope", help="область реестра id документов (doc_key_scope + sequence)")
    p.add_argument("--name", required=True); p.add_argument("--sequence"); flags(p, live=False)
    p = sub.add_parser("scope-delete", help="удалить пустую область (ни одного выданного id)")
    p.add_argument("--name", required=True); flags(p, live=False)

    g = sub.add_parser("group", help="группы оркестрации").add_subparsers(dest="gcmd", required=True)
    p = g.add_parser("show"); p.add_argument("--group")
    p = g.add_parser("set", help="создать/изменить группу")
    p.add_argument("--group", required=True); p.add_argument("--dag"); p.add_argument("--position", type=int)
    p.add_argument("--runner", choices=["ch_sync", "reference_dim"]); p.add_argument("--description")
    p.add_argument("--active", choices=["on", "off"]); flags(p)
    p = g.add_parser("add", help="включить ch_sync в группу")
    p.add_argument("--group", required=True); p.add_argument("--code", required=True)
    p.add_argument("--priority", type=int); flags(p)
    p = g.add_parser("order", help="порядок конфигураций в группе (priority)")
    p.add_argument("--group", required=True); p.add_argument("--codes", required=True, help="через запятую, по порядку")
    p.add_argument("--start", type=int); p.add_argument("--step", type=int, default=1); flags(p)
    p = g.add_parser("delete", help="удалить пустую неактивную группу"); p.add_argument("--group", required=True); flags(p, live=False)

    a = ap.parse_args(argv)
    conn_id = require_conn("--conn", a.conn)
    tx = Tx(conn_id, getattr(a, "apply", False))
    tx.conn_id = conn_id
    try:
        if a.cmd == "scaffold":
            spec = scaffold(tx, a.register, a.target, shadow=a.shadow, group=a.group, code=a.code, table=a.table,
                            database=a.database, source_conn=a.source_conn, priority=a.priority)
            tx.conn.rollback()
            print(json.dumps(spec, ensure_ascii=False, indent=1))
            return 0
        if a.cmd == "export":
            spec = export_spec(tx, a.code)
            tx.conn.rollback()
            print(json.dumps(spec, ensure_ascii=False, indent=1, default=str))
            return 0
        if a.cmd == "sync":
            spec = json.load(sys.stdin if a.spec == "-" else open(a.spec))
            return cmd_sync(tx, spec, a.allow_live)
        if a.cmd == "sync-delete":
            return cmd_sync_delete(tx, a.code)
        if a.cmd == "scope":
            return cmd_scope(tx, a.name, a.sequence)
        if a.cmd == "scope-delete":
            return cmd_scope_delete(tx, a.name)
        if a.gcmd == "show":
            return group_show(tx, a.group)
        if a.gcmd == "set":
            active = None if a.active is None else a.active == "on"
            return group_set(tx, a.group, {"dag_id": a.dag, "position": a.position, "is_active": active,
                                           "runner": a.runner, "description": a.description}, a.allow_live)
        if a.gcmd == "add":
            return group_add(tx, a.group, a.code, a.priority, a.allow_live)
        if a.gcmd == "order":
            return group_order(tx, a.group, [c.strip() for c in a.codes.split(",") if c.strip()], a.start, a.step, a.allow_live)
        if a.gcmd == "delete":
            return group_delete(tx, a.group)
    except Refused as e:
        return tx.abort(str(e))
    except Exception as e:
        return tx.abort(f"{type(e).__name__}: {e}")
    finally:
        tx.conn.close()
    return 1


if __name__ == "__main__":
    sys.exit(main())
