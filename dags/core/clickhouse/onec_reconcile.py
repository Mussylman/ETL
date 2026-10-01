"""
Независимая сверка 1С ↔ ClickHouse — напрямую, без PostgreSQL.

Отпечаток партиции считается дважды независимыми реализациями: на стороне 1С —
выражениями T-SQL над физическими полями, на стороне ClickHouse — над опубликованной
целью. Выражения 1С не зашиты: они выводятся из column_mappings источников цели по
конфигурации движка:

    регистр (standalone)       — одна таблица;
    шапка документа (header)   — таблица шапки со своим where_clause;
    строки (detail / union)    — каждая табличная часть, присоединённая к шапке
                                 (ключ, период и where берутся у шапки).

Члены отпечатка складываются: число строк, контрольная сумма, суммы и суммы
квадратов аддитивны.

В контрольную сумму идут бизнес-ключ строки и ссылки на справочники, лежащие в
источнике строки. Ссылка сравнивается по guid — на стороне ClickHouse id
разворачивается в guid через реплику справочника, то есть проверяется вся цепочка
«ссылка 1С → реестр → id → справочник». Меры — через sum и sum квадратов.
"""

import contextlib
import io
from decimal import Decimal
from typing import Dict, List, Tuple

EMPTY = "00000000-0000-0000-0000-000000000000"
UTF8 = "Latin1_General_100_BIN2_UTF8"
KEY_ORDER = ("recorder", "recorder_type", "vt_kind", "line_no")
ZERO16 = "0x" + "00" * 16


def _uuid_1c(col: str) -> str:
    h = f"CONVERT(char(32),{col},2)"
    return (f"LOWER(SUBSTRING({h},25,8)+'-'+SUBSTRING({h},21,4)+'-'+SUBSTRING({h},17,4)"
            f"+'-'+SUBSTRING({h},1,4)+'-'+SUBSTRING({h},5,12))")


def _maps(pg, source_id: int) -> Dict[str, Tuple[str, str, bool]]:
    return {tc: (sc, tt, bool(ex)) for tc, sc, tt, ex in pg.get_records(
        "SELECT target_column, source_column, transform_type, coalesce(is_expression, false) "
        "FROM etl_meta.column_mappings WHERE source_id = %s AND is_active", parameters=(source_id,))}


def _expr(m: Tuple[str, str, bool], alias: str) -> str:
    sc, _, is_expr = m
    return sc.replace("{alias}", alias) if is_expr else f"[{alias}].[{sc}]"


def plan(pg, spec) -> Dict:
    """Что и как сверять — из конфигурации движка, метаданных ссылок и конфигурации цели."""
    from ..config import refs
    from ..etl_engine import ETLEngine

    with contextlib.redirect_stdout(io.StringIO()):
        engine = ETLEngine(spec.source_object, mode="incremental",
                           config_conn_id="etl_prod", dst_conn_id="etl_prod")
    tname = spec.source_params.get("target")
    target = next(t for t in engine._get_active_targets() if t.target_table == tname)
    if target.union_config:
        roots = [(m.source, m.where_clause) for m in target.union_config.members]
    else:
        roots = [(target.source_config, None)]
    if target.target_role == "dimension" and roots[0][0].source_type == "standalone":
        raise RuntimeError(f"{spec.code}: шапка собрана из строк регистра — сверяется через строки")

    tcols = set(spec.target_columns)
    ctype = {c.target_column: c.target_type for c in spec.columns}
    keys = [k for k in KEY_ORDER if k in tcols]
    measures = [m for m in spec.measure_columns]
    # ссылка → колонка цели: у строк продаж ссылки шапки переименованы (line_*) —
    # берём ту колонку цели, чей source_expr указывает на fk ссылки
    links = []
    for l in refs.dim_links(pg, target.id):
        col = next((c.target_column for c in spec.columns if c.source_expr == l["fk_col"]), None)
        if col in tcols:
            links.append({"key": l["key"], "dim": l["dim_table"], "col": col})

    members, seen = [], set()
    for src, member_where in roots:
        own = _maps(pg, src.id)
        parent = src.parent_source if src.source_type == "detail" else None
        pm = _maps(pg, parent.id) if parent else {}

        def col(tc):
            if tc in own:
                return _expr(own[tc], "s"), own[tc][1]
            if tc in pm:
                return _expr(pm[tc], "p"), pm[tc][1]
            return None, None

        frm = f"[{src.mssql_schema or 'dbo'}].[{src.mssql_table}] AS [s] WITH (NOLOCK)"
        where = []
        if parent:
            frm += (f" INNER JOIN [{parent.mssql_schema or 'dbo'}].[{parent.mssql_table}] AS [p] WITH (NOLOCK)"
                    f" ON [p].[{src.join_key_parent}] = [s].[{src.join_key_source}]")
            if parent.where_clause:
                where.append(parent.where_clause.replace("{alias}", "p"))
        for w in (src.where_clause, member_where):
            if w:
                where.append(w.replace("{alias}", "s"))
        kx = []
        for k in keys:
            e, tt = col(k)
            if e is None:
                raise RuntimeError(f"{spec.code}: у источника {src.mssql_table} нет ключа {k}")
            t = ctype.get(k, "")
            kx.append(_uuid_1c(e) if tt == "binary_to_uuid" or t == "UUID"
                      else f"CAST({e} AS nvarchar(4000))" if "String" in t
                      else f"CAST(CAST({e} AS bigint) AS varchar(20))")
        # ссылка, которой у члена нет (у услуг заказа — качества), — пустая ссылка 1С;
        # в цели у таких строк id 0, и через реплику справочника она тоже даёт нулевой guid
        rx = []
        for l in links:
            e, _ = col(f"raw_refs.{l['key']}")
            rx.append(_uuid_1c(f"ISNULL({e}, {ZERO16})") if e is not None else f"'{EMPTY}'")
            if e is not None:
                seen.add(l["key"])
        mx = {}
        for m in measures:
            e, _ = col(m)
            if e is not None:
                mx[m] = e
        pe, _ = col("period")
        if pe is None:
            raise RuntimeError(f"{spec.code}: у источника {src.mssql_table} нет period")
        members.append({"from": frm, "where": where, "keys": kx, "refs": rx, "measures": mx, "period": pe})

    # мера сверяется, если она есть хотя бы у одного члена; у остальных она 0
    measures = [m for m in measures if any(m in mb["measures"] for mb in members)]
    for mb in members:
        mb["measures"] = [mb["measures"].get(m, "CAST(0 AS decimal(38,8))") for m in measures]
    # ссылка, которой нет ни у одного члена, в источнике не существует — не сверяется
    keep = [i for i, l in enumerate(links) if l["key"] in seen]
    for mb in members:
        mb["refs"] = [mb["refs"][i] for i in keep]
    return {"keys": keys, "refs": [links[i] for i in keep], "measures": measures, "members": members,
            "history_from": (spec.source_params or {}).get("history_from")}


def prehistory(p: Dict, partition: str) -> bool:
    """Партиция до начала полной истории частична по определению — с месяцем 1С не сверяется."""
    h = str(p.get("history_from") or "")
    return bool(h) and int(partition) < int(h[:4] + h[5:7])


def fingerprint_1c(ms, p: Dict, partition: str) -> List[str]:
    y, m = int(partition[:4]), int(partition[4:])
    a = f"{y + 2000:04d}-{m:02d}-01"
    b = f"{y + 2000 + (m == 12):04d}-{(1 if m == 12 else m + 1):02d}-01"
    total = None
    for mb in p["members"]:
        canon = "CONCAT_WS('|', " + ", ".join(mb["keys"] + mb["refs"]) + ")"
        # Построчно во внутреннем запросе, агрегаты — во внешнем: ссылка или мера может быть
        # подзапросом (ссылка из документа-источника), а SQL Server не допускает агрегат
        # над выражением с подзапросом.
        inner = [f"CONVERT(bigint, CONVERT(binary(4), SUBSTRING(HASHBYTES('MD5', "
                 f"CAST(({canon}) COLLATE {UTF8} AS varchar(4000))), 1, 4))) AS h"]
        inner += [f"CAST({e} AS decimal(38,8)) AS m{i}" for i, e in enumerate(mb["measures"])]
        meas = ", ".join(f"ISNULL(SUM(m{i}),0), ISNULL(SUM(m{i}*m{i}),0)" for i in range(len(mb["measures"])))
        # окно партиции — строго исключительное справа: документ ровно на полуночи 1-го
        # числа относится к следующему месяцу, как и в пересборке прямого пути
        cond = [f"{mb['period']} >= '{a}'", f"{mb['period']} < '{b}'"] + [f"({w})" for w in mb["where"]]
        sql = (f"SELECT COUNT(*), ISNULL(SUM(h),0)" + (f", {meas}" if meas else "") +
               f" FROM (SELECT {', '.join(inner)} FROM {mb['from']} WHERE " + " AND ".join(cond) + ") x")
        row = [Decimal(str(x if x is not None else 0)) for x in ms.get_first(sql)]
        total = row if total is None else [u + v for u, v in zip(total, row)]
    return [format(x, "f") for x in total]


def fingerprint_ch(ch, spec, p: Dict, partition: str) -> List[str]:
    parts, joins = [], []
    for k in p["keys"]:
        parts.append(f"lower(toString(f.{k}))" if k == "recorder" else f"toString(f.{k})")
    for i, r in enumerate(p["refs"]):
        joins.append(f"LEFT JOIN {spec.target_database}.{r['dim']} d{i} ON d{i}.id = f.{r['col']}")
        parts.append(f"lower(toString(d{i}.guid))")   # нет соответствия → нулевой uuid, как пустая ссылка 1С
    canon = "concat(" + ", '|', ".join(parts) + ")"
    h = f"sum(reinterpretAsUInt32(reverse(unhex(substring(lower(hex(MD5({canon}))),1,8)))))"
    meas = ", ".join(f"sum(f.{m}), sum(toDecimal128(f.{m},8)*toDecimal128(f.{m},8))" for m in p["measures"])
    sql = (f"SELECT count(), {h}" + (f", {meas}" if meas else "") +
           f" FROM {spec.fqn} f {' '.join(joins)} WHERE {spec.partition_expr.replace('period', 'f.period')} = {int(partition)}")
    return ch.row(sql)


def compare(a: List[str], b: List[str], p: Dict) -> List[Tuple[str, str, str]]:
    names = ["строк", "контрольная сумма"] + [x for m in p["measures"] for x in (f"sum({m})", f"sum({m}²)")]
    def n(v):
        d = Decimal(str(v)).normalize(); return format(abs(d) if d == 0 else d, "f")
    return [(nm, n(a[i]), n(b[i])) for i, nm in enumerate(names) if n(a[i]) != n(b[i])]
