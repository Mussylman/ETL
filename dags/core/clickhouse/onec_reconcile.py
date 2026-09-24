"""
Независимая сверка 1С ↔ ClickHouse — напрямую, без PostgreSQL.

Отпечаток партиции считается дважды независимыми реализациями: на стороне 1С —
выражениями T-SQL над физическими полями регистра, на стороне ClickHouse — над
опубликованной целью. Выражения 1С не зашиты: они выводятся из column_mappings
основного источника регистра (source_type='standalone') по типу преобразования:

    binary_to_uuid  → uuid в порядке байт core.transform.binary_to_uuid
    binary_to_int   → CAST(... AS int)
    fix_year        → окно партиции со сдвигом +2000 к году
    без преобразования → поле как есть (меры)

В контрольную сумму идут только колонки, которые воспроизводимы по одной строке
регистра: ключ строки и ссылки, лежащие на самой строке. Ссылка сравнивается по
guid — на стороне ClickHouse id разворачивается в guid через реплику справочника,
то есть проверяется вся цепочка «ссылка 1С → реестр → id → справочник».

Меры сравниваются через sum и sum квадратов, как везде в движке.
"""

from decimal import Decimal
from typing import Dict, List, Optional, Tuple

EMPTY = "00000000-0000-0000-0000-000000000000"
UTF8 = "Latin1_General_100_BIN2_UTF8"


def _uuid_1c(col: str) -> str:
    h = f"CONVERT(char(32),{col},2)"
    return (f"LOWER(SUBSTRING({h},25,8)+'-'+SUBSTRING({h},21,4)+'-'+SUBSTRING({h},17,4)"
            f"+'-'+SUBSTRING({h},1,4)+'-'+SUBSTRING({h},5,12))")


def plan(pg, spec) -> Dict:
    """Что и как сверять — из метаданных регистра и конфигурации цели."""
    from ..config import refs
    reg = spec.source_object
    src = pg.get_first("""SELECT s.id, s.mssql_table FROM etl_meta.register_sources s
        JOIN etl_meta.registers r ON r.id = s.register_id
        WHERE r.code = %s AND s.source_type = 'standalone' AND s.is_active""", parameters=(reg,))
    if not src:
        raise RuntimeError(f"{spec.code}: у регистра {reg} нет основного (standalone) источника")
    maps = {tc: (sc, tt) for tc, sc, tt in pg.get_records(
        "SELECT target_column, source_column, transform_type FROM etl_meta.column_mappings "
        "WHERE source_id = %s AND is_active AND NOT coalesce(is_expression, false)", parameters=(src[0],))}
    tcols = set(spec.target_columns)
    keys = [k for k in ("recorder", "recorder_type", "line_no") if k in maps and k in tcols]
    # ссылки на справочники, лежащие на самой строке регистра и присутствующие в цели
    tid = pg.get_first("SELECT t.id FROM etl_meta.register_targets t JOIN etl_meta.registers r "
                       "ON r.id=t.register_id WHERE r.code=%s AND t.target_table=%s",
                       parameters=(reg, spec.source_params.get("target")))
    ref = []
    for l in (refs.dim_links(pg, tid[0]) if tid else []):
        m = maps.get(f"raw_refs.{l['key']}")
        target_col = l["fk_col"]
        # у строк продаж ссылки шапки переименованы (line_*) — берём ту колонку цели,
        # чей source_expr указывает на ссылку строки
        col = next((c.target_column for c in spec.columns if c.source_expr == target_col), None)
        if m and col in tcols:
            ref.append({"key": l["key"], "src": m[0], "dim": l["dim_table"], "col": col})
    measures = [(m, maps[m][0]) for m in spec.measure_columns if m in maps]
    period = maps.get("period")
    return {"table": src[1], "keys": [(k, maps[k][0], maps[k][1]) for k in keys], "refs": ref,
            "measures": measures, "period": period[0] if period else "_Period"}


def fingerprint_1c(ms, p: Dict, partition: str) -> List[str]:
    y, m = int(partition[:4]), int(partition[4:])
    a = f"{y + 2000:04d}-{m:02d}-01"
    b = f"{y + 2000 + (m == 12):04d}-{(1 if m == 12 else m + 1):02d}-01"
    parts = []
    for _, sc, tt in p["keys"]:
        parts.append(_uuid_1c(sc) if tt == "binary_to_uuid" else f"CAST(CAST({sc} AS bigint) AS varchar(20))"
                     if tt in ("binary_to_int", None) else sc)
    for r in p["refs"]:
        parts.append(_uuid_1c(r["src"]))
    canon = "CONCAT_WS('|', " + ", ".join(parts) + ")"
    h = (f"SUM(CONVERT(bigint, CONVERT(binary(4), SUBSTRING(HASHBYTES('MD5', "
         f"CAST(({canon}) COLLATE {UTF8} AS varchar(4000))), 1, 4))))")
    meas = ", ".join(f"ISNULL(SUM({sc}),0), ISNULL(SUM(CAST({sc} AS decimal(38,8))*CAST({sc} AS decimal(38,8))),0)"
                     for _, sc in p["measures"])
    # окно партиции — строго исключительное справа: документ ровно на полуночи 1-го
    # числа относится к следующему месяцу, как и в пересборке прямого пути
    sql = (f"SELECT COUNT(*), {h}" + (f", {meas}" if meas else "") +
           f" FROM {p['table']} WITH (NOLOCK) WHERE {p['period']} >= '{a}' AND {p['period']} < '{b}'")
    return [str(x) for x in ms.get_first(sql)]


def fingerprint_ch(ch, spec, p: Dict, partition: str) -> List[str]:
    parts, joins = [], []
    for k, _, _ in p["keys"]:
        parts.append(f"lower(toString(f.{k}))" if k == "recorder" else f"toString(f.{k})")
    for i, r in enumerate(p["refs"]):
        joins.append(f"LEFT JOIN {spec.target_database}.{r['dim']} d{i} ON d{i}.id = f.{r['col']}")
        parts.append(f"lower(toString(d{i}.guid))")   # нет соответствия → нулевой uuid, как пустая ссылка 1С
    canon = "concat(" + ", '|', ".join(parts) + ")"
    h = f"sum(reinterpretAsUInt32(reverse(unhex(substring(lower(hex(MD5({canon}))),1,8)))))"
    meas = ", ".join(f"sum(f.{m}), sum(toDecimal128(f.{m},8)*toDecimal128(f.{m},8))" for m, _ in p["measures"])
    sql = (f"SELECT count(), {h}" + (f", {meas}" if meas else "") +
           f" FROM {spec.fqn} f {' '.join(joins)} WHERE {spec.partition_expr.replace('period', 'f.period')} = {int(partition)}")
    return ch.row(sql)


def compare(a: List[str], b: List[str], p: Dict) -> List[Tuple[str, str, str]]:
    names = ["строк", "контрольная сумма"] + [x for m, _ in p["measures"] for x in (f"sum({m})", f"sum({m}²)")]
    def n(v):
        d = Decimal(str(v)).normalize(); return format(abs(d) if d == 0 else d, "f")
    return [(nm, n(a[i]), n(b[i])) for i, nm in enumerate(names) if n(a[i]) != n(b[i])]
