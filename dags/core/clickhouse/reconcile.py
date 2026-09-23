"""
Сверка источника и ClickHouse.

Ответственность разделена намеренно, потому что одной формулой всё не покрыть:

  checksum_columns — ключи и измерения. MD5 по канонической строке, просуммированный
      по партиции. Сумма порядко-независима, поэтому сравнима между СУБД.
      Ловит то, чего НЕ ловят SUM: изменение dimension/id при неизменных мерах,
      переезд строки между партициями, удаление строки.
      Проверено на живых данных: на партиции 202608 из 76 194 строк формула нашла
      ровно одну расходящуюся — документ, перепроведённый в 1С уже после загрузки,
      у которого поменялся podrazdelenie_id, а все суммы остались прежними.

  measure_columns — меры. sum(x) и sum(x*x). Компенсирующие правки, сохраняющие
      обе величины сразу, практически невозможны.
      Меры в MD5 сознательно НЕ включены: текстовое представление numeric в
      PostgreSQL и Decimal в ClickHouse расходится хвостовыми нулями, и
      контрольная сумма ломалась бы на форматировании, а не на данных.

Утверждать, что обычных SUM достаточно, нельзя — это проверено и опровергнуто.
"""

from decimal import Decimal
from typing import Dict, List, Optional, Tuple

# Первые 8 hex-символов MD5 → беззнаковое 32-битное. Одинаково во всех трёх диалектах.
_PG_HASH = "sum(('x' || substr(md5({canon}), 1, 8))::bit(32)::bigint)"
_MS_HASH = ("SUM(CONVERT(bigint, CONVERT(binary(4), "
            "SUBSTRING(HASHBYTES('MD5', {canon}), 1, 4))))")
_CH_HASH = "sum(reinterpretAsUInt32(reverse(unhex(substring(lower(hex(MD5({canon}))), 1, 8)))))"


def _types(spec) -> Dict[str, str]:
    return {c.target_column: c.target_type for c in spec.columns}


# Нормализуется ТИП колонки, а не вся строка целиком. Сплошной lower() применять
# нельзя: в ClickHouse он работает только по ASCII, а в PostgreSQL по Unicode —
# на кириллических названиях справочников канонические строки расходятся, хотя
# данные одинаковы. Регистр нужно править только у UUID: MSSQL отдаёт их
# заглавными, PostgreSQL и ClickHouse — строчными.
def _canon_pg(spec, cols: List[str]) -> str:
    t = _types(spec)
    parts = [f"lower({c}::text)" if t.get(c, "").startswith("UUID") else f"{c}::text"
             for c in cols]
    return "concat_ws('|', " + ", ".join(parts) + ")"


def _canon_ms(spec, cols: List[str]) -> str:
    t = _types(spec)
    parts = [(f"LOWER(CONVERT(nvarchar(64), {c}))" if t.get(c, "").startswith("UUID")
              else f"CONVERT(nvarchar(64), {c})") for c in cols]
    return "CONCAT_WS('|', " + ", ".join(parts) + ")"


def _canon_ch(spec, cols: List[str]) -> str:
    t = _types(spec)
    parts = [f"lower(toString({c}))" if t.get(c, "").startswith("UUID") else f"toString({c})"
             for c in cols]
    return "concat(" + ", '|', ".join(parts) + ")"


def fingerprint_sql(spec, dialect: str, where: str = "") -> str:
    """
    Один запрос, отдающий отпечаток партиции: строки, контрольная сумма,
    суммы мер и суммы их квадратов. Порядок полей одинаков во всех диалектах —
    сравнение позиционное.
    """
    parts = ["count(*)" if dialect != "clickhouse" else "count()"]

    if spec.checksum_columns:
        if dialect == "postgres":
            parts.append(_PG_HASH.format(canon=_canon_pg(spec, spec.checksum_columns)))
        elif dialect == "mssql":
            parts.append(_MS_HASH.format(canon=_canon_ms(spec, spec.checksum_columns)))
        else:
            parts.append(_CH_HASH.format(canon=_canon_ch(spec, spec.checksum_columns)))
    else:
        parts.append("0")

    for m in spec.measure_columns:
        if dialect == "postgres":
            parts.append(f"coalesce(sum({m}), 0)")
            parts.append(f"coalesce(sum(({m})::numeric * ({m})::numeric), 0)")
        elif dialect == "mssql":
            parts.append(f"ISNULL(SUM({m}), 0)")
            parts.append(f"ISNULL(SUM(CAST({m} AS decimal(38,8)) * CAST({m} AS decimal(38,8))), 0)")
        else:
            parts.append(f"sum({m})")
            parts.append(f"sum(toDecimal128({m}, 8) * toDecimal128({m}, 8))")

    return "SELECT " + ", ".join(parts)


def labels(spec) -> List[str]:
    out = ["строк", "контрольная сумма"]
    for m in spec.measure_columns:
        out.append(f"sum({m})")
        out.append(f"sum({m}^2)")
    return out


def _norm(i: int, v) -> str:
    """Каноническое представление метрики. i<2 — счётчики, дальше — числа."""
    if v is None:
        return "0"
    if i < 2:
        return str(int(v))
    d = Decimal(str(v)).normalize()
    return format(abs(d) if d == 0 else d, "f")


def compare(spec, src_row, dst_row) -> List[Tuple[str, str, str]]:
    """Расхождения источника и цели. Пустой список = отпечатки совпали полностью."""
    out = []
    for i, name in enumerate(labels(spec)):
        a = _norm(i, src_row[i] if i < len(src_row) else None)
        b = _norm(i, dst_row[i] if i < len(dst_row) else None)
        if a != b:
            out.append((name, a, b))
    return out


def duplicates_sql(spec, table: str, where: str = "") -> Optional[str]:
    """Дубли бизнес-ключа в ClickHouse. None, если ключ в конфигурации не задан."""
    if not spec.business_key:
        return None
    keys = ", ".join(spec.business_key)
    nums = ", ".join(str(i + 1) for i in range(len(spec.business_key)))
    return (f"SELECT count() FROM (SELECT {keys} FROM {table}{where} "
            f"GROUP BY {nums} HAVING count() > 1)")


def to_json(spec, src_row, dst_row, diff) -> Dict:
    """Результат сверки для ch_sync_history.reconcile — в том виде, в каком его читать людям."""
    return {
        "metrics": {n: {"source": _norm(i, src_row[i] if i < len(src_row) else None),
                        "target": _norm(i, dst_row[i] if i < len(dst_row) else None)}
                    for i, n in enumerate(labels(spec))},
        "diff": [{"metric": n, "source": a, "target": b} for n, a, b in diff],
        "ok": not diff,
    }


def extra_checks(spec, table: str) -> List[Tuple[str, str]]:
    """
    Проверки, которых не выражают отпечатки: уникальность и обязательные значения.
    Задаются конфигом в reconcile_metrics, а не кодом:
        {"unique": [["id"], ["guid"]], "not_empty": ["guid"]}
    Каждый SQL обязан вернуть 0 — иначе партиция не публикуется.
    """
    m = spec.reconcile_metrics or {}
    out: List[Tuple[str, str]] = []
    for cols in m.get("unique", []):
        keys = ", ".join(cols)
        nums = ", ".join(str(i + 1) for i in range(len(cols)))
        out.append((f"дубли {'+'.join(cols)}",
                    f"SELECT count() FROM (SELECT {keys} FROM {table} "
                    f"GROUP BY {nums} HAVING count() > 1)"))
    for c in m.get("not_zero", []):
        # обогащённая колонка без соответствия в справочнике получает 0 —
        # это не «неизвестно», а несошедшийся lookup, и публиковать его нельзя
        out.append((f"без соответствия {c}", f"SELECT countIf({c} = 0) FROM {table}"))
    for c in m.get("not_empty", []):
        # пустой строкой считается и '', и нулевой uuid 1С — он семантически NULL
        out.append((f"пустые {c}",
                    f"SELECT countIf(toString({c}) IN ('', "
                    f"'00000000-0000-0000-0000-000000000000')) FROM {table}"))
    return out
