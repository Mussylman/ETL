"""
Реестр ключей: guid или natural key документа → стабильный суррогатный id.

Это единственное, что прямой путь в ClickHouse не может взять на себя. У ClickHouse
нет ни sequence, ни IDENTITY, ни транзакционной уникальности: выдать id атомарно и
гарантировать, что один guid не получит два разных id при параллельной загрузке, он
не может. Поэтому id выдаёт PostgreSQL, а ClickHouse получает уже готовые числа.

Справочники: stub-строка по первому появлению guid (INSERT ... ON CONFLICT (guid)
DO NOTHING) — ровно тот же механизм, что у post_load_sql фактов. Id выдаётся один
раз и не меняется никогда: на него ссылается BI.

Документы: id шапки по (recorder, recorder_type). Нужен потому, что факты
ссылаются на другие факты — возврат на исходную реализацию (doc_sale_id), продажа
на заказ (zakaz_id). Без реестра документов этим ссылкам не на что указывать.

Связи «какая ссылка → какой справочник/регистр» не описываются здесь: они приходят
из core.config.refs, то есть из тех же метаданных, что и у старого пути.
"""

import json
from typing import Dict, Iterable, List, Optional, Tuple

EMPTY_REF = "00000000-0000-0000-0000-000000000000"


def _ref(raw, key):
    """Значение raw_refs[key]; raw_refs может прийти dict'ом или JSON-строкой."""
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None
    return raw.get(key) if isinstance(raw, dict) else None


def _norm_uuid(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().lower()
    return None if not s or s == EMPTY_REF else s


def dim_ids(pg, dim_table: str, guids: Iterable[str], create_stubs: bool = True) -> Dict[str, int]:
    """guid → id. Незнакомые guid получают stub-строку, если create_stubs."""
    uniq = sorted({g for g in (_norm_uuid(x) for x in guids) if g})
    if not uniq:
        return {}
    if create_stubs:
        # тот же механизм, что у post_load_sql: id из IDENTITY, is_stub=true по умолчанию
        pg.run(f"INSERT INTO public.{dim_table} (guid) SELECT unnest(%s::uuid[]) "
               f"ON CONFLICT (guid) DO NOTHING", parameters=(uniq,))
    rows = pg.get_records(f"SELECT guid::text, id FROM public.{dim_table} WHERE guid = ANY(%s::uuid[])",
                          parameters=(uniq,))
    return {g: int(i) for g, i in rows}


def doc_ids(pg, table: str, pairs: Iterable[Tuple[str, int]]) -> Dict[Tuple[str, int], int]:
    """(recorder, recorder_type) → id шапки документа."""
    uniq = sorted({(u, int(t)) for u, t in pairs if u is not None and t is not None})
    if not uniq:
        return {}
    rows = pg.get_records(
        f"SELECT d.recorder::text, d.recorder_type, d.id FROM public.{table} d "
        f"JOIN unnest(%s::uuid[], %s::int[]) AS k(r, t) ON d.recorder = k.r AND d.recorder_type = k.t",
        parameters=([u for u, _ in uniq], [t for _, t in uniq]))
    return {(r, int(t)): int(i) for r, t, i in rows}


def resolve(pg, df, dim_links: List[Dict], doc_links: List[Dict], *,
            own_table: Optional[str] = None, create_stubs: bool = True):
    """
    Проставляет во фрейме суррогатные id по метаданным ссылок.

      dim_links  — [{key, dim_table, fk_col}]: raw_refs.<key> → справочник
      doc_links  — [{key, ref_table, fk_col}]: raw_refs.<key>.{uid,type} → документ регистра
      own_table  — шапка своего регистра: колонка id по (recorder, recorder_type)

    Нет соответствия → 0. Пустая ссылка 1С в raw_refs не пишется, поэтому и её id 0:
    это семантический NULL, а не дыра.
    """
    if df.empty:
        return df
    refs = df["raw_refs"] if "raw_refs" in df.columns else None

    for l in dim_links:
        vals = refs.map(lambda r, k=l["key"]: _norm_uuid(_ref(r, k))) if refs is not None else None
        if vals is None:
            df[l["fk_col"]] = 0
            continue
        m = dim_ids(pg, l["dim_table"], vals.dropna().tolist(), create_stubs=create_stubs)
        df[l["fk_col"]] = vals.map(lambda g: m.get(g, 0) if g else 0).astype("int64")

    for l in doc_links:
        if refs is None:
            df[l["fk_col"]] = 0
            continue
        vals = refs.map(lambda r, k=l["key"]: _ref(r, k))

        def key_of(v):
            if isinstance(v, dict) and v.get("uid") is not None and v.get("type") is not None:
                u = _norm_uuid(v["uid"])
                return (u, int(v["type"])) if u else None
            return None
        keys = vals.map(key_of)
        m = doc_ids(pg, l["ref_table"], [k for k in keys if k])
        df[l["fk_col"]] = keys.map(lambda k: m.get(k, 0) if k else 0).astype("int64")

    if own_table and {"recorder", "recorder_type"} <= set(df.columns):
        pairs = list(zip(df["recorder"].map(_norm_uuid), df["recorder_type"].astype(int)))
        m = doc_ids(pg, own_table, pairs)
        df["id"] = [m.get(p, 0) for p in pairs]
    return df
