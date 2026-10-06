"""
Ссылки цели на справочники и на документы других регистров — из метаданных.

Единственная реализация правила. Им пользуются и конфигуратор (генерирует из него
post_load_sql для PostgreSQL), и прямой путь в ClickHouse (резолвит id через реестр
ключей). Если правило живёт в двух местах, они рано или поздно расходятся — именно
так в PROD post_load_sql оказался впереди метаданных: связи gruzopoluchatel и zakaz
исполнялись, но нигде не были объявлены.

Правило (стандарт raw_refs):
  raw_refs.<key>          → справочник transform_params.dim, иначе dim_<key>, если
                            такая таблица есть; FK-колонка <key>_id. Нет справочника —
                            ссылка живёт только в raw_refs, фейковых *_id не бывает.
  raw_refs.<key>.uid      → документ другого регистра, объявленный явно:
                            transform_params.ref_target; FK-колонка <key>_id.
                            Без декларации — только в raw_refs.
"""

import json
from typing import Dict, List, Optional


def _params(tp) -> dict:
    if isinstance(tp, dict):
        return tp
    if isinstance(tp, str):
        try:
            return json.loads(tp)
        except Exception:
            return {}
    return {}


# Пайплайны, которые собирают цель из ВСЕХ источников регистра, а не из source_id.
# accumrg_with_documents: регистр как основа + LEFT JOIN всех шапок документов по
# _RecorderTRef + всех табличных частей по _LineNo (см. build_accumrg_with_documents).
# Формально цель привязана только к регистру, но колонки вроде sklad и kachestvo
# приходят из шапок и ТЧ — без этого правила их ссылки на справочники терялись бы.
WHOLE_REGISTER_PIPELINES = {"accumrg_with_documents"}


def target_source_ids(pg, target_id: int) -> List[int]:
    """Источники, из которых цель реально берёт колонки."""
    t = pg.get_first(
        "SELECT t.source_id, t.union_id, t.register_id, r.pipeline_type "
        "FROM etl_meta.register_targets t JOIN etl_meta.registers r ON r.id = t.register_id "
        "WHERE t.id = %s", parameters=(target_id,))
    if not t:
        return []
    if t[3] in WHOLE_REGISTER_PIPELINES:
        return [r[0] for r in pg.get_records(
            "SELECT id FROM etl_meta.register_sources WHERE register_id=%s AND is_active ORDER BY id",
            parameters=(t[2],))]
    ids: List[int] = []

    def add(sid):
        if sid and sid not in ids:
            ids.append(sid)
            p = pg.get_first("SELECT parent_source_id FROM etl_meta.register_sources WHERE id=%s",
                             parameters=(sid,))
            if p and p[0]:
                add(p[0])

    if t[0]:
        add(t[0])
    elif t[1]:
        for (sid,) in pg.get_records(
                "SELECT source_id FROM etl_meta.source_union_members WHERE union_id=%s AND is_active",
                parameters=(t[1],)):
            add(sid)
    return ids


def _mappings(pg, target_id: int):
    include = pg.get_first("SELECT include_columns FROM etl_meta.register_targets WHERE id=%s",
                           parameters=(target_id,))
    include = set((include or [None])[0] or [])
    sids = target_source_ids(pg, target_id)
    if not sids:
        return []
    rows = pg.get_records(
        "SELECT target_column, transform_params FROM etl_meta.column_mappings "
        "WHERE source_id = ANY(%s) AND is_active ORDER BY id", parameters=(sids,))
    return [(tc, _params(tp)) for tc, tp in rows if not include or tc in include]


def _dim_exists(pg, table: str, schema: str = "public") -> bool:
    return pg.get_first("SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
                        parameters=(schema, table)) is not None


def dim_links(pg, target_id: int) -> List[Dict]:
    """[{key, dim_table, fk_col}] — ссылки raw_refs.<key> на справочники."""
    keys: Dict[str, Optional[str]] = {}
    for tc, p in _mappings(pg, target_id):
        parts = tc.split(".")
        if not tc.startswith("raw_refs.") or len(parts) != 2:
            continue
        key = parts[1]
        if key not in keys or p.get("dim"):
            keys[key] = p.get("dim")
    out = []
    for key, dim in keys.items():
        table = dim or f"dim_{key}"
        if _dim_exists(pg, table):
            out.append({"key": key, "dim_table": table, "fk_col": f"{key}_id"})
        elif dim:
            # справочник назван в конфиге явно, а таблицы нет — молча потерять ссылку нельзя:
            # факты получили бы *_id = 0 без единой ошибки
            raise RuntimeError(f"raw_refs.{key}: справочник {table} (transform_params.dim) не найден")
    return out


def register_links(pg, target_id: int) -> List[Dict]:
    """[{key, ref_table, fk_col}] — полиморфные ссылки raw_refs.<key>.uid на документы регистров."""
    out: Dict[str, Dict] = {}
    for tc, p in _mappings(pg, target_id):
        if not (tc.startswith("raw_refs.") and tc.endswith(".uid")):
            continue
        ref = p.get("ref_target")
        key = tc.split(".")[1]
        if ref and key not in out:
            out[key] = {"key": key, "ref_table": ref, "fk_col": f"{key}_id"}
    return list(out.values())
