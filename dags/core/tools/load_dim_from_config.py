"""
Универсальная первичная заливка справочника: 1С → dim_*, метка ← retail.

Чем отличается от load_dim_names.py: тот знает ровно две колонки (name, code) —
они зашиты в SQL. Здесь список полей ЧИТАЕТСЯ ИЗ КОНФИГА (etl_meta.column_mappings),
поэтому новый справочник не требует правок кода: привязал источник и поля в UI —
скрипт работает.

Что делает (порядок важен):
  1. MAX(updated_at) из retail-таблицы, UTC→Almaty — ОДНА метка на весь прогон.
     Правило проекта: первичная и перезаливка ставят одну max-дату всем строкам,
     построчно бывает только инкремент.
  2. Из etl_meta читает: источник 1С (register_sources.mssql_table) и ВСЕ поля
     (column_mappings: source_column → target_column + transform_type).
  3. Идёт в 1С по guid, уже имеющимся в dim, и забирает все поля из мэппинга.
  4. Пишет в dim все забранные поля, снимает is_stub, ставит всем ту одну метку.

Строки НЕ создаются: их заводят факты (stub-резолв post_load по guid из 1С).
Скрипт только дозаполняет уже существующие.

Использование:
    source venv/bin/activate
    PYTHONPATH=dags python3 -m core.tools.load_dim_from_config --dim dim_sklad --dry-run
    PYTHONPATH=dags python3 -m core.tools.load_dim_from_config --dim dim_sklad
    PYTHONPATH=dags python3 -m core.tools.load_dim_from_config            # все reference_dim
    PYTHONPATH=dags python3 -m core.tools.load_dim_from_config --mode reload   # поля из 1С, метку не трогает

Параметры:
    --dim       какие справочники (по умолчанию все с pipeline_type='reference_dim')
    --mode      initial     — первичная: поля из 1С + ОДНА max retail-дата всем строкам
                reload      — ресинк полей из 1С; retail_updated_at НЕ трогает (безопасный дефолт)
                incremental — только изменённые в retail, per-row метки
                hierarchy   — догрузить недостающих предков по _ParentIDRRef
    --dry-run   ничего не писать: показать план, поля из конфига, что изменилось бы
    --batch     размер батча uid в IN-списке MSSQL и в execute_values (default 500)
    --set-mark  reload: всё-таки переставить метку всем строкам (с WARNING — см. _resolve_set_mark)
    --no-retail-mark  initial: не трогать retail_updated_at. Для reload устарел, игнорируется.

Почему reload по умолчанию метку не трогает: stamp_all перетирает per-row метки
инкремента одной MAX(updated_at) и сдвигает watermark вперёд. Объект, уже изменённый
в retail, но ещё не обновлённый в 1С, получит чужую позднюю метку — и следующий
инкремент его не увидит. Тихая потеря обновлений без единой ошибки.
"""

import argparse
import sys
import warnings
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

# Трансформации, встречающиеся в конфиге справочников. Значение — функция
# из core.transform.binary. Незнакомый transform_type → падаем с явной
# ошибкой, а не пишем мусор в справочник.
def _transformers() -> Dict[str, Any]:
    from ..transform.binary import (
        binary_to_uuid, binary_to_int, binary_to_bool, process_binary_auto,
    )
    from ..transform.transform_utils import TransformUtils

    def _auto(v):
        # binary_auto — то, что UI ставит по умолчанию: тип выбирается по длине
        # (16 байт → uuid, 4 → int, 1 → bool). uuid приводим к str для psycopg2.
        if v is None:
            return None
        out = process_binary_auto(v)
        return str(out) if isinstance(out, UUID) else out

    return {
        "binary_to_uuid": lambda v: (str(binary_to_uuid(bytes(v))) if v is not None else None),
        "binary_to_int":  lambda v: binary_to_int(bytes(v)) if v is not None else None,
        "binary_to_bool": lambda v: binary_to_bool(bytes(v)) if v is not None else None,
        "binary_auto":    _auto,
        # _Folder в 1С: 0x00 = ГРУППА, 0x01 = элемент — прямой bool даёт
        # значение, обратное смыслу «ЭтоГруппа». Отсюда инверсия.
        "invert_bool":    lambda v: TransformUtils._invert_bool(v),
    }

# Пустая ссылка 1С. Семантически NULL: в справочник не заводится,
# *_id остаётся пустым (см. CLAUDE.md, ловушки).
EMPTY_REF = "00000000-0000-0000-0000-000000000000"

# Колонки dim, которыми управляет не мэппинг, а сам механизм.
MANAGED_COLS = {"id", "is_stub", "etl_updated_at", "retail_updated_at"}

# Watermark-колонка СПРАВОЧНИКОВ. Одна. У фактов их три с fallback-цепочкой
# (retail_updated_at → retail_snapshot_at → updated_at, см. data_checker),
# справочникам snapshot-колонка не нужна и не создаётся миграциями 009/010.
WATERMARK_COL = "retail_updated_at"


def read_config(pg, dim_code: str) -> Optional[dict]:
    """
    Вся привязка справочника — из etl_meta. Ничего не захардкожено.
      registers        → retail_table (источник метки)
      register_sources → mssql_schema/mssql_table (источник данных 1С)
      column_mappings  → какие поля тянуть
    """
    reg = pg.get_first("""
        SELECT r.id, r.code, r.retail_table, t.target_schema, t.target_table, r.retail_uid_column
        FROM   etl_meta.registers r
        LEFT JOIN etl_meta.register_targets t
               ON t.register_id = r.id AND t.is_active AND t.target_role = 'dimension'
        WHERE  r.code = %s AND r.is_active
    """, parameters=(dim_code,))
    if not reg:
        return None

    src = pg.get_first("""
        SELECT id, mssql_schema, mssql_table, onec_name
        FROM   etl_meta.register_sources
        WHERE  register_id = %s AND is_active
        ORDER  BY priority, id LIMIT 1
    """, parameters=(reg[0],))
    if not src:
        return None

    maps = pg.get_records("""
        SELECT source_column, target_column, transform_type
        FROM   etl_meta.column_mappings
        WHERE  source_id = %s AND is_active
        ORDER  BY target_column
    """, parameters=(src[0],))

    return {
        "register_id": reg[0], "code": reg[1], "retail_table": reg[2],
        "retail_uid_column": reg[5],
        "dim_schema": reg[3] or "public", "dim_table": reg[4] or dim_code,
        "mssql_schema": src[1], "mssql_table": src[2], "onec_name": src[3],
        "mappings": [{"src": m[0], "tgt": m[1], "transform": m[2]} for m in maps],
    }


def open_history(pg, register_id: int, run_mode: str) -> Optional[int]:
    """
    Открыть строку в etl_meta.load_history — тот же контракт, что у ETLEngine
    (_open_history_run). Нужен, чтобы UI-портал видел загрузки справочников:
    dao.list_registers берёт last_success_at / last_status именно отсюда, и без
    этой записи портал показывает «никогда не запускался», хотя DAG отработал.
    Ошибка журналирования не должна ронять саму загрузку — отсюда None и warn.
    """
    try:
        rows = pg.get_records(
            "INSERT INTO etl_meta.load_history (register_id, run_mode, status, started_at) "
            "VALUES (%s, %s, 'running', NOW()) RETURNING id",
            parameters=(register_id, run_mode))
        return rows[0][0]
    except Exception as e:
        print(f"    ⚠ не смог открыть load_history: {str(e)[:120]}")
        return None


def close_history(pg, run_id: Optional[int], status: str, rows_loaded: int = 0,
                  checkpoint: Optional[str] = None, error: Optional[str] = None) -> None:
    """Закрыть строку load_history. Молча пропускает, если открыть не удалось."""
    if run_id is None:
        return
    try:
        pg.run(
            "UPDATE etl_meta.load_history "
            "SET status=%s, finished_at=NOW(), rows_loaded=%s, "
            "    checkpoint_value=COALESCE(%s, checkpoint_value), "
            "    error_message=COALESCE(%s, error_message) "
            "WHERE id=%s",
            parameters=(status, rows_loaded, checkpoint, error, run_id))
    except Exception as e:
        print(f"    ⚠ не смог закрыть load_history: {str(e)[:120]}")


def resolve_keys(cfg: dict) -> dict:
    """
    Ключи и watermark-колонка — ИЗ КОНФИГА, не литералами.

    Справочники и факты держатся на разных ключах, поэтому фактовые хардкоды
    (recorder / retail_snapshot_at из data_checker) сюда не тянем — у справочника
    свой контур:
      dwh_key    — колонка dim, по которой сопоставляем. Берётся из мэппинга
                   (column_mappings.target_column), а не из литерала 'guid':
                   если справочник замаплен на другую колонку, механизм это увидит.
      retail_key — registers.retail_uid_column (какая колонка в retail несёт тот же ключ).
      watermark  — колонка dim, по которой считается MAX(). У справочников
                   ровно одна: retail_updated_at. Snapshot-колонки у них НЕТ,
                   и требовать её нельзя (это фактовая конструкция).
    """
    tgt_cols = [m["tgt"] for m in cfg["mappings"]]
    dwh_key = next((c for c in tgt_cols if c in ("guid", "uid")), None)
    if not dwh_key:
        raise RuntimeError(
            f"{cfg['code']}: в column_mappings нет колонки-ключа (guid/uid) — "
            f"по чему сопоставлять строки с retail и 1С?")
    retail_key = cfg.get("retail_uid_column")
    if not retail_key:
        raise RuntimeError(
            f"{cfg['code']}: не заполнен registers.retail_uid_column — "
            f"неизвестно, какая колонка retail несёт ключ")
    return {"dwh_key": dwh_key, "retail_key": retail_key,
            "watermark_col": WATERMARK_COL}


def retail_mark(rt, retail_table: str):
    """ОДНА метка на весь прогон: MAX(updated_at) из retail (UTC→Almaty)."""
    if not retail_table:
        return None
    has_del = rt.get_first(
        "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' "
        "AND table_name=%s AND column_name='deleted_at'", parameters=(retail_table,))[0]
    where = "WHERE deleted_at IS NULL" if has_del else ""
    row = rt.get_first(f"""
        SELECT MAX((updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp)
        FROM   public.{retail_table} {where}
    """)
    return row[0] if row else None


def fetch_from_1c(ms, cfg: dict, guids: List[str], batch: int) -> List[Tuple]:
    """
    Забирает из 1С ВСЕ поля мэппинга по списку guid.
    Ключ сопоставления — guid: ищем ту колонку мэппинга, что ложится в dim.guid.
    """
    from ..transform.binary import uuid_to_mssql_hex_1c

    dwh_key = resolve_keys(cfg)["dwh_key"]
    key_map = next((m for m in cfg["mappings"] if m["tgt"] == dwh_key), None)
    if not key_map:
        raise RuntimeError(
            f"{cfg['code']}: в мэппинге нет колонки, ведущей в guid — "
            f"по чему сопоставлять строки с 1С? (проверьте column_mappings)")

    key_src = key_map["src"]                     # обычно _IDRRef
    cols = [m["src"] for m in cfg["mappings"]]   # все поля, включая ключ
    tf = _transformers()
    for m in cfg["mappings"]:
        if m["transform"] and m["transform"] not in tf:
            raise RuntimeError(
                f"{cfg['code']}.{m['tgt']}: неизвестный transform_type "
                f"'{m['transform']}' — добавьте его в _transformers()")

    select = ", ".join(f"[{c}]" for c in cols)
    table = f"[{cfg['mssql_schema']}].[{cfg['mssql_table']}]"
    out: List[Tuple] = []

    for i in range(0, len(guids), batch):
        hexes = [uuid_to_mssql_hex_1c(g) for g in guids[i:i + batch]]
        hexes = [h for h in hexes if h]
        if not hexes:
            continue
        rows = ms.get_records(
            f"SELECT {select} FROM {table} WITH (NOLOCK) "
            f"WHERE [{key_src}] IN ({','.join(hexes)})")
        for r in rows:
            vals = []
            for m, raw in zip(cfg["mappings"], r):
                v = tf[m["transform"]](raw) if m["transform"] else raw
                if isinstance(v, str):
                    v = v.strip() or None
                # Пустая ссылка 1С — семантический NULL, а не значение:
                # у корней иерархии _ParentIDRRef именно такой. Иначе
                # parent_guid ссылается «в никуда» и выглядит битым FK.
                if v == EMPTY_REF:
                    v = None
                vals.append(v)
            out.append(tuple(vals))
    return out


def _col_types(pg, cfg: dict) -> Dict[str, str]:
    """
    Фактические типы колонок dim из information_schema.

    Нужны, чтобы кастовать значения из `VALUES %s`: psycopg2 отдаёт их как
    text, и сравнение `d."parent_guid" IS DISTINCT FROM v."parent_guid"`
    падает на `operator does not exist: uuid = text`. Пока в справочниках были
    только text-поля, это не всплывало — первая же uuid/boolean-колонка ломает
    загрузку. Source of truth — сама таблица, а не target_type в конфиге.
    """
    rows = pg.get_records(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s",
        parameters=(cfg["dim_schema"], cfg["dim_table"]),
    )
    pg_of = {
        "uuid": "uuid", "boolean": "boolean", "integer": "integer",
        "bigint": "bigint", "numeric": "numeric", "date": "date",
        "timestamp without time zone": "timestamp",
        "double precision": "double precision",
    }
    return {r[0]: pg_of[r[1]] for r in rows if r[1] in pg_of}


def apply_rows(pg, cfg: dict, rows: List[Tuple], mark, set_mark: bool,
               batch: int, dry_run: bool) -> int:
    """
    UPDATE dim: все поля из мэппинга + is_stub=false + одна метка всем.
    Ключ — guid. Строки не создаются (их заводят факты).
    """
    if not rows:
        return 0
    from psycopg2.extras import execute_values

    key = resolve_keys(cfg)["dwh_key"]
    targets = [m["tgt"] for m in cfg["mappings"]]
    data_cols = [c for c in targets if c != key and c not in MANAGED_COLS]
    if not data_cols:
        raise RuntimeError(f"{cfg['code']}: в мэппинге нет полей данных кроме ключа {key}")

    guid_pos = targets.index(key)
    ordered = [key] + data_cols
    idx = [guid_pos] + [targets.index(c) for c in data_cols]
    payload = [tuple(r[i] for i in idx) for r in rows]

    types = _col_types(pg, cfg)
    def _v(c: str) -> str:
        t = types.get(c)
        return f'v."{c}"::{t}' if t else f'v."{c}"'

    set_parts = [f'"{c}" = {_v(c)}' for c in data_cols]
    set_parts.append("is_stub = false")
    set_parts.append("etl_updated_at = timezone('Asia/Almaty', now())")
    if set_mark and mark is not None:
        set_parts.append(f"{WATERMARK_COL} = %(mark)s")

    # обновляем только если что-то реально меняется — идемпотентность
    diff = " OR ".join([f'd."{c}" IS DISTINCT FROM {_v(c)}' for c in data_cols] + ["d.is_stub"])
    vcols = ", ".join(f'"{c}"' for c in ordered)

    sql = f'''
        UPDATE {cfg["dim_schema"]}.{cfg["dim_table"]} d
        SET    {", ".join(set_parts)}
        FROM  (VALUES %s) AS v({vcols})
        WHERE  d.{key} = v."{key}"::uuid AND ({diff})
    '''
    if set_mark and mark is not None:
        sql = sql.replace("%(mark)s", "'" + str(mark) + "'::timestamp")

    conn = pg.get_conn()
    total = 0
    try:
        for i in range(0, len(payload), batch):
            with conn.cursor() as cur:
                execute_values(cur, sql, payload[i:i + batch], page_size=batch)
                total += max(cur.rowcount, 0)
            conn.rollback() if dry_run else conn.commit()
        return total
    finally:
        conn.close()


def stamp_all(pg, cfg: dict, mark, dry_run: bool) -> int:
    """Одна метка ВСЕМ строкам справочника (в т.ч. тем, кого нет в 1С)."""
    if mark is None:
        return 0
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            key = resolve_keys(cfg)["dwh_key"]
            cur.execute(f'''
                UPDATE {cfg["dim_schema"]}.{cfg["dim_table"]}
                SET    {WATERMARK_COL} = %s
                WHERE  {key} IS NOT NULL
                  AND  {WATERMARK_COL} IS DISTINCT FROM %s::timestamp
            ''', (mark, mark))
            n = cur.rowcount
        conn.rollback() if dry_run else conn.commit()
        return n
    finally:
        conn.close()


def fetch_changed_guids(rt, cfg: dict, since, batch_limit: int = 50000) -> List[str]:
    """
    Режим incremental: guid, которые retail пометил изменёнными ПОСЛЕ метки.

    Watermark — MAX(retail_updated_at) самого справочника, отдельного хранилища
    меток нет (тот же принцип, что в фактах: data_checker.get_last_update).
    """
    retail_table = cfg["retail_table"]
    if not retail_table:
        return []
    uid_col = resolve_keys(cfg)["retail_key"]
    has_del = rt.get_first(
        "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' "
        "AND table_name=%s AND column_name='deleted_at'", parameters=(retail_table,))[0]

    where = [f"{uid_col} IS NOT NULL", "updated_at IS NOT NULL"]
    if has_del:
        where.append("deleted_at IS NULL")
    params: tuple = ()
    if since is not None:
        where.append("(updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp > %s")
        params = (since,)

    rows = rt.get_records(
        f"SELECT DISTINCT lower({uid_col}) FROM public.{retail_table} "
        f"WHERE {' AND '.join(where)} LIMIT {int(batch_limit)}",
        parameters=params or None)
    return [r[0] for r in rows]


def process_incremental(cfg: dict, pg, rt, ms, batch: int, dry_run: bool) -> dict:
    """
    Инкремент справочника: retail говорит ЧТО изменилось → идём в 1С за полями
    → пишем поля + PER-ROW метку (единственный режим, где метка построчная).

    Обрабатываются только guid, которые УЖЕ есть в dim: строки заводят факты
    через stub-резолв, справочник их не создаёт.
    """
    keys = resolve_keys(cfg)
    dim_full = f'{cfg["dim_schema"]}.{cfg["dim_table"]}'
    print(f"    ключи из конфига: dwh={keys['dwh_key']} ← retail={keys['retail_key']} "
          f"| watermark-колонка: {keys['watermark_col']}")
    watermark = pg.get_first(
        f"SELECT MAX({keys['watermark_col']}) FROM {dim_full}")[0]
    changed = fetch_changed_guids(rt, cfg, watermark)
    if not changed:
        print(f"    watermark {watermark} | изменений в retail 0 — нечего делать")
        return {"dim": cfg["code"], "changed": 0, "in_dim": 0, "touched": 0,
                "watermark": watermark}

    # оставляем только те guid, что есть в справочнике
    k = keys["dwh_key"]
    present = {r[0] for r in pg.get_records(
        f"SELECT {k}::text FROM {dim_full} WHERE {k} = ANY(%s::uuid[])",
        parameters=(changed,))}
    guids = [g for g in changed if g in present]
    if not guids:
        print(f"    watermark {watermark} | изменений в retail {len(changed)} | "
              f"из них есть в dim 0 — новые объекты придут с фактами")
        return {"dim": cfg["code"], "changed": len(changed), "in_dim": 0,
                "touched": 0, "watermark": watermark}

    # поля из 1С + per-row метки из retail
    rows = fetch_from_1c(ms, cfg, guids, batch)
    marks = dict(fetch_retail_marks(rt, cfg, guids))
    touched = apply_rows_per_row(pg, cfg, rows, marks, batch, dry_run)

    print(f"    watermark {watermark} | изменений в retail {len(changed)} | "
          f"из них в dim {len(guids)} | найдено в 1С {len(rows)} | "
          f"{'обновилось бы' if dry_run else 'обновлено'} {touched}")
    return {"dim": cfg["code"], "changed": len(changed), "in_dim": len(guids),
            "touched": touched, "watermark": watermark}


def fetch_retail_marks(rt, cfg: dict, guids: List[str]) -> List[Tuple[str, Any]]:
    """(guid, updated_at) для конкретных guid — для per-row метки в инкременте."""
    uid_col = resolve_keys(cfg)["retail_key"]
    rows = rt.get_records(
        f"SELECT lower({uid_col}), "
        f"       (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp "
        f"FROM public.{cfg['retail_table']} WHERE lower({uid_col}) = ANY(%s)",
        parameters=(guids,))
    return [(r[0], r[1]) for r in rows]


def apply_rows_per_row(pg, cfg: dict, rows: List[Tuple], marks: Dict[str, Any],
                       batch: int, dry_run: bool) -> int:
    """UPDATE полей из 1С + PER-ROW метка (только инкремент)."""
    if not rows:
        return 0
    from psycopg2.extras import execute_values

    key = resolve_keys(cfg)["dwh_key"]
    targets = [m["tgt"] for m in cfg["mappings"]]
    data_cols = [c for c in targets if c != key and c not in MANAGED_COLS]
    guid_pos = targets.index(key)
    idx = [guid_pos] + [targets.index(c) for c in data_cols]
    payload = [tuple(r[i] for i in idx) + (marks.get(r[guid_pos]),) for r in rows]

    types = _col_types(pg, cfg)
    def _v(c: str) -> str:
        t = types.get(c)
        return f'v."{c}"::{t}' if t else f'v."{c}"'

    set_parts = [f'"{c}" = {_v(c)}' for c in data_cols]
    wm = WATERMARK_COL
    set_parts += ["is_stub = false", "etl_updated_at = timezone('Asia/Almaty', now())",
                  f"{wm} = COALESCE(v.mark::timestamp, d.{wm})"]
    diff = " OR ".join([f'd."{c}" IS DISTINCT FROM {_v(c)}' for c in data_cols]
                       + ["d.is_stub",
                          f"d.{wm} IS DISTINCT FROM v.mark::timestamp"])
    vcols = ", ".join([f'"{c}"' for c in [key] + data_cols] + ["mark"])

    sql = f'''
        UPDATE {cfg["dim_schema"]}.{cfg["dim_table"]} d
        SET    {", ".join(set_parts)}
        FROM  (VALUES %s) AS v({vcols})
        WHERE  d.{key} = v."{key}"::uuid AND ({diff})
    '''
    conn = pg.get_conn()
    total = 0
    try:
        for i in range(0, len(payload), batch):
            with conn.cursor() as cur:
                execute_values(cur, sql, payload[i:i + batch], page_size=batch)
                total += max(cur.rowcount, 0)
            conn.rollback() if dry_run else conn.commit()
        return total
    finally:
        conn.close()


def parent_col(cfg: dict) -> Optional[str]:
    """
    Колонка dim, хранящая ссылку на родителя, — по мэппингу, а не по имени.

    Ищем `_ParentIDRRef`: это имя платформы 1С у любого иерархического
    справочника, а не наша конвенция, поэтому опознаётся у всех dim одинаково.
    Нет такого мэппинга → справочник плоский, иерархию грузить нечего.
    """
    m = next((m for m in cfg["mappings"] if m["src"].lower() == "_parentidrref"), None)
    return m["tgt"] if m else None


def insert_rows(pg, cfg: dict, rows: List[Tuple], batch: int, dry_run: bool) -> int:
    """
    INSERT недостающих строк справочника (ON CONFLICT guid DO NOTHING).

    Отступление от общего правила «строки в dim заводят факты»: группы 1С
    никогда не продаются, а без них self-ссылка parent_guid никуда не ведёт —
    бренд и категорию товара не достать. id выдаётся IDENTITY и, как у всех
    строк dim, не меняется больше никогда.
    """
    if not rows:
        return 0
    from psycopg2.extras import execute_values

    cols = [m["tgt"] for m in cfg["mappings"]]
    key = resolve_keys(cfg)["dwh_key"]
    types = _col_types(pg, cfg)
    placeholders = ", ".join(
        "%s::" + types[c] if c in types else "%s" for c in cols)
    collist = ", ".join('"' + c + '"' for c in cols)
    dim = cfg["dim_schema"] + "." + cfg["dim_table"]

    sql = (
        "INSERT INTO " + dim + " (" + collist + ", is_stub, etl_updated_at) "
        "VALUES %s ON CONFLICT (" + key + ") DO NOTHING"
    )
    template = "(" + placeholders + ", false, timezone('Asia/Almaty', now()))"

    conn = pg.get_conn()
    total = 0
    try:
        for i in range(0, len(rows), batch):
            with conn.cursor() as cur:
                execute_values(cur, sql, rows[i:i + batch],
                               template=template, page_size=batch)
                total += max(cur.rowcount, 0)
            conn.rollback() if dry_run else conn.commit()
        return total
    finally:
        conn.close()


def process_hierarchy(cfg: dict, pg, ms, batch: int, dry_run: bool) -> dict:
    """
    Догружает предков, на которых ссылается сам справочник, — до корня.

    Каждый проход: берём parent-ссылки, которых нет среди guid справочника,
    достаём эти строки из 1С, вставляем. У вставленных снова есть родители —
    повторяем, пока не перестанут появляться новые. Так иерархия любой глубины
    сходится сама, без знания о том, сколько в ней уровней.
    """
    pcol = parent_col(cfg)
    if not pcol:
        print("    в мэппинге нет _ParentIDRRef — справочник плоский, иерархии нет")
        return {"dim": cfg["code"], "touched": 0, "levels": 0}

    dim = cfg["dim_schema"] + "." + cfg["dim_table"]
    total, level = 0, 0
    while True:
        level += 1
        missing = [r[0] for r in pg.get_records(
            'SELECT DISTINCT d."' + pcol + '"::text FROM ' + dim + ' d '
            'LEFT JOIN ' + dim + ' p ON p.guid = d."' + pcol + '" '
            'WHERE d."' + pcol + '" IS NOT NULL AND p.guid IS NULL')]
        # пустая ссылка 1С — семантический NULL, в справочник не заводится
        missing = [g for g in missing
                   if g and g != "00000000-0000-0000-0000-000000000000"]
        if not missing:
            print("    уровень %d: недостающих предков нет — иерархия сошлась" % level)
            break

        rows = fetch_from_1c(ms, cfg, missing, batch)
        added = insert_rows(pg, cfg, rows, batch, dry_run)
        total += added
        print("    уровень %d: не хватало %d | нашлось в 1С %d | %s %d"
              % (level, len(missing), len(rows),
                 "добавилось бы" if dry_run else "добавлено", added))

        if dry_run:
            print("    (dry-run: вставки откатаны, следующие уровни не видны)")
            break
        if not added:
            print("    ⚠ %d ссылок не нашлось в 1С — оборваны, дальше не идём"
                  % len(missing))
            break

    return {"dim": cfg["code"], "touched": total, "levels": level}


def process(dim_code: str, pg, rt, ms, mode: str, batch: int,
            dry_run: bool, set_mark: bool = False) -> Optional[dict]:
    """Обёртка с журналированием в load_history — для видимости в UI-портале."""
    cfg_probe = read_config(pg, dim_code)
    run_id = None
    if cfg_probe and not dry_run:
        run_id = open_history(pg, cfg_probe["register_id"], mode)
    try:
        res = _process_inner(dim_code, pg, rt, ms, mode, batch, dry_run, set_mark)
    except Exception as e:
        close_history(pg, run_id, "failed", error=str(e)[:2000])
        raise
    if res is None:
        close_history(pg, run_id, "failed", error="нет конфига или мэппингов")
    else:
        wm = res.get("watermark") or res.get("mark")
        close_history(pg, run_id, "success",
                      rows_loaded=res.get("touched", 0),
                      checkpoint=f"mode={mode}; watermark={wm}; "
                                 f"changed={res.get('changed', res.get('found', 0))}")
    return res


def _process_inner(dim_code: str, pg, rt, ms, mode: str, batch: int,
                   dry_run: bool, set_mark: bool = False) -> Optional[dict]:
    cfg = read_config(pg, dim_code)
    if not cfg:
        print(f"  ✗ {dim_code}: нет конфига (registers / register_sources) — заведите привязку")
        return None
    if not cfg["mappings"]:
        print(f"  ✗ {dim_code}: в column_mappings нет ни одного активного поля")
        return None

    fields = ", ".join(f"{m['src']}→{m['tgt']}" for m in cfg["mappings"])
    print(f"\n  {dim_code}")
    print(f"    источник 1С : {cfg['mssql_schema']}.{cfg['mssql_table']} ({cfg['onec_name'] or '—'})")
    print(f"    поля из конфига: {fields}")
    print(f"    retail       : {cfg['retail_table'] or '— (не привязан, метки не будет)'}")

    if mode == "incremental":
        return process_incremental(cfg, pg, rt, ms, batch, dry_run)
    if mode == "hierarchy":
        return process_hierarchy(cfg, pg, ms, batch, dry_run)

    guids = [r[0] for r in pg.get_records(
        f'SELECT guid::text FROM {cfg["dim_schema"]}.{cfg["dim_table"]}')]
    if not guids:
        print(f"    строк в dim 0 — нечего заполнять (строки создают факты)")
        return {"dim": dim_code, "rows": 0, "found": 0, "touched": 0, "stamped": 0}

    mark = retail_mark(rt, cfg["retail_table"]) if set_mark else None
    rows = fetch_from_1c(ms, cfg, guids, batch)
    touched = apply_rows(pg, cfg, rows, mark, set_mark, batch, dry_run)
    stamped = stamp_all(pg, cfg, mark, dry_run) if (set_mark and mark) else 0

    stub_left = pg.get_first(
        f'SELECT count(*) FROM {cfg["dim_schema"]}.{cfg["dim_table"]} WHERE is_stub')[0]
    print(f"    строк в dim {len(guids)} | найдено в 1С {len(rows)} | "
          f"{'обновилось бы' if dry_run else 'обновлено'} {touched} | "
          f"метка {mark or '—'} {'(проставилась бы всем: %d)' % stamped if dry_run else '(проставлена: %d)' % stamped}")
    if not dry_run and stub_left:
        print(f"    ⚠ осталось stub {stub_left} — этих guid нет в справочнике 1С (удалены/архив)")
    return {"dim": dim_code, "rows": len(guids), "found": len(rows),
            "touched": touched, "stamped": stamped, "mark": mark}


def _warn(text: str) -> None:
    """WARNING, который видно и в терминале, и в логе Airflow-таски."""
    import logging
    bar = "!" * 100
    print(f"\n{bar}\n  WARNING: {text}\n{bar}\n", flush=True)
    logging.getLogger(__name__).warning(text)


def _resolve_set_mark(args) -> bool:
    """
    Контракт метки retail_updated_at по режимам.

    initial      — метка ставится (одна MAX-дата всем), --no-retail-mark выключает.
                   Первичная заливка: per-row меток ещё нет, терять нечего.
    reload       — метка по умолчанию ВЫКЛЮЧЕНА, включается только --set-mark.
                   Причина: stamp_all перетирает все per-row метки инкремента одной
                   MAX(updated_at) и сдвигает watermark вперёд. Объект, который retail
                   уже пометил, а 1С ещё не обновил, получит чужую позднюю метку —
                   и следующий инкремент его изменение уже не увидит. Ничего не
                   падает, просто часть справочника тихо перестаёт обновляться.
    incremental  — per-row метки из retail, флаги не влияют.
    hierarchy    — метки не трогает, флаги не влияют.
    """
    mode = args.mode
    if mode == "initial":
        if args.set_mark:
            _warn("--set-mark для initial не нужен: первичная заливка ставит метку и так")
        return not args.no_retail_mark

    if mode == "reload":
        if args.no_retail_mark:
            _warn("--no-retail-mark для reload устарел и игнорируется: "
                  "reload по умолчанию метку не трогает")
        if args.set_mark:
            _warn("reload --set-mark: retail_updated_at будет ПЕРЕСТАВЛЕН ВСЕМ строкам dim "
                  "одной MAX-датой из retail. Per-row метки инкремента затрутся, watermark "
                  "сдвинется вперёд. Объекты, изменённые в retail, но ещё не доехавшие до "
                  "1С, следующий инкремент НЕ УВИДИТ. Делайте это только осознанно.")
            return True
        return False

    # incremental / hierarchy — метка управляется внутри режима, флаги не про них
    if args.set_mark or args.no_retail_mark:
        _warn(f"--set-mark / --no-retail-mark на режим {mode} не влияют")
    return False


def main() -> None:
    ap = argparse.ArgumentParser(description="Универсальная заливка справочника по конфигу")
    ap.add_argument("--dim", nargs="*", default=None,
                    help="коды справочников; по умолчанию все pipeline_type='reference_dim'")
    ap.add_argument("--mode",
                    choices=["initial", "reload", "incremental", "hierarchy"],
                    default="initial",
                    help="hierarchy — догрузить недостающих предков по _ParentIDRRef")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--set-mark", action="store_true",
                    help="reload: поставить ОДНУ max retail-дату всем строкам dim. "
                         "По умолчанию reload метку НЕ трогает — иначе per-row метки "
                         "инкремента затираются и watermark уезжает вперёд")
    ap.add_argument("--no-retail-mark", action="store_true",
                    help="initial: не трогать retail_updated_at (только поля из 1С). "
                         "Для reload — устарел и игнорируется: там метка и так выключена")
    ap.add_argument("--pg-conn", default="postgre_test_base")
    ap.add_argument("--retail-conn", default="bd_retail")
    ap.add_argument("--mssql-conn", default="mssql_1c_conn")
    args = ap.parse_args()
    warnings.filterwarnings("ignore")
    set_mark = _resolve_set_mark(args)

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    pg = PostgresHook(postgres_conn_id=args.pg_conn)
    rt = PostgresHook(postgres_conn_id=args.retail_conn)
    ms = MsSqlHook(mssql_conn_id=args.mssql_conn)

    targets = args.dim or [r[0] for r in pg.get_records(
        "SELECT code FROM etl_meta.registers "
        "WHERE pipeline_type='reference_dim' AND is_active ORDER BY code")]
    if not targets:
        print("не найдено ни одного справочника (pipeline_type='reference_dim')")
        sys.exit(2)

    print("=" * 100)
    mark_kind = ("PER-ROW метка (только инкремент)" if args.mode == "incremental"
                 else "метки не трогаем" if args.mode == "hierarchy"
                 else "ОДНА max-дата всем строкам" if set_mark
                 else "метку НЕ трогаем (только поля из 1С)")
    print(f"  СПРАВОЧНИКИ ПО КОНФИГУ, режим '{args.mode}' — поля из etl_meta, "
          f"{mark_kind}{'  [DRY-RUN]' if args.dry_run else ''}")
    print("=" * 100)

    results, failed = [], []
    for code in targets:
        try:
            r = process(code, pg, rt, ms, args.mode, args.batch,
                        args.dry_run, set_mark)
            (results if r else failed).append(r or code)
        except Exception as e:
            print(f"    ✗ ОШИБКА: {str(e)[:200]}")
            failed.append(code)

    print("\n" + "-" * 100)
    if results:
        # 'rows' есть только у initial/reload; инкремент отдаёт 'changed'/'in_dim'.
        # Считаем по 'touched' — он общий для всех режимов.
        print(f"  ИТОГО {'обновилось бы' if args.dry_run else 'обновлено'} "
              f"{sum(r.get('touched', 0) for r in results)} строк "
              f"в {len([r for r in results if r.get('touched')])} справочниках")
    if failed:
        print(f"  С ОШИБКОЙ: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
