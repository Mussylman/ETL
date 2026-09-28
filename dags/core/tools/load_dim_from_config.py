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

Строки заводят факты (stub-резолв по guid из 1С) — у справочников с dim_key_source='facts'.
Справочник с dim_key_source='source' (номенклатура) заводит строки сам: инкремент — каждый
объект, о котором сигналит retail; режим register — все объекты таблицы 1С разом.
Скрипт только дозаполняет уже существующие.

Использование:
    source venv/bin/activate
    PYTHONPATH=dags python3 -m core.tools.load_dim_from_config --dim dim_sklad --dry-run
    PYTHONPATH=dags python3 -m core.tools.load_dim_from_config --dim dim_sklad
    PYTHONPATH=dags python3 -m core.tools.load_dim_from_config            # все reference_dim
    PYTHONPATH=dags python3 -m core.tools.load_dim_from_config --mode reload   # поля из 1С, метку не трогает

Параметры:
    --dim       какие справочники (по умолчанию все с pipeline_type='reference_dim')
    --mode      initial     — первичная: поля из 1С + ОДНА max retail-дата всем строкам;
                              на ПУСТОМ справочнике — полная заливка всех объектов из 1С
                reload      — ресинк полей из 1С; retail_updated_at НЕ трогает (безопасный дефолт)
                incremental — с retail-привязкой: изменённые в retail (per-row метки),
                              затем stub-pass; без привязки (source-only): только
                              stub-pass. Stub-pass дозаполняет строки is_stub=true из 1С
                              по guid: метку не трогает, строк не создаёт, за проход
                              берёт не больше --stub-batch строк по циклической очереди id
                              и снимает is_stub только при непустом name/code
                hierarchy   — догрузить недостающих предков по _ParentIDRRef
    --dry-run   ничего не писать: показать план, поля из конфига, что изменилось бы
    --batch     размер батча uid в IN-списке MSSQL и в execute_values (default 500)
    --stub-batch  сколько stub-строк за проход (default 500); курсор очереди — в checkpoint
    --set-mark  reload: всё-таки переставить метку всем строкам (с WARNING — см. _resolve_set_mark)
    --no-retail-mark  initial: не трогать retail_updated_at. Для reload устарел, игнорируется.

Source-only справочник (без registers.retail_table / retail_uid_column):
    initial/reload  — загрузка полей из 1С БЕЗ retail-метки: retail_mark даёт None,
                      stamp_all не вызывается. Это штатный контракт, не ошибка.
    incremental     — только stub-pass (retail-части нет, watermark не выдумывается).
    Retail-часть (fetch_changed_guids, fetch_retail_marks, process_incremental)
    по-прежнему требует привязку через строгий resolve_keys.

Почему reload по умолчанию метку не трогает: stamp_all перетирает per-row метки
инкремента одной MAX(updated_at) и сдвигает watermark вперёд. Объект, уже изменённый
в retail, но ещё не обновлённый в 1С, получит чужую позднюю метку — и следующий
инкремент его не увидит. Тихая потеря обновлений без единой ошибки.
"""

import argparse
import re
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

# Сколько stub-строк берём за один проход. Очередь циклическая: дошли до конца —
# следующий проход начинается с нуля. Без лимита один тик мог утащить в 1С все
# накопленные stub разом (после массовой перезаливки это сотни round-trip к боевой базе).
STUB_BATCH_DEFAULT = 500

# Курсор очереди stub. Живёт в checkpoint_value последнего прогона регистра —
# отдельного хранилища и новых колонок в dim сознательно не заводим.
CURSOR_KEY = "last_stub_id"

# Поля, по которым судим о полноте строки справочника. Временный generic-критерий:
# «непусто хотя бы одно из mapped name/code». Индивидуальных правил по справочникам
# и признака is_required в column_mappings здесь намеренно нет.
DISPLAY_COLS = ("name", "code")

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
        SELECT r.id, r.code, r.retail_table, t.target_schema, t.target_table, r.retail_uid_column,
               coalesce(to_jsonb(r) ->> 'dim_key_source', 'facts')
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
        SELECT source_column, target_column, transform_type, is_expression
        FROM   etl_meta.column_mappings
        WHERE  source_id = %s AND is_active
        ORDER  BY target_column
    """, parameters=(src[0],))

    return {
        "register_id": reg[0], "code": reg[1], "retail_table": reg[2],
        "retail_uid_column": reg[5],
        # facts  — строки справочника заводят факты (stub по первой ссылке);
        # source — справочник сам регистрирует каждый объект источника (1С),
        #          независимо от того, встречался ли он в фактах
        "key_source": reg[6],
        "dim_schema": reg[3] or "public", "dim_table": reg[4] or dim_code,
        "mssql_schema": src[1], "mssql_table": src[2], "onec_name": src[3],
        # is_expression — как у фактов (QueryBuilder): source_column — готовое
        # SQL-выражение над строкой источника, а не имя колонки. Так бренд
        # приезжает из _Reference25969 подзапросом прямо в строку номенклатуры,
        # без отдельного справочника.
        "mappings": [{"src": m[0], "tgt": m[1], "transform": m[2], "expr": bool(m[3])} for m in maps],
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


def has_retail_binding(cfg: dict) -> bool:
    """Есть ли у справочника retail-привязка (registers.retail_table + retail_uid_column).

    Определяет ПОВЕДЕНИЕ инкремента, а не право на него: с привязкой —
    retail-инкремент + stub-pass, без неё — только stub-pass (source-only DIM).
    """
    return bool(cfg.get("retail_table") and cfg.get("retail_uid_column"))


def display_columns(cfg: dict) -> List[str]:
    """Какие из DISPLAY_COLS реально замаплены у этого справочника."""
    tgts = [m["tgt"] for m in cfg["mappings"]]
    return [c for c in DISPLAY_COLS if c in tgts]


def is_complete(row: Tuple, positions: List[int]) -> bool:
    """
    Полна ли строка, приехавшая из 1С: непусто хотя бы одно display-поле.

    Справочник, у которого не замаплено ни name, ни code (positions пуст), судить
    не по чему — считаем полным, иначе его stub не закрылись бы никогда.
    """
    if not positions:
        return True
    return any(str(row[p]).strip() for p in positions if row[p] is not None)


def read_stub_cursor(pg, register_id: int) -> int:
    """
    Курсор очереди stub — из checkpoint_value последнего прогона этого регистра.

    Читается терпимо: нет ключа, старый формат checkpoint, пусто, не парсится —
    всё это 0, то есть «начать с начала очереди». Таска не должна падать из-за
    курсора: он ускоряет обход, но не является данными.
    """
    try:
        row = pg.get_first(
            "SELECT checkpoint_value FROM etl_meta.load_history "
            "WHERE register_id = %s AND checkpoint_value LIKE %s "
            "ORDER BY id DESC LIMIT 1",
            parameters=(register_id, f"%{CURSOR_KEY}=%"))
        if not row or not row[0]:
            return 0
        m = re.search(CURSOR_KEY + r"=(\d+)", row[0])
        return int(m.group(1)) if m else 0
    except Exception as e:
        print(f"    ⚠ курсор stub не прочитан ({str(e)[:80]}) — иду с начала очереди")
        return 0


def resolve_dwh_key(cfg: dict) -> str:
    """
    Ключ dim, по которому сопоставляем с 1С — из мэппинга, не литералом.

    Выделен из resolve_keys: он нужен и там, где retail не при чём —
    fetch_from_1c, apply_rows, stub-pass. Требовать для них retail_uid_column
    означало бы, что справочник без retail-привязки не может даже дозаполнить
    свои stub из 1С. Retail-часть по-прежнему ходит через строгий resolve_keys.
    """
    tgt_cols = [m["tgt"] for m in cfg["mappings"]]
    dwh_key = next((c for c in tgt_cols if c in ("guid", "uid")), None)
    if not dwh_key:
        raise RuntimeError(
            f"{cfg['code']}: в мэппинге нет колонки-ключа (guid/uid) — "
            f"по чему сопоставлять строки с 1С? (проверьте column_mappings)")
    return dwh_key


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
    dwh_key = resolve_dwh_key(cfg)
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


def _select_list(cfg: dict) -> str:
    """Колонки мэппинга для SELECT из 1С: имя → [имя], выражение → как есть с алиасом."""
    parts = []
    for i, m in enumerate(cfg["mappings"]):
        parts.append(f"({m['src']}) AS [__e{i}]" if m.get("expr") else f"[{m['src']}]")
    return ", ".join(parts)


def fetch_from_1c(ms, cfg: dict, guids: List[str], batch: int) -> List[Tuple]:
    """
    Забирает из 1С ВСЕ поля мэппинга по списку guid.
    Ключ сопоставления — guid: ищем ту колонку мэппинга, что ложится в dim.guid.
    """
    from ..transform.binary import uuid_to_mssql_hex_1c

    dwh_key = resolve_dwh_key(cfg)
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

    select = _select_list(cfg)
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
            out.append(_transform_row(cfg, tf, r))
    return out


def _transform_row(cfg: dict, tf: Dict[str, Any], r: Tuple) -> Tuple:
    """Одна строка 1С → значения в порядке cfg["mappings"] с трансформациями."""
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
    return tuple(vals)


def full_load_from_1c(ms, pg, cfg: dict, batch: int, dry_run: bool) -> Tuple[int, int]:
    """
    Первичная заливка ПУСТОГО справочника целиком из 1С: (прочитано, вставлено).

    Зачем: initial по контракту идёт по guid, уже лежащим в dim («строки заводят
    факты»). На новой базе фактов нет — и initial честно говорил «нечего
    заполнять». Для PROD-порядка «сначала справочники, потом регистры» этого
    мало: справочник должен появиться до первого факта.

    Что берём: ВСЕ объекты таблицы 1С по мэппингу — включая группы иерархии
    и помеченные на удаление (факты могут ссылаться на любые). Пустой guid
    (EMPTY_REF) отсекается запросом. Чтение потоковое, батчами через курсор —
    у контрагентов и договоров по 1.4–1.5 млн строк, IN(...) здесь не годится.
    Запись — insert_rows: ON CONFLICT (guid) DO NOTHING, id раздаёт IDENTITY.
    Работает только когда dim пуст; на непустом initial ведёт себя как раньше.
    """
    from ..transform.binary import uuid_to_mssql_hex_1c

    key_src = next(m["src"] for m in cfg["mappings"] if m["tgt"] == resolve_dwh_key(cfg))
    cols = _select_list(cfg)
    table = f"[{cfg['mssql_schema']}].[{cfg['mssql_table']}]"
    tf = _transformers()
    for m in cfg["mappings"]:
        if m["transform"] and m["transform"] not in tf:
            raise RuntimeError(
                f"{cfg['code']}.{m['tgt']}: неизвестный transform_type "
                f"'{m['transform']}' — добавьте его в _transformers()")

    conn = ms.get_conn()
    read = inserted = 0
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {cols} FROM {table} WITH (NOLOCK) "
                    f"WHERE [{key_src}] <> {uuid_to_mssql_hex_1c(EMPTY_REF)}")
        while True:
            chunk = cur.fetchmany(batch)
            if not chunk:
                break
            rows = [_transform_row(cfg, tf, r) for r in chunk]
            read += len(rows)
            inserted += insert_rows(pg, cfg, rows, batch, dry_run)
            if read % (batch * 40) == 0:
                print(f"      … прочитано {read:,}".replace(",", " "), flush=True)
    finally:
        conn.close()
    return read, inserted


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
               batch: int, dry_run: bool, clear_stub: bool = True) -> int:
    """
    UPDATE dim: все поля из мэппинга + is_stub=false + одна метка всем.
    Ключ — guid. Строки не создаются (их заводят факты).

    clear_stub=False — записать поля, но оставить строку stub. Нужно stub-pass'у
    для incomplete: объект в 1С есть, но name и code пусты, закрывать его рано.
    """
    if not rows:
        return 0
    from psycopg2.extras import execute_values

    key = resolve_dwh_key(cfg)
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
    if clear_stub:
        set_parts.append("is_stub = false")
    set_parts.append("etl_updated_at = timezone('Asia/Almaty', now())")
    if set_mark and mark is not None:
        set_parts.append(f"{WATERMARK_COL} = %(mark)s")

    # обновляем только если что-то реально меняется — идемпотентность
    diff = " OR ".join([f'd."{c}" IS DISTINCT FROM {_v(c)}' for c in data_cols]
                       + (["d.is_stub"] if clear_stub else []))
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
            key = resolve_dwh_key(cfg)
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
    registered = 0
    if cfg.get("key_source") == "source":
        # Справочник — хозяин ключей: объект, о котором сигналит retail, заводится
        # с полями из 1С, даже если ни один факт на него ещё не ссылался.
        new_guids = [g for g in changed if g not in present]
        if new_guids:
            rows_new = fetch_from_1c(ms, cfg, new_guids, batch)
            registered = insert_rows(pg, cfg, rows_new, batch, dry_run)
            key_pos = [m["tgt"] for m in cfg["mappings"]].index(resolve_dwh_key(cfg))
            guids += [str(r[key_pos]) for r in rows_new]
            print(f"    новых объектов в retail {len(new_guids)} | найдено в 1С {len(rows_new)} | "
                  f"{'заводилось бы' if dry_run else 'заведено'} {registered}")
    if not guids:
        print(f"    watermark {watermark} | изменений в retail {len(changed)} | "
              f"из них есть в dim 0 — новые объекты придут с фактами")
        return {"dim": cfg["code"], "changed": len(changed), "in_dim": 0,
                "touched": 0, "watermark": watermark}

    # поля из 1С + per-row метки из retail
    rows = fetch_from_1c(ms, cfg, guids, batch)
    marks = dict(fetch_retail_marks(rt, cfg, guids))
    touched = apply_rows_per_row(pg, cfg, rows, marks, batch, dry_run)
    key_pos = [m["tgt"] for m in cfg["mappings"]].index(resolve_dwh_key(cfg))
    touched_guids = [str(r[key_pos]) for r in rows]

    print(f"    watermark {watermark} | изменений в retail {len(changed)} | "
          f"из них в dim {len(guids)} | найдено в 1С {len(rows)} | "
          f"{'обновилось бы' if dry_run else 'обновлено'} {touched}")
    return {"dim": cfg["code"], "changed": len(changed), "in_dim": len(guids),
            "touched": touched, "registered": registered, "watermark": watermark,
            "touched_guids": touched_guids}


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

    key = resolve_dwh_key(cfg)
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


def process_stub_pass(cfg: dict, pg, ms, batch: int, dry_run: bool,
                      stub_batch: int = STUB_BATCH_DEFAULT,
                      cursor: Optional[int] = None) -> dict:
    """
    Дешёвый проход по stub-строкам этого справочника — после retail-инкремента.

    Откуда stub: post_load фактов, встретив незнакомый guid, заводит строку
    с id, но без полей (is_stub=true приходит из DEFAULT колонки) — объект есть
    в 1С, а retail о нём ещё не сигналил (или не сигналит вовсе). Раньше их
    закрывала отдельная таска load_dim_names с зашитыми name/code. Теперь
    reference_dim обслуживает свои stub сам — вызывающему DAG об этом знать не нужно.

    Порция и очередь. За проход берём не больше stub_batch строк, отсортированных
    по id, начиная за курсором. Курсор — id последней взятой строки, он лежит
    в checkpoint_value прошлого прогона. Если хвост короче порции (или пуст) —
    очередь пройдена до конца, курсор сбрасывается в 0. Поэтому guid, которых
    в 1С нет никогда, не занимают начало очереди навсегда: они уезжают в конец
    круга, а новые stub с большими id гарантированно доходят до обработки.

    Критерий закрытия stub — не «guid нашёлся», а «приехало непустое name или code»
    (is_complete). Объект, который в 1С есть, но пуст, остаётся stub и попадает
    в метрику incomplete: поля ему записываем, флаг не снимаем.

    Чего проход не делает: не читает и не двигает retail watermark, не трогает
    retail_updated_at (apply_rows с set_mark=False), не создаёт строк, не меняет id.
    Цена холостого хода: один COUNT по is_stub; если stub нет — в 1С не идём.
    """
    dim = f'{cfg["dim_schema"]}.{cfg["dim_table"]}'
    key = resolve_dwh_key(cfg)
    before = pg.get_first(f"SELECT count(*) FROM {dim} WHERE is_stub")[0]
    start = read_stub_cursor(pg, cfg["register_id"]) if cursor is None else cursor

    def _empty(last_id: int) -> dict:
        return {"stub_before": before, "stub_requested": 0, "stub_found": 0,
                "stub_filled": 0, "stub_not_found": 0, "stub_incomplete": 0,
                "stub_after": before, CURSOR_KEY: last_id, "stub_guids": []}

    if not before:
        return _empty(0)

    def _take(after_id: int):
        return pg.get_records(
            f"SELECT id, {key}::text FROM {dim} "
            f"WHERE is_stub AND id > %s ORDER BY id LIMIT %s",
            parameters=(after_id, stub_batch))

    picked = _take(start)
    if not picked and start:
        # хвост очереди пуст — сразу заходим с начала круга, не теряя тик
        start, picked = 0, _take(0)
    if not picked:
        return _empty(0)

    ids = [r[0] for r in picked]
    guids = [r[1] for r in picked]
    # порция короче лимита — дальше по id ничего нет, следующий проход с начала
    next_cursor = 0 if len(picked) < stub_batch else ids[-1]

    rows = fetch_from_1c(ms, cfg, guids, batch)
    tgts = [m["tgt"] for m in cfg["mappings"]]
    disp = display_columns(cfg)
    disp_pos = [tgts.index(c) for c in disp]
    if not disp:
        _warn(f"{cfg['code']}: не замаплено ни одно из полей {DISPLAY_COLS} — "
              f"полноту строки проверять не по чему, stub снимается по факту наличия в 1С")

    complete = [r for r in rows if is_complete(r, disp_pos)]
    incomplete = [r for r in rows if not is_complete(r, disp_pos)]

    filled = apply_rows(pg, cfg, complete, mark=None, set_mark=False,
                        batch=batch, dry_run=dry_run, clear_stub=True)
    if incomplete:
        # данные пишем, флаг не снимаем — объект в 1С есть, но показать нечего
        apply_rows(pg, cfg, incomplete, mark=None, set_mark=False,
                   batch=batch, dry_run=dry_run, clear_stub=False)

    after = before - filled if dry_run else pg.get_first(
        f"SELECT count(*) FROM {dim} WHERE is_stub")[0]
    key_pos = tgts.index(key)
    print(f"    stub-pass: было {before} | взято {len(picked)} (id > {start}, лимит {stub_batch}) | "
          f"найдено в 1С {len(rows)} | нет в 1С {len(picked) - len(rows)} | "
          f"неполных {len(incomplete)} | {'заполнилось бы' if dry_run else 'заполнено'} {filled} | "
          f"осталось {after} | {CURSOR_KEY}={next_cursor}")
    return {"stub_before": before, "stub_requested": len(picked),
            "stub_found": len(rows), "stub_filled": filled,
            "stub_not_found": len(picked) - len(rows),
            "stub_incomplete": len(incomplete), "stub_after": after,
            CURSOR_KEY: next_cursor,
            "stub_guids": [str(r[key_pos]) for r in complete]}


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
    key = resolve_dwh_key(cfg)
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


def _dim_columns(pg, cfg: dict) -> set:
    return {r[0] for r in pg.get_records(
        "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
        parameters=(cfg["dim_schema"], cfg["dim_table"]))}


def hierarchy_levels(pg, cfg: dict) -> List[str]:
    """
    Колонки плоской иерархии этого справочника — из САМОЙ таблицы, не из конфига.

    Иерархия включена, если (а) в мэппинге есть _ParentIDRRef (parent_col) и
    (б) в dim есть колонка category. Число уровней = 1 + сколько колонок
    subcategoryN существует. Так глубина задаётся DDL под фактическое дерево
    (аудит: номенклатура 8 предков, контрагенты 5, склады 3), а loader ничего
    про конкретный справочник не знает и не ограничивает глубину заранее.
    Новых полей в etl_meta для этого не нужно.
    """
    if not parent_col(cfg):
        return []
    cols = {r[0] for r in pg.get_records(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s",
        parameters=(cfg["dim_schema"], cfg["dim_table"]))}
    if "category" not in cols:
        return []
    subs = sorted((c for c in cols if re.fullmatch(r"subcategory\d+", c)),
                  key=lambda c: int(c[len("subcategory"):]))
    # уровни должны идти подряд: category, subcategory1, subcategory2, ...
    levels = ["category"]
    for i, c in enumerate(subs, start=1):
        if c != f"subcategory{i}":
            break
        levels.append(c)
    return levels


def flatten_hierarchy(pg, cfg: dict, guids: Optional[List[str]], dry_run: bool) -> int:
    """
    Заполняет category / subcategory1..N именами ПРЕДКОВ узла сверху вниз.

    Для каждой строки: подъём по parent_guid до корня (рекурсивный CTE, любая
    глубина, защита от цикла по длине пути), разворот от корня; category —
    первый предок, subcategory1 — второй, ... , недостающие уровни — NULL.
    Сам узел в свой путь не входит: у товара уровни = его группы, у группы —
    её надгруппы, у корня — всё NULL. Так у листа и у группы одинаковый смысл
    колонок, а число уровней = максимум предков в дереве.

    guids=None — пересчитать все строки (initial/reload/hierarchy),
    список — только их (incremental/stub-pass). id и метки не трогаются.
    """
    levels = hierarchy_levels(pg, cfg)
    if not levels:
        return 0
    dim = f'{cfg["dim_schema"]}.{cfg["dim_table"]}'
    pcol = parent_col(cfg)
    key = resolve_dwh_key(cfg)
    n = len(levels)
    sets = ", ".join(f'"{c}" = p.path[{i}]' for i, c in enumerate(levels, start=1))
    diff = " OR ".join(f'd."{c}" IS DISTINCT FROM p.path[{i}]' for i, c in enumerate(levels, start=1))
    scope = f"WHERE d.{key} = ANY(%(guids)s::uuid[])" if guids is not None else ""
    sql = f"""
        WITH RECURSIVE up AS (
            SELECT d.{key} AS node, d."{pcol}" AS anc, 1 AS dist,
                   ARRAY[d.{key}] AS seen
            FROM {dim} d {scope}
            UNION ALL
            SELECT up.node, a."{pcol}", up.dist + 1, up.seen || a.{key}
            FROM up JOIN {dim} a ON a.{key} = up.anc
            WHERE up.anc IS NOT NULL AND NOT (a.{key} = ANY(up.seen)) AND up.dist < 64
        ),
        paths AS (
            SELECT up.node,
                   (array_agg(a.name ORDER BY up.dist DESC))[1:{n}] AS path
            FROM up JOIN {dim} a ON a.{key} = up.anc
            WHERE up.anc IS NOT NULL
            GROUP BY up.node
        ),
        p AS (
            SELECT d.{key} AS node,
                   COALESCE(paths.path, ARRAY[]::text[]) AS path
            FROM {dim} d LEFT JOIN paths ON paths.node = d.{key} {scope}
        )
        UPDATE {dim} d SET {sets}
        FROM p WHERE d.{key} = p.node AND ({diff})
    """
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"guids": guids} if guids is not None else None)
            touched = max(cur.rowcount, 0)
        conn.rollback() if dry_run else conn.commit()
        return touched
    finally:
        conn.close()


def post_hierarchy(pg, cfg: dict, guids: Optional[List[str]], dry_run: bool) -> dict:
    """Плоская иерархия после записи полей; guids=None — весь справочник."""
    levels = hierarchy_levels(pg, cfg)
    if not levels:
        return {}
    flat = flatten_hierarchy(pg, cfg, guids, dry_run)
    scope = "все строки" if guids is None else f"{len(guids)} затронутых"
    print(f"    иерархия     : {scope} | уровней {len(levels)} | "
          f"{'пересчиталось бы' if dry_run else 'пересчитано'} {flat}")
    return {"flat_touched": flat}


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
            dry_run: bool, set_mark: bool = False,
            stub_batch: int = STUB_BATCH_DEFAULT) -> Optional[dict]:
    """Обёртка с журналированием в load_history — для видимости в UI-портале."""
    cfg_probe = read_config(pg, dim_code)
    run_id = None
    if cfg_probe and not dry_run:
        run_id = open_history(pg, cfg_probe["register_id"], mode)
    try:
        res = _process_inner(dim_code, pg, rt, ms, mode, batch, dry_run, set_mark,
                             stub_batch=stub_batch)
    except Exception as e:
        close_history(pg, run_id, "failed", error=str(e)[:2000])
        raise
    if res is None:
        close_history(pg, run_id, "failed", error="нет конфига или мэппингов")
    else:
        parts = [f"mode={mode}"]
        if res.get("source_only"):
            parts.append("source-only")          # watermark'а нет — и не выдумываем
        else:
            parts.append(f"watermark={res.get('watermark') or res.get('mark')}")
            parts.append(f"changed={res.get('changed', res.get('found', 0))}")
        if "stub_before" in res:
            # компактные метрики stub-pass в существующий checkpoint: их читает
            # UI-портал, и отсюда же следующий прогон берёт курсор очереди
            parts.append(
                f"stub={res['stub_before']}→{res['stub_after']}; "
                f"requested={res.get('stub_requested', 0)}; found={res.get('stub_found', 0)}; "
                f"filled={res.get('stub_filled', 0)}; not_found={res.get('stub_not_found', 0)}; "
                f"incomplete={res.get('stub_incomplete', 0)}; "
                f"{CURSOR_KEY}={res.get(CURSOR_KEY, 0)}")
        close_history(pg, run_id, "success",
                      rows_loaded=res.get("touched", 0) + res.get("stub_filled", 0),
                      checkpoint="; ".join(parts))
    return res


def _process_inner(dim_code: str, pg, rt, ms, mode: str, batch: int,
                   dry_run: bool, set_mark: bool = False,
                   stub_batch: int = STUB_BATCH_DEFAULT) -> Optional[dict]:
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
        if has_retail_binding(cfg):
            res = process_incremental(cfg, pg, rt, ms, batch, dry_run)
        else:
            # source-only DIM: retail о нём не сигналит, watermark'а нет и не
            # будет — инкремент для него = дозаполнить stub из 1С, и только.
            print("    инкремент    : source-only — retail-части нет, только stub-pass")
            res = {"dim": cfg["code"], "touched": 0, "source_only": True}
        res.update(process_stub_pass(cfg, pg, ms, batch, dry_run, stub_batch=stub_batch))
        # Плоская иерархия и lookups — только для затронутых строк. Если среди
        # них есть ГРУППА со сменившимся parent, путь её потомков устареет до
        # следующего reload — см. предупреждение ниже; массовый пересчёт
        # потомков намеренно не делаем на каждом тике.
        affected = list({*res.get("touched_guids", []), *res.get("stub_guids", [])})
        if affected and hierarchy_levels(pg, cfg):
            res.update(post_hierarchy(pg, cfg, affected, dry_run))
            moved = pg.get_first(
                f'SELECT count(*) FROM {cfg["dim_schema"]}.{cfg["dim_table"]} '
                f'WHERE is_group AND {resolve_dwh_key(cfg)} = ANY(%s::uuid[])',
                parameters=(affected,))[0] if "is_group" in _dim_columns(pg, cfg) else 0
            if moved:
                print(f"    ⚠ среди изменённых {moved} групп(ы): если у них сменился parent, "
                      f"путь потомков обновится при следующем reload")
        return res
    if mode == "register":
        # Все объекты таблицы 1С, которых нет в справочнике, — с полями, id из IDENTITY.
        # Существующие строки не меняются (ON CONFLICT (guid) DO NOTHING): id не
        # переназначается никогда. Идемпотентен — повторный прогон ничего не заводит.
        dim_full = f'{cfg["dim_schema"]}.{cfg["dim_table"]}'
        before = pg.get_first(f"SELECT count(*) FROM {dim_full}")[0]
        read, inserted = full_load_from_1c(ms, pg, cfg, batch, dry_run)
        print(f"    в справочнике было {before} | прочитано из 1С {read} | "
              f"{'завелось бы' if dry_run else 'заведено'} {inserted}")
        res = {"dim": dim_code, "found": read, "touched": inserted, "registered": inserted}
        if inserted and not dry_run and hierarchy_levels(pg, cfg):
            res.update(post_hierarchy(pg, cfg, None, dry_run))
        return res
    if mode == "hierarchy":
        res = process_hierarchy(cfg, pg, ms, batch, dry_run)
        if not dry_run:
            res.update(post_hierarchy(pg, cfg, None, dry_run))
        return res

    dim_full = f'{cfg["dim_schema"]}.{cfg["dim_table"]}'
    guids = [r[0] for r in pg.get_records(f'SELECT guid::text FROM {dim_full}')]
    if not guids:
        if mode != "initial":
            print(f"    строк в dim 0 — нечего перезаливать (строки создают факты или initial)")
            return {"dim": dim_code, "rows": 0, "found": 0, "touched": 0, "stamped": 0}
        # Пустой справочник на первичной заливке — берём его из 1С целиком.
        print(f"    dim пуст → полная заливка из 1С ({cfg['mssql_schema']}.{cfg['mssql_table']})")
        read, inserted = full_load_from_1c(ms, pg, cfg, batch, dry_run)
        mark = retail_mark(rt, cfg["retail_table"]) if set_mark else None
        stamped = stamp_all(pg, cfg, mark, dry_run) if (set_mark and mark) else 0
        print(f"    прочитано из 1С {read} | {'вставилось бы' if dry_run else 'вставлено'} {inserted} | "
              f"метка {mark or '—'} {'(проставилась бы всем: %d)' % stamped if dry_run else '(проставлена: %d)' % stamped}")
        res = {"dim": dim_code, "rows": inserted, "found": read, "touched": inserted,
               "stamped": stamped, "mark": mark, "full_load": True}
        if not dry_run:   # в dry-run вставки откатаны — считать путь не по чему
            res.update(post_hierarchy(pg, cfg, None, dry_run))
        return res

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
    res = {"dim": dim_code, "rows": len(guids), "found": len(rows),
           "touched": touched, "stamped": stamped, "mark": mark}
    res.update(post_hierarchy(pg, cfg, None, dry_run))   # весь справочник: reload меняет parent'ы
    return res


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
                    choices=["initial", "reload", "incremental", "hierarchy", "register"],
                    default="initial",
                    help="hierarchy — догрузить недостающих предков по _ParentIDRRef")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--stub-batch", type=int, default=STUB_BATCH_DEFAULT,
                    help=f"сколько stub-строк обрабатывать за проход (default {STUB_BATCH_DEFAULT}); "
                         f"очередь по id циклическая, курсор — в checkpoint прошлого прогона")
    ap.add_argument("--set-mark", action="store_true",
                    help="reload: поставить ОДНУ max retail-дату всем строкам dim. "
                         "По умолчанию reload метку НЕ трогает — иначе per-row метки "
                         "инкремента затираются и watermark уезжает вперёд")
    ap.add_argument("--no-retail-mark", action="store_true",
                    help="initial: не трогать retail_updated_at (только поля из 1С). "
                         "Для reload — устарел и игнорируется: там метка и так выключена")
    ap.add_argument("--pg-conn", required=True)
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
                        args.dry_run, set_mark, stub_batch=args.stub_batch)
            (results if r else failed).append(r or code)
        except Exception as e:
            print(f"    ✗ ОШИБКА: {str(e)[:200]}")
            failed.append(code)

    print("\n" + "-" * 100)
    if results:
        # 'rows' есть только у initial/reload; инкремент отдаёт 'changed'/'in_dim'.
        # Считаем по 'touched' — он общий для всех режимов.
        n_ret = sum(r.get("touched", 0) for r in results)
        n_stub = sum(r.get("stub_filled", 0) for r in results)
        print(f"  ИТОГО {'обновилось бы' if args.dry_run else 'обновлено'} "
              f"{n_ret + n_stub} строк "
              f"в {len([r for r in results if r.get('touched') or r.get('stub_filled')])} справочниках"
              + (f"  (из них stub-pass: {n_stub})" if n_stub else ""))
    if failed:
        print(f"  С ОШИБКОЙ: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
