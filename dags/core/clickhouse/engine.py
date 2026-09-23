"""
Движок синхронизации. Знает алгоритм, но не знает ни одной таблицы —
всё приходит из etl_meta.ch_sync.

Порядок работы с одной партицией неизменен и уже проверен на 10.4 млн строк:
    staging → полная сверка → и только потом атомарная публикация.
Цель не меняется, пока сверка не сошлась: оборванный прогон физически не может
оставить полузагруженную партицию.
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Set

from . import reconcile as rec
from .source import open_source
from .target import ClickHouse, fmt_value

ALL = "all"   # ключ единственной партиции при load_mode=full


# ---------------------------------------------------------------- состояние
def _state(pg, spec) -> Dict[str, dict]:
    rows = pg.get_records(
        "SELECT partition_key, status, row_count, source_watermark, source_fingerprint, "
        "       last_success_at, last_checked_at "
        "  FROM etl_meta.ch_sync_partition_state WHERE sync_id = %s", parameters=(spec.id,))
    return {r[0]: {"status": r[1], "rows": r[2], "watermark": r[3],
                   "fingerprint": r[4], "success": r[5], "checked": r[6]} for r in rows}


def _save_state(pg, spec, key, status, rows=None, watermark=None,
                fingerprint=None, error=None) -> None:
    pg.run("""
        INSERT INTO etl_meta.ch_sync_partition_state
              (sync_id, partition_key, status, row_count, source_watermark,
               source_fingerprint, last_success_at, last_checked_at, last_error)
        VALUES (%s, %s, %s, %s, %s, %s, CASE WHEN %s = 'ok' THEN now() END, now(), %s)
        ON CONFLICT (sync_id, partition_key) DO UPDATE SET
              status = EXCLUDED.status,
              row_count = COALESCE(EXCLUDED.row_count, etl_meta.ch_sync_partition_state.row_count),
              source_watermark = COALESCE(EXCLUDED.source_watermark,
                                          etl_meta.ch_sync_partition_state.source_watermark),
              source_fingerprint = COALESCE(EXCLUDED.source_fingerprint,
                                            etl_meta.ch_sync_partition_state.source_fingerprint),
              last_success_at = COALESCE(EXCLUDED.last_success_at,
                                         etl_meta.ch_sync_partition_state.last_success_at),
              last_checked_at = now(),
              last_error = EXCLUDED.last_error
    """, parameters=(spec.id, key, status, rows, watermark,
                     json.dumps(fingerprint) if fingerprint else None, status, error))


def _history(pg, spec, run_mode, key, status, started, rows_src=None, rows_dst=None,
             method=None, reconcile=None, error=None) -> None:
    pg.run("""
        INSERT INTO etl_meta.ch_sync_history
              (sync_id, run_mode, partition_key, status, rows_source, rows_target,
               method, reconcile, started_at, finished_at, duration_ms, error_message)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s,%s)
    """, parameters=(spec.id, run_mode, key, status, rows_src, rows_dst, method,
                     json.dumps(reconcile, ensure_ascii=False) if reconcile else None,
                     started, int((datetime.now() - started).total_seconds() * 1000), error))


# -------------------------------------------------- поиск затронутых партиций
def affected_partitions(pg, ch: ClickHouse, spec, src, force_sweep: bool = False) -> dict:
    """
    Четыре источника кандидатов. Watermark сам по себе недостаточен: у удалённой
    строки watermark'а не осталось, а у переехавшей он указывает только на НОВУЮ
    партицию — старая осталась бы со stale-строкой.

      A. горячее окно   — последние hot_window партиций, всегда
      B. watermark      — изменившиеся строки источника → их новые партиции
      C. обратный поиск — те же бизнес-ключи в ClickHouse → их СТАРЫЕ партиции
      D. sweep          — отпечатки всех партиций источник vs ClickHouse → расходящиеся
                          единственное, что ловит удаление
    """
    if not spec.is_partitioned:
        return {"keys": [ALL], "reason": {ALL: "load_mode=full"}}

    state = _state(pg, spec)
    reason: Dict[str, str] = {}
    keys: Set[str] = set()
    pexpr = src.partition_key_expr(spec.partition_column)

    # все партиции источника
    src_keys = [str(r[0]) for r in src.conn.get_records(
        f"SELECT DISTINCT {pexpr} AS k FROM {src.projected()} ORDER BY k")]

    # A. горячее окно
    for k in sorted(src_keys)[-spec.hot_window:]:
        keys.add(k); reason[k] = "горячее окно"

    # B. watermark → новые партиции изменившихся строк
    changed_keys: List[tuple] = []
    if spec.watermark_column:
        last_wm = max((s["watermark"] for s in state.values() if s["watermark"]), default=None)
        if last_wm:
            wm_lit = f"'{last_wm:%Y-%m-%d %H:%M:%S}'"
            for r in src.conn.get_records(
                    f"SELECT DISTINCT {pexpr} AS k FROM {src.projected()} "
                    f"WHERE {spec.watermark_column} > {wm_lit}"):
                k = str(r[0])
                if k not in keys:
                    keys.add(k); reason[k] = "изменение по watermark"
            # C. обратный поиск старых партиций тех же ключей
            if spec.business_key:
                bk = ", ".join(spec.business_key)
                changed_keys = src.conn.get_records(
                    f"SELECT DISTINCT {bk} FROM {src.projected()} "
                    f"WHERE {spec.watermark_column} > {wm_lit} LIMIT 50000")
    if changed_keys:
        tuples = ", ".join(
            "(" + ", ".join(f"'{str(v)}'" if not isinstance(v, (int, float)) else str(v)
                            for v in row) + ")" for row in changed_keys)
        bk = ", ".join(spec.business_key)
        old = ch.query(f"SELECT DISTINCT {spec.partition_expr} AS k FROM {spec.fqn} "
                       f"WHERE ({bk}) IN ({tuples})")
        for k in (old.split("\n") if old else []):
            k = k.strip()
            if k and k not in keys:
                keys.add(k); reason[k] = "старая партиция переехавшей строки"

    # D. sweep по отпечаткам
    due = force_sweep or not state
    if not due:
        newest = max((s["checked"] for s in state.values() if s["checked"]), default=None)
        due = newest is None or (datetime.now() - newest).total_seconds() / 60 >= spec.sweep_interval_min
    if due:
        for k in _sweep(ch, spec, src, src_keys):
            if k not in keys:
                keys.add(k); reason[k] = "расхождение отпечатка (sweep)"

    ch_keys = {x.strip() for x in (ch.query(
        f"SELECT DISTINCT {spec.partition_expr} FROM {spec.fqn}") or "").split("\n") if x.strip()}

    # Партиция есть в источнике и полностью отсутствует в цели — берём ВСЕГДА,
    # не дожидаясь sweep. Иначе догрузка истории молча ограничивалась бы горячим
    # окном, а пропуск выглядел бы как успешный прогон.
    for k in sorted(set(src_keys) - ch_keys):
        if k not in keys:
            keys.add(k); reason[k] = "нет в цели"

    # партиции, которых в источнике нет, а в ClickHouse есть — не удаляем молча
    orphan = sorted(ch_keys - set(src_keys))

    return {"keys": sorted(keys), "reason": reason, "orphan": orphan,
            "src_total": len(src_keys), "swept": due}


def _sweep(ch: ClickHouse, spec, src, src_keys: List[str]) -> List[str]:
    """Отпечатки всех партиций с обеих сторон. Возвращает расходящиеся."""
    pexpr = src.partition_key_expr(spec.partition_column)
    dialect = spec.source_type
    body = rec.fingerprint_sql(spec, dialect).replace("SELECT ", "", 1)
    src_rows = {str(r[0]): r[1:] for r in src.conn.get_records(
        f"SELECT {pexpr} AS k, {body} FROM {src.projected()} GROUP BY {pexpr}")}
    ch_body = rec.fingerprint_sql(spec, "clickhouse").replace("SELECT ", "", 1)
    ch_out = ch.query(f"SELECT {spec.partition_expr} AS k, {ch_body} "
                      f"FROM {spec.fqn} GROUP BY k")
    ch_rows = {}
    for line in (ch_out.split("\n") if ch_out else []):
        if not line.strip():
            continue
        f = line.split("\t")
        ch_rows[f[0]] = f[1:]
    bad = []
    for k in set(src_rows) | set(ch_rows):
        if rec.compare(spec, src_rows.get(k, []), ch_rows.get(k, [])):
            bad.append(k)
    return sorted(bad)


# ------------------------------------------------------- загрузка партиции
def sync_partition(pg, ch: ClickHouse, spec, src, key: str, apply: bool) -> dict:
    started = datetime.now()
    res = {"partition": key, "status": "plan"}
    where = "" if key == ALL else src.partition_filter(key)
    dialect = spec.source_type

    if not apply:
        res["rows_source"] = int(src.conn.get_first(
            f"SELECT count(*) FROM {src.projected(where)}")[0])
        res["rows_target"] = ch.partition_rows(
            spec.fqn, spec.partition_expr, None if key == ALL else key) \
            if ch.table_exists(spec.fqn) else 0
        return res

    fp_src = src.conn.get_first(
        rec.fingerprint_sql(spec, dialect) + f" FROM {src.projected(where)}")
    res["rows_source"] = int(fp_src[0])

    if res["rows_source"] == 0:
        # Пустая партиция: «данные пропали» чаще означает сломанный источник,
        # чем законное отсутствие строк, поэтому по умолчанию — остановка.
        if spec.empty_partition_policy == "fail":
            res["status"] = "failed"
            res["note"] = "в источнике 0 строк, empty_partition_policy=fail"
            _history(pg, spec, "apply", key, "failed", started, 0, None, None, None, res["note"])
            _save_state(pg, spec, key, "failed", error=res["note"])
            return res
        res["note"] = "в источнике 0 строк, партиция очищена (policy=clear)"

    # Партиция уже совпадает с источником — стримить нечего. Сравниваем с ФАКТИЧЕСКОЙ
    # целью, а не с сохранённым отпечатком: так проверяется реальное состояние, а не
    # наше представление о нём. Без этого горячее окно переливалось бы каждый запуск.
    w_dst = "" if key == ALL else f" WHERE {spec.partition_expr} = {int(key)}"
    if ch.partition_rows(spec.fqn, spec.partition_expr, None if key == ALL else key):
        fp_now = ch.row(rec.fingerprint_sql(spec, "clickhouse") + f" FROM {spec.fqn}{w_dst}")
        if not rec.compare(spec, fp_src, fp_now):
            res["status"] = "skip"
            res["rows_target"] = int(fp_now[0])
            _save_state(pg, spec, key, "ok", rows=res["rows_target"])
            return res

    t0 = time.monotonic()
    ch.execute(f"TRUNCATE TABLE {spec.stage_fqn}")
    sql = src.select_sql(where)

    def lines():
        for row in src.stream(sql, spec.batch_size):
            yield "\t".join(fmt_value(v) for v in row) + "\n"

    if spec.lookup:
        # Обогащение: часть колонок в источнике отсутствует и подставляется
        # соединением уже внутри ClickHouse. Так внешний ключ аналитики берётся
        # из НАШЕГО справочника, а чужой идентификатор источника в витрину
        # не попадает. Соединять на источнике нельзя — справочник в другой БД.
        ch.execute(f"TRUNCATE TABLE {spec.raw_fqn}")
        ch.insert_tsv(spec.raw_fqn, [c.target_column for c in spec.stream_columns], lines())
        lk = spec.lookup
        cols, sel = [], []
        for c in spec.columns:
            cols.append(c.target_column)
            if c.source_expr.startswith(spec.LOOKUP_MARK):
                sel.append(f"l.{lk['return']}")
            else:
                sel.append(f"r.{c.target_column}")
        # LEFT JOIN, а не INNER: строка без соответствия не должна молча исчезнуть.
        # Она попадёт в staging с нулём и будет поймана проверкой not_zero и сверкой.
        ch.execute(
            f"INSERT INTO {spec.stage_fqn} ({', '.join(cols)}) SELECT {', '.join(sel)} "
            f"FROM {spec.raw_fqn} r LEFT JOIN {lk['table']} l "
            f"ON l.{lk['lookup_key']} = r.{lk['source_key']}")
        ch.execute(f"TRUNCATE TABLE {spec.raw_fqn}")
    else:
        ch.insert_tsv(spec.stage_fqn, spec.target_columns, lines())
    res["t_stream"] = round(time.monotonic() - t0, 2)

    # --- сверка staging с источником ДО касания цели ---------------------
    t1 = time.monotonic()
    fp_stage = ch.row(rec.fingerprint_sql(spec, "clickhouse") + f" FROM {spec.stage_fqn}")
    diff = rec.compare(spec, fp_src, fp_stage)
    dup_sql = rec.duplicates_sql(spec, spec.stage_fqn)
    dup = int(ch.scalar(dup_sql) or 0) if dup_sql else 0
    # проверки из конфигурации: уникальность ключей, обязательные значения
    extra = []
    for label, sql in rec.extra_checks(spec, spec.stage_fqn):
        n = int(ch.scalar(sql) or 0)
        if n:
            extra.append((label, n))
    res["t_verify"] = round(time.monotonic() - t1, 2)
    res["reconcile"] = rec.to_json(spec, fp_src, fp_stage, diff)
    res["reconcile"]["duplicates"] = dup
    res["reconcile"]["checks"] = {l: n for l, n in extra}

    if diff or dup or extra:
        res["status"] = "failed"
        res["note"] = "; ".join(f"{n}: источник {a} против staging {b}" for n, a, b in diff) \
                      + (f"; дублей business key {dup}" if dup else "") \
                      + "".join(f"; {l}: {n}" for l, n in extra)
        _history(pg, spec, "apply", key, "failed", started, res["rows_source"],
                 int(fp_stage[0]), None, res["reconcile"], res["note"])
        _save_state(pg, spec, key, "failed", error=res["note"])
        return res

    # --- атомарная публикация --------------------------------------------
    t2 = time.monotonic()
    part = "tuple()" if key == ALL else str(int(key))
    existing = ch.partition_rows(spec.fqn, spec.partition_expr, None if key == ALL else key)
    if existing == 0:
        ch.execute(f"ALTER TABLE {spec.stage_fqn} MOVE PARTITION {part} TO TABLE {spec.fqn}")
        res["method"] = "MOVE PARTITION TO TABLE"
    else:
        ch.execute(f"ALTER TABLE {spec.fqn} REPLACE PARTITION {part} FROM {spec.stage_fqn}")
        res["method"] = "REPLACE PARTITION"
    res["t_publish"] = round(time.monotonic() - t2, 2)

    # --- контроль уже на цели --------------------------------------------
    w = "" if key == ALL else f" WHERE {spec.partition_expr} = {int(key)}"
    fp_dst = ch.row(rec.fingerprint_sql(spec, "clickhouse") + f" FROM {spec.fqn}{w}")
    diff_after = rec.compare(spec, fp_src, fp_dst)
    if diff_after:
        res["status"] = "failed"
        res["note"] = "после публикации: " + "; ".join(
            f"{n}: источник {a} против цели {b}" for n, a, b in diff_after)
        _history(pg, spec, "apply", key, "failed", started, res["rows_source"],
                 int(fp_dst[0]), res["method"], rec.to_json(spec, fp_src, fp_dst, diff_after),
                 res["note"])
        _save_state(pg, spec, key, "failed", error=res["note"])
        return res

    ch.execute(f"TRUNCATE TABLE {spec.stage_fqn}")
    res["rows_target"] = int(fp_dst[0])
    res["status"] = "ok"
    res["t_total"] = round(time.monotonic() - t0, 2)

    wm = None
    if spec.watermark_column:
        wm = src.conn.get_first(
            f"SELECT max({spec.watermark_column}) FROM {src.projected(where)}")[0]
    _save_state(pg, spec, key, "ok", rows=res["rows_target"], watermark=wm,
                fingerprint=res["reconcile"]["metrics"])
    _history(pg, spec, "apply", key, "success", started, res["rows_source"],
             res["rows_target"], res["method"], res["reconcile"])
    return res
