"""
Источник onec_register: регистр 1С → ClickHouse напрямую, без фактов в PostgreSQL.

Здесь нет ни одной строки бизнес-логики 1С. Она вся уже описана в метаданных
(registers / register_sources / column_mappings) и исполняется существующим
ETLEngine.extract_frame — тем же кодом, что у первого hop'а. Этот модуль только
подключает к нему три вещи, которых у первого hop'а нет в нужном виде:

  • набор изменений с watermark в control plane, а не MAX() по таблице факта;
  • реестр ключей вместо post_load_sql;
  • публикацию патчем партиций вместо upsert в PostgreSQL.

Один регистр питает несколько целей (шапка и строки), поэтому извлекается один раз
на группу конфигураций, а не на каждую цель.
"""

import io
import contextlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from ..config import refs
from ..extract.data_checker import DataChecker
from . import changes, patch, registry
from .target import ClickHouse

YEAR_OFFSET = 2000   # год в 1С хранится со сдвигом +2000


# ------------------------------------------------------------ набор изменений
class DirectChangeProvider(DataChecker):
    """
    Окно изменений retail и добор хвоста — логика DataChecker без изменений.
    Переопределены только две точки, привязывавшие её к таблице факта:
      watermark — из etl_meta.ch_source_state;
      присутствие документа для добора хвоста — в ClickHouse.

    upper_bound — верхняя граница окна. В shadow это граница последнего успешного
    окна старого пути: так прямой путь берёт только документы, которые старый путь
    уже зарегистрировал в реестре ключей, и сравнение не зависит от гонки двух путей.
    """

    def __init__(self, *, pg_meta, ch: ClickHouse, source_key: str, presence_table: str,
                 upper_bound: Optional[datetime] = None, **kw):
        super().__init__(**kw)
        self.pg_meta, self.ch = pg_meta, ch
        self.source_key, self.presence_table = source_key, presence_table
        self.upper_bound = upper_bound

    def get_last_update(self) -> str:
        r = self.pg_meta.get_first(
            "SELECT watermark FROM etl_meta.ch_source_state WHERE source_key = %s",
            parameters=(self.source_key,))
        if not r or r[0] is None:
            raise RuntimeError(f"{self.source_key}: watermark не задан — источник не засеян")
        return r[0].strftime("%Y-%m-%d %H:%M:%S.%f")

    def _presence_available(self) -> bool:
        return True

    def _present_uids(self, uids: List[str]) -> set:
        if not uids:
            return set()
        lst = ", ".join(f"'{u}'" for u in uids)   # кортеж констант — один узел AST
        out = self.ch.query(f"SELECT DISTINCT toString(recorder) FROM {self.presence_table} "
                            f"WHERE recorder IN ({lst})")
        return {x.strip() for x in out.split("\n") if x.strip()}

    def get_changed_uids(self):
        df, from_ts, to_ts = super().get_changed_uids()
        if self.upper_bound is not None:
            ub = pd.Timestamp(self.upper_bound)
            if len(df):
                df = df[pd.to_datetime(df["updated_at"]) < ub].reset_index(drop=True)
            if pd.Timestamp(to_ts) > ub:
                to_ts = ub.strftime("%Y-%m-%d %H:%M:%S.%f")
        return df, from_ts, to_ts


    def signal_for(self, keys: List[str]) -> pd.DataFrame:
        """Последний сигнал источника по явному списку ключей (режим keys): df[uid, updated_at]."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        rt = PostgresHook(postgres_conn_id=self.retail_conn_id)
        got = rt.get_pandas_df(
            f"SELECT lower({self.key_column}) AS uid, MAX((updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp) AS updated_at "
            f"FROM public.{self.retail_table} WHERE lower({self.key_column}) = ANY(%s) GROUP BY 1", parameters=(list(keys),))
        ts = dict(zip(got["uid"], got["updated_at"]))
        return pd.DataFrame({"uid": list(keys), "updated_at": [ts.get(k, pd.NaT) for k in keys]})


def old_path_upper_bound(pg, register_id: int) -> Optional[datetime]:
    """Верхняя граница последнего успешного окна старого пути (из load_history)."""
    r = pg.get_first(
        "SELECT checkpoint_value FROM etl_meta.load_history "
        "WHERE register_id = %s AND run_mode = 'incremental' AND status = 'success' "
        "  AND checkpoint_value LIKE '%%to=%%' ORDER BY id DESC LIMIT 1", parameters=(register_id,))
    if not r:
        return None
    for part in r[0].split(";"):
        part = part.strip()
        if part.startswith("to="):
            return datetime.fromisoformat(part[3:].strip())
    return None


# ------------------------------------------------------------ подготовка кадра
def _typed(series: pd.Series, ch_type: str) -> pd.Series:
    """Значение под тип ClickHouse. Пустое заменяется так же, как у старого пути (coalesce)."""
    t = ch_type.replace("LowCardinality(", "").rstrip(")")
    if t.startswith(("UInt", "Int")):
        return pd.to_numeric(series, errors="coerce").fillna(0).astype("int64")
    if t.startswith(("Decimal", "Float")):
        return pd.to_numeric(series, errors="coerce").fillna(0.0)
    if t == "UUID":
        return series.map(lambda v: str(v).lower() if v is not None and str(v) != "nan"
                          else registry.EMPTY_REF)
    if t.startswith("DateTime"):
        s = pd.to_datetime(series, errors="coerce")
        return s.fillna(pd.Timestamp("1970-01-01")).dt.floor("s")
    if t == "Date":
        s = pd.to_datetime(series, errors="coerce")
        return s.fillna(pd.Timestamp("1970-01-01"))
    return series.map(lambda v: "" if v is None or (isinstance(v, float) and v != v) else str(v))


def prepare(spec, df: pd.DataFrame) -> pd.DataFrame:
    """Колонки цели из подготовленного кадра: source_expr — имя колонки кадра, тип — из конфига."""
    if df.empty:
        # Законный случай: все документы набора изменений из 1С исчезли. Свежих строк
        # нет, а патч по-прежнему должен убрать эти документы из их партиций.
        return pd.DataFrame(columns=spec.target_columns)
    out = pd.DataFrame(index=df.index)
    for c in spec.columns:
        src = c.source_expr
        if src not in df.columns:
            raise RuntimeError(f"{spec.code}: в кадре нет колонки {src!r} для {c.target_column}")
        out[c.target_column] = _typed(df[src], c.target_type)
    return out.reset_index(drop=True)


# ------------------------------------------------------------ прогон регистра
def _period_bounds(partition: str) -> Tuple[str, str]:
    """Партиция YYYYMM → границы в датах 1С (+2000 к году)."""
    y, m = int(partition[:4]), int(partition[4:6])
    a = f"{y + YEAR_OFFSET:04d}-{m:02d}-01"
    b = f"{y + YEAR_OFFSET + (m == 12):04d}-{(1 if m == 12 else m + 1):02d}-01"
    return a, b


@contextlib.contextmanager
def source_lock(pg, source_key: str):
    """
    Один прогон на источник: патч раз в 5 минут и пересборка горячего окна пишут в
    одну и ту же партицию текущего месяца и без блокировки затёрли бы друг друга.

    Двухаргументная форма pg_advisory_lock(int, int) — отдельное пространство ключей
    от одноаргументной (bigint), которую берёт первый hop по register_id. Поэтому
    прямой путь не может заблокировать старый, даже на том же регистре.
    """
    conn = pg.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT pg_try_advisory_lock(hashtext('ch_source'), hashtext(%s))", (source_key,))
        if not cur.fetchone()[0]:
            raise RuntimeError(f"{source_key}: другой прогон этого источника уже идёт")
        conn.commit()
        yield
    finally:
        try:
            cur.execute("SELECT pg_advisory_unlock(hashtext('ch_source'), hashtext(%s))", (source_key,))
            conn.commit()
        finally:
            cur.close()
            conn.close()


def run_register(pg, ch: ClickHouse, specs: List, **kw) -> Dict:
    """Регистр 1С → все его цели, под блокировкой источника."""
    params0 = specs[0].source_params or {}
    key = params0.get("state_key") or f"onec_register:{specs[0].source_object}"
    with source_lock(pg, key):
        return _run_register(pg, ch, specs, **kw)


def _run_register(pg, ch: ClickHouse, specs: List, *, mode: str = "patch",
                  partitions: Optional[List[str]] = None, keys: Optional[List[str]] = None,
                  shadow: bool = True, quiet: bool = True) -> Dict:
    """
    Один регистр → все его цели.
      mode='patch'   — набор изменений из retail, публикация патчем
      mode='rebuild' — полная пересборка указанных партиций из 1С (ремонт, sweep, backfill)
      mode='keys'    — патч явного списка документов (ремонт, документы до начала истории);
                       та же классификация, watermark не трогается
    shadow=True — пишет только в shadow-таблицы (влияет на run_mode истории).

    Заготовки справочников прямой путь создаёт всегда: это тот же идемпотентный
    INSERT … ON CONFLICT (guid) DO NOTHING, что у post_load старого пути, id выдаёт
    IDENTITY справочника — двух выдающих не бывает. Кто выдаёт id документов, решает
    реестр (doc_key_scope.issuer): пока это старый путь (pg_facts), окно изменений
    ограничено сверху его последним успешным окном — прямой путь берёт только
    документы, которым старый путь уже выдал id.
    """
    from ..etl_engine import ETLEngine, _now_local

    register = specs[0].source_object
    params0 = specs[0].source_params or {}
    source_key = params0.get("state_key") or f"onec_register:{register}"
    specs = sorted(specs, key=lambda s: s.priority)
    header = next((s for s in specs if s.source_params.get("own_id")), specs[0])

    sink = io.StringIO()
    with contextlib.redirect_stdout(sink) if quiet else contextlib.nullcontext():
        engine = ETLEngine(register, mode="incremental", config_conn_id="etl_prod", dst_conn_id="etl_prod")
    cfg = engine.config
    report: Dict = {"register": register, "mode": mode, "source_key": source_key}

    # id документов выдаёт старый путь — окно не обгоняет его
    own_scope = header.source_params.get("target")
    follows_old_path = registry._scope(pg, own_scope)[0] == "pg_facts" if own_scope else True

    if mode == "rebuild":
        early = sorted(p for p in (partitions or []) if any(patch.prehistory(sp, p) for sp in specs))
        if early:
            raise RuntimeError(f"{source_key}: партиции {early} раньше начала истории (history_from) — "
                               f"их не пересобирают целиком; отдельные документы — mode='keys'")

    uid_ts: Dict[str, datetime] = {}
    changed: List[str] = []
    to_ts = None
    cs = None
    targets = engine._get_active_targets()
    if mode in ("patch", "keys"):
        prov = DirectChangeProvider(
            pg_meta=pg, ch=ch, source_key=source_key, presence_table=header.fqn,
            upper_bound=old_path_upper_bound(pg, cfg.id) if follows_old_path else None,
            retail_table=cfg.retail_table, retail_conn_id=engine.retail_conn_id,
            config_conn_id="etl_prod", register_id=cfg.id, key_column=cfg.retail_uid_column,
            etl_table=None, etl_conn_id=None)
        hdr_target = next(t for t in targets if t.target_table == header.source_params["target"])
        with contextlib.redirect_stdout(sink) if quiet else contextlib.nullcontext():
            if mode == "keys":
                cdf, from_ts, to_ts = prov.signal_for(sorted({changes.norm_key(k) for k in keys or []} - {""})), \
                    "1970-01-01 00:00:00", None
            else:
                cdf, from_ts, to_ts = prov.get_changed_uids()
                cdf = _with_unresolved(ch, header, cdf)
            cs = changes.classify(cdf, from_ts, to_ts,
                                  ready_lookup=lambda keys: engine.ready_keys(hdr_target, keys),
                                  present_lookup=prov._present_uids)
        uid_ts = cs.signal_ts
        changed = cs.patch_keys
        report.update(cs.summary())
        if not changed:
            # всё ждёт источник: ничего не извлекаем и не публикуем. Окно двигается —
            # ожидающие документы не в витрине, поэтому остаются в хвосте и
            # перепроверяются следующими циклами.
            if mode == "patch":
                _save_watermark(pg, source_key, to_ts, report)
            report.update(patched=0, published={})
            return report

    # извлечение один раз на регистр — все цели
    frames: Dict[str, pd.DataFrame] = {}
    returned: set = set()
    for t in targets:
        with contextlib.redirect_stdout(sink) if quiet else contextlib.nullcontext():
            if mode in ("patch", "keys"):
                if cs.ready:
                    df, _, got = engine.extract_frame(t, key_values=sorted(cs.ready))
                else:
                    df, got = pd.DataFrame(), set()      # только удаления
            else:
                parts = []
                got = set()
                for p in partitions:
                    a, b = _period_bounds(p)
                    d, _, g = engine.extract_frame(t, period_start=a, period_end=b)
                    parts.append(d); got |= g
                df = pd.concat([d for d in parts if len(d)], ignore_index=True) if any(len(d) for d in parts) \
                    else parts[0]
        returned |= {registry._norm_uuid(x) for x in got}
        if df.empty:
            frames[t.target_table] = df
            continue
        own = t.target_table if t.target_role == "dimension" else None
        df = registry.resolve(pg, df, refs.dim_links(pg, t.id), refs.register_links(pg, t.id),
                              own_table=own, create_stubs=True)
        df["etl_updated_at"] = _now_local()
        if t.target_role == "dimension":
            df["retail_updated_at"] = df["recorder"].map(
                lambda r: uid_ts.get(registry._norm_uuid(r))) if uid_ts else None
        frames[t.target_table] = df

    hdr_rb = next((t.target_table for t in targets if t.target_role == "dimension"), None)
    if mode == "rebuild" and hdr_rb and len(frames[hdr_rb]):
        # Окно извлечения — BETWEEN построителя запроса, включительно с обеих сторон:
        # документ ровно на полуночи 1-го числа следующего месяца попадает в оба окна.
        # Старому пути это безвредно (upsert по документу), а здесь такой документ
        # иначе лёг бы в соседнюю партицию и заменил её целиком. Оставляем только
        # документы, чей период шапки строго внутри пересобираемых месяцев; «полуночный»
        # документ достанется пересборке своего месяца.
        hdr_t = hdr_rb
        per = pd.to_datetime(frames[hdr_t]["period"])
        keep = per.dt.strftime("%Y%m").isin(set(partitions))
        allowed = set(frames[hdr_t].loc[keep, "recorder"].map(registry._norm_uuid))
        dropped = int((~keep).sum())
        for name, df in frames.items():
            frames[name] = df[df["recorder"].map(registry._norm_uuid).isin(allowed)].reset_index(drop=True)
        report["outside_partition_docs"] = dropped

    if mode in ("patch", "keys"):
        # Lookup и извлечение — два запроса; извлекаются только ключи, найденные lookup'ом.
        # Документ, исчезнувший между ними, — состояние не определено: ничего не
        # публикуем, окно не двигаем, следующий цикл повторит.
        hdr_returned = {registry._norm_uuid(x) for x in frames[hdr_target.target_table]["recorder"]} \
            if len(frames[hdr_target.target_table]) else set()
        lost = cs.ready - hdr_returned
        if lost:
            raise RuntimeError(f"{source_key}: {len(lost)} документов найдены lookup'ом, но не извлечены "
                               f"(например {sorted(lost)[:3]}) — публикация отменена")
        report["patched"] = len(changed)

    # публикация: цель за целью в порядке priority
    published: Dict[str, List] = {}
    for spec in specs:
        sp = spec.source_params
        df = frames[sp["target"]].copy()
        if sp.get("parent") and len(df):
            pk, pref = sp.get("parent_key", ["recorder", "recorder_type"]), sp.get("parent_prefix", "hdr_")
            hdr = frames[sp["parent"]]
            hdr = hdr.rename(columns={c: f"{pref}{c}" for c in hdr.columns if c not in pk})
            df = df.merge(hdr, on=pk, how="left")
            lost = int(df[f"{pref}period"].isna().sum()) if f"{pref}period" in df.columns else 0
            if lost:
                raise RuntimeError(f"{spec.code}: у {lost} строк нет шапки в том же извлечении")
        frame = prepare(spec, df)
        res = patch.publish(pg, ch, spec, frame, changed,
                            rebuild=partitions if mode == "rebuild" else None,
                            run_mode="shadow" if shadow else mode)
        published[spec.code] = res
        if any(r["status"] == "failed" for r in res):
            report["published"] = published
            report["failed"] = spec.code
            return report          # watermark не двигается: следующий цикл повторит окно

    if mode == "patch":
        _save_watermark(pg, source_key, to_ts, report)
    report["published"] = published
    return report


def _with_unresolved(ch: ClickHouse, header, cdf: pd.DataFrame, days: int = 15) -> pd.DataFrame:
    """
    Документы витрины с неразрешённым собственным id (0) за последние days суток —
    в набор изменений: id выдал ещё не старый путь (он загрузит документ позже), и
    retail повторно о документе не сообщит. Повторяются, пока id не появится.
    """
    if not header.source_params.get("own_id") or "id" not in header.target_columns:
        return cdf
    out = ch.query(f"SELECT toString(recorder), max(retail_updated_at) FROM {header.fqn} "
                   f"WHERE id = 0 AND period >= now() - INTERVAL {int(days)} DAY GROUP BY recorder")
    rows = [l.split("\t") for l in out.splitlines() if l.strip()]
    if not rows:
        return cdf
    extra = pd.DataFrame({"uid": [r[0] for r in rows],
                          "updated_at": pd.to_datetime([r[1] for r in rows], errors="coerce")})
    extra = extra[~extra["uid"].isin(set(cdf["uid"].astype(str).str.lower()))]
    # метка — та, что уже в витрине (она же вернётся в retail_updated_at шапки)
    extra["updated_at"] = extra["updated_at"].fillna(pd.Timestamp("1970-01-01"))
    return pd.concat([cdf, extra], ignore_index=True) if len(extra) else cdf


def _save_watermark(pg, source_key: str, to_ts, report: Dict) -> None:
    import json
    pg.run("""INSERT INTO etl_meta.ch_source_state (source_key, watermark, last_to_ts, updated_at, details)
              VALUES (%s, %s, %s, now(), %s)
              ON CONFLICT (source_key) DO UPDATE SET watermark = EXCLUDED.watermark,
                     last_to_ts = EXCLUDED.last_to_ts, updated_at = now(), details = EXCLUDED.details""",
           parameters=(source_key, to_ts, to_ts,
                       json.dumps({k: v for k, v in report.items() if k != "published"}, default=str)))
