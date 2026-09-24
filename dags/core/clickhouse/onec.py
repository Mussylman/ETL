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
from . import patch, registry
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
        lst = ", ".join(f"toUUID('{u}')" for u in uids)
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
                  partitions: Optional[List[str]] = None, shadow: bool = True,
                  quiet: bool = True) -> Dict:
    """
    Один регистр → все его цели.
      mode='patch'   — набор изменений из retail, публикация патчем
      mode='rebuild' — полная пересборка указанных партиций из 1С (ремонт, sweep, backfill)
    shadow=True — реестр ключей только читается: прямой путь не пишет в PostgreSQL ничего,
    кроме собственного состояния.
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

    uid_ts: Dict[str, datetime] = {}
    changed: List[str] = []
    to_ts = None
    if mode == "patch":
        prov = DirectChangeProvider(
            pg_meta=pg, ch=ch, source_key=source_key, presence_table=header.fqn,
            upper_bound=old_path_upper_bound(pg, cfg.id) if shadow else None,
            retail_table=cfg.retail_table, retail_conn_id=engine.retail_conn_id,
            config_conn_id="etl_prod", register_id=cfg.id, key_column=cfg.retail_uid_column,
            etl_table=None, etl_conn_id=None)
        with contextlib.redirect_stdout(sink) if quiet else contextlib.nullcontext():
            cdf, from_ts, to_ts = prov.get_changed_uids()
        for u, ts in zip(cdf["uid"].tolist(), cdf["updated_at"].tolist()):
            n = registry._norm_uuid(u)
            if n:
                uid_ts[n] = ts
        changed = sorted(uid_ts)
        report.update(window=(str(from_ts), str(to_ts)), changed=len(changed))
        if not changed:
            _save_watermark(pg, source_key, to_ts, report)
            report["published"] = {}
            return report

    # извлечение один раз на регистр — все цели
    frames: Dict[str, pd.DataFrame] = {}
    returned: set = set()
    for t in engine._get_active_targets():
        with contextlib.redirect_stdout(sink) if quiet else contextlib.nullcontext():
            if mode == "patch":
                df, _, got = engine.extract_frame(t, key_values=changed)
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
        own = t.target_table if t.target_role == "dimension" else None
        df = registry.resolve(pg, df, refs.dim_links(pg, t.id), refs.register_links(pg, t.id),
                              own_table=own, create_stubs=not shadow)
        df["etl_updated_at"] = _now_local()
        if t.target_role == "dimension":
            df["retail_updated_at"] = df["recorder"].map(
                lambda r: uid_ts.get(registry._norm_uuid(r))) if uid_ts else None
        frames[t.target_table] = df

    if mode == "rebuild":
        # Окно извлечения — BETWEEN построителя запроса, включительно с обеих сторон:
        # документ ровно на полуночи 1-го числа следующего месяца попадает в оба окна.
        # Старому пути это безвредно (upsert по документу), а здесь такой документ
        # иначе лёг бы в соседнюю партицию и заменил её целиком. Оставляем только
        # документы, чей период шапки строго внутри пересобираемых месяцев; «полуночный»
        # документ достанется пересборке своего месяца.
        hdr_t = next(t.target_table for t in engine._get_active_targets() if t.target_role == "dimension")
        per = pd.to_datetime(frames[hdr_t]["period"])
        keep = per.dt.strftime("%Y%m").isin(set(partitions))
        allowed = set(frames[hdr_t].loc[keep, "recorder"].map(registry._norm_uuid))
        dropped = int((~keep).sum())
        for name, df in frames.items():
            frames[name] = df[df["recorder"].map(registry._norm_uuid).isin(allowed)].reset_index(drop=True)
        report["outside_partition_docs"] = dropped

    deleted = sorted(set(changed) - returned) if mode == "patch" else []
    report["returned"] = len(returned)
    report["deleted"] = len(deleted)

    # публикация: цель за целью в порядке priority
    published: Dict[str, List] = {}
    for spec in specs:
        sp = spec.source_params
        df = frames[sp["target"]].copy()
        if sp.get("parent"):
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


def _save_watermark(pg, source_key: str, to_ts, report: Dict) -> None:
    import json
    pg.run("""INSERT INTO etl_meta.ch_source_state (source_key, watermark, last_to_ts, updated_at, details)
              VALUES (%s, %s, %s, now(), %s)
              ON CONFLICT (source_key) DO UPDATE SET watermark = EXCLUDED.watermark,
                     last_to_ts = EXCLUDED.last_to_ts, updated_at = now(), details = EXCLUDED.details""",
           parameters=(source_key, to_ts, to_ts,
                       json.dumps({k: v for k, v in report.items() if k != "published"}, default=str)))
