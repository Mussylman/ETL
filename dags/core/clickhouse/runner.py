"""
Универсальный runner: группа конфигураций → источники → ClickHouse.

Единственное, что вызывает Airflow. DAG передаёт имя группы и режим — и больше
ничего не знает: ни таблиц, ни источников, ни порядка. Всё это в etl_meta.ch_sync.
Новый объект попадает в загрузку записью конфигурации с нужной sync_group.

Диспетчеризация по типу источника:
  postgres / mssql  — поиск затронутых партиций и их перезаливка (core.clickhouse.engine);
  onec_register     — регистр 1С целиком, одним извлечением на все его цели
                      (core.clickhouse.onec), патчем документов или пересборкой.

Порядок — priority из конфигурации. Регистр 1С обрабатывается на месте своей
первой конфигурации, все его цели вместе: они питаются одним извлечением.

Режимы регистров 1С:
  patch   — набор изменений retail (+ хвост), патч документов;
  rebuild — пересборка явно указанных партиций;
  hot     — пересборка горячего окна (текущий и прошлый месяц): слепая зона retail —
            документы без сигнала, правки некассовых документов;
  sweep   — сверка всей истории с 1С; несошедшиеся месяцы пересобираются.
Для остальных источников режим не важен: у них свой поиск затронутых партиций.

Любое расхождение — исключение: плохая партиция не публикуется, цель остаётся
прежней, причина — в ch_sync_history и ch_sync_partition_state.
"""

from collections import OrderedDict
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from . import engine as eng
from . import onec
from .config import load_group_specs
from .source import open_source
from .target import ClickHouse


def hot_partitions(now: Optional[datetime] = None) -> List[str]:
    """Текущий и прошлый месяц по бизнес-времени (Almaty)."""
    now = now or datetime.now(ZoneInfo("Asia/Almaty"))
    prev = (now.year - (now.month == 1), 12 if now.month == 1 else now.month - 1)
    return [f"{prev[0]:04d}{prev[1]:02d}", f"{now.year:04d}{now.month:02d}"]


def sweep_partitions(pg, ch, ms, specs: List) -> Dict[str, List]:
    """
    Партиции регистра, где ClickHouse не совпал с 1С, — по всем целям, которые умеют
    сверяться (шапка, собранная из строк регистра, сверяется через строки).
    """
    from . import onec_reconcile as orc
    bad: Dict[str, List] = {}
    for spec in specs:
        try:
            p = orc.plan(pg, spec)
        except RuntimeError:
            continue
        parts = [x for x in ch.query(f"SELECT DISTINCT {spec.partition_expr} FROM {spec.fqn}").split() if x]
        for part in sorted(parts):
            if orc.prehistory(p, part):
                continue
            d = orc.compare(orc.fingerprint_1c(ms, p, part), orc.fingerprint_ch(ch, spec, p, part), p)
            if d:
                bad.setdefault(part, []).append((spec.code, d[:3]))
    return bad


def run_group(group: str, *, mode: str = "patch", partitions: Optional[List[str]] = None,
              include_inactive: bool = False, config_conn_id: str = "etl_prod",
              ch_conn_id: str = "clickhouse_etl") -> Dict:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=config_conn_id)
    ch = ClickHouse(ch_conn_id)
    specs = load_group_specs(pg, group, include_inactive=include_inactive)
    if not specs:
        return {"group": group, "objects": 0, "failed": []}

    registers: "OrderedDict[tuple, List]" = OrderedDict()
    order: List = []
    for s in specs:
        if s.source_type == "onec_register":
            key = (s.source_object, (s.source_params or {}).get("state_key"))
            if key not in registers:
                registers[key] = []
                order.append(("onec", key))
            registers[key].append(s)
        else:
            order.append(("generic", s))

    report: Dict = {"group": group, "objects": len(specs), "results": [], "failed": []}
    for kind, item in order:
        if kind == "onec":
            grp = registers[item]
            shadow = bool((grp[0].source_params or {}).get("shadow"))
            if mode == "hot":
                rep = onec.run_register(pg, ch, grp, mode="rebuild", partitions=hot_partitions(), shadow=shadow)
            elif mode == "sweep":
                from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
                bad = sweep_partitions(pg, ch, MsSqlHook(mssql_conn_id="mssql_1c_conn"), grp)
                rep = {"register": item[0], "mode": "sweep", "mismatched": {k: v for k, v in bad.items()}}
                if bad:
                    rep.update(onec.run_register(pg, ch, grp, mode="rebuild", partitions=sorted(bad), shadow=shadow))
                    rep["mode"] = "sweep"
            else:
                rep = onec.run_register(pg, ch, grp, mode=mode, partitions=partitions, shadow=shadow)
            report["results"].append(rep)
            if rep.get("failed"):
                report["failed"].append(f"{item[0]}: {rep['failed']}")
                break
            continue
        spec = item
        if not ch.table_exists(spec.fqn):
            report["failed"].append(f"{spec.code}: нет таблицы {spec.fqn}")
            break
        src = open_source(spec)
        plan = eng.affected_partitions(pg, ch, spec, src)
        done = 0
        for key in plan["keys"]:
            res = eng.sync_partition(pg, ch, spec, src, key, apply=True)
            if res["status"] == "failed":
                report["failed"].append(f"{spec.code}/{key}: {res.get('note')}")
                break
            done += res["status"] == "ok"
        report["results"].append({"code": spec.code, "partitions": len(plan["keys"]), "reloaded": done})
        if report["failed"]:
            break

    if report["failed"]:
        # цель не изменена ни по одной несошедшейся партиции — падаем, чтобы это было видно
        raise RuntimeError(f"группа {group} не сошлась:\n  " + "\n  ".join(report["failed"]))
    return report


SWEEP_HOUR = 3          # ночная сверка — после 03:00 по бизнес-времени


def due_mode(pg, dag_id: str, now: Optional[datetime] = None) -> str:
    """
    Режим прогона по отметкам последнего успешного выполнения в control plane, а не по
    минуте слота: прогон, пропустивший слот (предыдущий шёл дольше 5 минут), не отменяет
    ни сверку, ни пересборку — их выполнит первый же следующий прогон.
      sweep — последней полной сверки не было после сегодняшних SWEEP_HOUR:00;
      hot   — в текущем часу горячее окно ещё не пересобиралось;
      patch — иначе.
    """
    from datetime import timedelta
    now = now or datetime.now(ZoneInfo("Asia/Almaty")).replace(tzinfo=None)
    last = {k.rsplit(":", 1)[1]: ts for k, ts in pg.get_records(
        "SELECT source_key, watermark FROM etl_meta.ch_source_state WHERE source_key IN (%s, %s)",
        parameters=(f"{dag_id}:sweep", f"{dag_id}:hot"))}
    sweep_due = now.replace(hour=SWEEP_HOUR, minute=0, second=0, microsecond=0)
    if now < sweep_due:
        sweep_due -= timedelta(days=1)
    if last.get("sweep") is None or last["sweep"] < sweep_due:
        return "sweep"
    hour = now.replace(minute=0, second=0, microsecond=0)
    if last.get("hot") is None or last["hot"] < hour:
        return "hot"
    return "patch"


def _mark_done(pg, dag_id: str, kinds: List[str]) -> None:
    now = datetime.now(ZoneInfo("Asia/Almaty")).replace(tzinfo=None)
    for k in kinds:
        pg.run("""INSERT INTO etl_meta.ch_source_state (source_key, watermark, last_to_ts, updated_at, details)
                  VALUES (%s, %s, %s, now(), '{}'::jsonb)
                  ON CONFLICT (source_key) DO UPDATE SET watermark = EXCLUDED.watermark,
                         last_to_ts = EXCLUDED.last_to_ts, updated_at = now()""",
               parameters=(f"{dag_id}:{k}", now, now))


def run_dag(dag_id: str, *, mode: str = "auto", config_conn_id: str = "etl_prod",
            ch_conn_id: str = "clickhouse_etl") -> Dict:
    """
    Все активные группы DAG'а по position (etl_meta.ch_sync_group) — в момент выполнения.
    Состав и порядок групп — конфигурация, а не структура DAG.

    Группы изолированы: сбой одной (например, cost_daily) не останавливает и не
    перезапускает другие — каждая публикует только то, что у неё сошлось. Ошибки
    собираются и поднимаются в конце, прогон виден упавшим. Отметки hot / sweep
    ставятся, только если все группы с регистрами 1С прошли.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=config_conn_id)
    if mode == "auto":
        mode = due_mode(pg, dag_id)
    groups = [r[0] for r in pg.get_records(
        "SELECT sync_group FROM etl_meta.ch_sync_group WHERE dag_id = %s AND is_active "
        "ORDER BY position, sync_group", parameters=(dag_id,))]
    onec_groups = {r[0] for r in pg.get_records(
        "SELECT DISTINCT sync_group FROM etl_meta.ch_sync WHERE is_active AND source_type = 'onec_register'")}
    out = {"dag_id": dag_id, "mode": mode, "groups": {}, "failed": {}}
    for g in groups:
        try:
            out["groups"][g] = run_group(g, mode=mode, config_conn_id=config_conn_id, ch_conn_id=ch_conn_id)
        except Exception as e:  # noqa: BLE001 — любая ошибка группы изолирована
            out["failed"][g] = str(e)[:2000]
            print(f"✗ группа {g}: {str(e)[:500]}")
    if mode in ("hot", "sweep") and not (set(out["failed"]) & onec_groups):
        _mark_done(pg, dag_id, ["hot", "sweep"] if mode == "sweep" else ["hot"])
    if out["failed"]:
        raise RuntimeError(f"{dag_id} [{mode}]: не сошлись группы {sorted(out['failed'])}:\n  "
                           + "\n  ".join(f"{g}: {m[:300]}" for g, m in out["failed"].items()))
    return out
