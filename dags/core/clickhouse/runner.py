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

Любое расхождение — исключение: плохая партиция не публикуется, цель остаётся
прежней, причина — в ch_sync_history и ch_sync_partition_state.
"""

from collections import OrderedDict
from typing import Dict, List, Optional

from . import engine as eng
from . import onec
from .config import load_group_specs
from .source import open_source
from .target import ClickHouse


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
