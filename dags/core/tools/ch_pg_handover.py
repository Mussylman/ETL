"""
Передача выдачи id документов реестру и остановка записи фактов в PostgreSQL.

    PYTHONPATH=dags python3 -m core.tools.ch_pg_handover --register order,sales            # план
    PYTHONPATH=dags python3 -m core.tools.ch_pg_handover --register order,sales --apply    # выполнить

Выполняется после ch_cutover: ClickHouse уже питается прямым путём, а id документов ещё
выдаёт старый путь. Порядок, при котором у документа ни в какой момент нет двух
выдающих id:

  1. incremental_prod, analytics_sync, clickhouse_sync — на паузу, идущие прогоны дожидаются;
  2. registers.pg_fact_write = false — первый hop больше не пишет факты регистра;
  3. догон реестра etl_meta.doc_key из фактов с сохранением id и проверка:
     каждый документ PostgreSQL есть в реестре с тем же id, guid → id и id → guid
     однозначны. Не сошлось — признаки возвращаются, ничего не переключается;
  4. doc_key_scope.issuer = registry — новые id выдаёт только реестр;
  5. группы второго hop'а (справочники, cost_daily) — в analytics_sync перед фактами;
     у clickhouse_sync групп не остаётся: он на паузе, триггер из incremental_prod
     исчезает (создаётся по реестру групп);
  6. пауза снимается с incremental_prod (справочники) и analytics_sync.

Таблицы фактов PostgreSQL не удаляются и не очищаются — остаются замороженными.
"""

import argparse
import sys

sys.path.insert(0, __file__.rsplit("/core/", 1)[0])

from core.clickhouse import registry                      # noqa: E402
from core.tools.ch_cutover import airflow, quiesce         # noqa: E402

DAGS = ["incremental_prod", "analytics_sync", "clickhouse_sync"]
GROUPS = [("core_pg_to_ch", 10), ("retail", 20)]           # справочники раньше фактов (onec_1c — 15)


def doc_table(pg, register: str) -> str:
    r = pg.get_first("SELECT source_params->>'target' FROM etl_meta.ch_sync WHERE source_type = 'onec_register' "
                     "AND source_object = %s AND is_active AND (source_params->>'own_id')::boolean",
                     parameters=(register,))
    if not r:
        raise RuntimeError(f"{register}: нет боевой конфигурации шапки прямого пути")
    return r[0]


def verify(pg, table: str) -> list:
    """Проблемы реестра по области; пусто — реестр полон и однозначен."""
    q = lambda s: pg.get_first(s, parameters=(table,))[0]
    problems = []
    missing = pg.get_first(f"SELECT count(*) FILTER (WHERE d.id IS NULL), count(*) FILTER (WHERE d.id <> s.id) "
                           f"FROM public.{table} s LEFT JOIN etl_meta.doc_key d ON d.doc_table = %s "
                           f"AND d.recorder = s.recorder AND d.recorder_type = s.recorder_type", parameters=(table,))
    if missing[0]:
        problems.append(f"{table}: {missing[0]} документов PostgreSQL нет в реестре")
    if missing[1]:
        problems.append(f"{table}: у {missing[1]} документов id в реестре ≠ id в PostgreSQL")
    n = q("SELECT count(*) FROM (SELECT recorder FROM etl_meta.doc_key WHERE doc_table = %s "
          "GROUP BY recorder HAVING count(DISTINCT id) > 1) x")
    if n:
        problems.append(f"{table}: {n} guid с несколькими id")
    n = q("SELECT count(*) FROM (SELECT id FROM etl_meta.doc_key WHERE doc_table = %s GROUP BY id HAVING count(*) > 1) x")
    if n:
        problems.append(f"{table}: {n} id с несколькими guid")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description="Реестр документов выдаёт id; факты в PostgreSQL замораживаются")
    ap.add_argument("--register", required=True, help="order,sales")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--config-conn", default="etl_prod")
    args = ap.parse_args()

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=args.config_conn)
    regs = [r.strip() for r in args.register.split(",") if r.strip()]
    tables = {r: doc_table(pg, r) for r in regs}
    mark = "✓" if args.apply else "[plan]"

    if args.apply:
        quiesce(DAGS)
    print(f"   {mark} {', '.join(DAGS)} на паузе, идущих прогонов нет")

    ids = {r: pg.get_first("SELECT id FROM etl_meta.registers WHERE code = %s", parameters=(r,))[0] for r in regs}
    if args.apply:
        for r in regs:
            pg.run("UPDATE etl_meta.registers SET pg_fact_write = false, updated_at = now() WHERE id = %s",
                   parameters=(ids[r],))
    print(f"   {mark} pg_fact_write = false: {', '.join(regs)}")

    problems = []
    for r, t in tables.items():
        if args.apply:
            n = registry.seed_scope(pg, t)
            print(f"   ✓ реестр {t}: догнано {n}")
        problems += verify(pg, t)
    if problems:
        for p in problems:
            print(f"   ✗ {p}")
        if args.apply:
            for r in regs:
                pg.run("UPDATE etl_meta.registers SET pg_fact_write = true, updated_at = now() WHERE id = %s",
                       parameters=(ids[r],))
            for d in DAGS:
                airflow("dags", "unpause", d)
        print("\nОСТАНОВ: реестр не однозначен — признаки возвращены, выдача id не переключалась")
        return 1
    print(f"   {'✓' if args.apply else '·'} реестр однозначен: каждый документ PostgreSQL в нём с тем же id")

    if args.apply:
        for t in tables.values():
            pg.run("UPDATE etl_meta.doc_key_scope SET issuer = 'registry', updated_at = now() WHERE doc_table = %s",
                   parameters=(t,))
    print(f"   {mark} issuer = registry: {', '.join(tables.values())}")

    for g, pos in GROUPS:
        if args.apply:
            pg.run("UPDATE etl_meta.ch_sync_group SET dag_id = 'analytics_sync', position = %s, updated_at = now() "
                   "WHERE sync_group = %s", parameters=(pos, g))
    print(f"   {mark} группы {', '.join(g for g, _ in GROUPS)} → analytics_sync; у clickhouse_sync групп нет")

    if args.apply:
        airflow("dags", "reserialize")
        for d in ("incremental_prod", "analytics_sync"):
            airflow("dags", "unpause", d)
    print(f"   {mark} incremental_prod и analytics_sync без паузы; clickhouse_sync остаётся на паузе")

    print("\nоткат: issuer = pg_facts; pg_fact_write = true; группы core_pg_to_ch/retail → clickhouse_sync; "
          "unpause clickhouse_sync. Документы, получившие id от реестра, старый путь загрузит с новыми id — "
          "реестр при этом остаётся правдой для ClickHouse.")
    if not args.apply:
        print("\nэто план. Выполнить: --apply")
    return 0


if __name__ == "__main__":
    sys.exit(main())
