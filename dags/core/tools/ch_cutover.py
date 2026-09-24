"""
Переключение регистров 1С на прямой путь 1С → ClickHouse.

    PYTHONPATH=dags python3 -m core.tools.ch_cutover --register order,sales               # план + предусловия
    PYTHONPATH=dags python3 -m core.tools.ch_cutover --register order,sales --apply \\
        --ch-config /path/ch_admin.xml                                                    # выполнить

Порядок (docs/knowledge/decisions/Прямой путь 1С → ClickHouse — переключение ...):
  0. предусловия: shadow-таблицы наполнены; закрытые месяцы прямого пути = 1С;
     заказы раньше продаж (продажа ссылается на заказ);
  1. registers.pg_fact_write = false — первый hop перестаёт писать регистр в PostgreSQL;
  2. ждём, пока идущий прогон старого пути отпустит блокировку регистра;
  3. реестр документов: финальный засев из фактов с сохранением id → issuer = registry.
     С этого момента id документов выдаёт только реестр;
  4. ClickHouse (ch_admin): EXCHANGE TABLES fact_X ↔ fact_X_direct (и _stage) — читатели
     видят данные прямого пути под прежним именем; старая таблица остаётся под именем
     *_direct замороженной копией для отката; raw staging и права etl_writer;
  5. ch_sync: конфигурации второго hop'а по этим целям — неактивны (legacy_frozen);
     прямой путь — боевой (shadow=false, свой state_key, группа onec_1c);
  6. пересборка горячего окна боевым прямым путём (заготовки справочников и документов
     теперь создаются) и сверка его с 1С;
  7. когда у clickhouse_sync не остаётся фактов — все группы переходят в analytics_sync,
     clickhouse_sync на паузу, analytics_sync со снятой паузой (цепочка incremental →
     clickhouse_sync исчезает сама — триггер создаётся по реестру групп).

Ничего не удаляется: ни таблицы PostgreSQL, ни старые таблицы ClickHouse, ни история.
"""

import argparse
import json
import subprocess
import sys
import time

sys.path.insert(0, __file__.rsplit("/core/", 1)[0])

from core.clickhouse import onec, onec_reconcile as orc, registry  # noqa: E402
from core.clickhouse.config import load_spec                        # noqa: E402
from core.clickhouse.runner import hot_partitions                   # noqa: E402
from core.clickhouse.target import ClickHouse                       # noqa: E402

SHADOW_GROUP, LIVE_GROUP, LEGACY_GROUP = "shadow_1c", "onec_1c", "legacy_frozen"
ORDER_FIRST = ["order", "sales"]


def pairs(pg, register: str):
    """(direct spec, old spec) по целям регистра."""
    codes = [r[0] for r in pg.get_records(
        "SELECT code FROM etl_meta.ch_sync WHERE source_type = 'onec_register' AND source_object = %s "
        "AND sync_group = %s ORDER BY priority, code", parameters=(register, SHADOW_GROUP))]
    out = []
    for c in codes:
        d = load_spec(pg, c)
        old_table = d.target_table[:-len("_direct")] if d.target_table.endswith("_direct") else None
        r = pg.get_first("SELECT code FROM etl_meta.ch_sync WHERE target_table = %s AND source_type <> 'onec_register'",
                         parameters=(old_table,)) if old_table else None
        out.append((d, load_spec(pg, r[0]) if r else None))
    return out


def preconditions(pg, ch, ms, registers, history: bool):
    problems = []
    for reg in registers:
        ps = pairs(pg, reg)
        if not ps:
            problems.append(f"{reg}: нет shadow-конфигураций в группе {SHADOW_GROUP}")
            continue
        for d, o in ps:
            if o is None:
                problems.append(f"{d.code}: не найдена конфигурация второго hop'а для {d.target_table}")
            if not ch.table_exists(d.fqn) or not int(ch.scalar(f"SELECT count() FROM {d.fqn}") or 0):
                problems.append(f"{d.code}: shadow-таблица пуста или отсутствует")
            if history:
                try:
                    p = orc.plan(pg, d)
                except RuntimeError:
                    continue            # шапка из строк регистра — сверяется через строки
                hot = set(hot_partitions())
                parts = [x for x in ch.query(f"SELECT DISTINCT {d.partition_expr} FROM {d.fqn}").split() if x]
                bad = [x for x in sorted(parts) if x not in hot and not orc.prehistory(p, x)
                       and orc.compare(orc.fingerprint_1c(ms, p, x), orc.fingerprint_ch(ch, d, p, x), p)]
                print(f"   сверка закрытых месяцев {d.code}: {len(parts)} партиций, не сошлось {bad or 'ни одной'}")
                if bad:
                    problems.append(f"{d.code}: закрытые месяцы ≠ 1С: {bad} — сначала пересобрать")
    order = [r for r in ORDER_FIRST if r in registers]
    if "sales" in registers and "order" not in registers:
        issuer = pg.get_first("SELECT issuer FROM etl_meta.doc_key_scope WHERE doc_table = 'orders'")
        if not issuer or issuer[0] != "registry":
            problems.append("sales: заказы должны быть переключены раньше или в той же процедуре")
    return problems, order


def wait_old_path(pg, register_id: int, timeout: int = 900) -> None:
    """Блокировка первого hop'а (одноаргументная, по register_id) свободна — прогон не идёт."""
    t0 = time.monotonic()
    while True:
        conn = pg.get_conn(); cur = conn.cursor()
        try:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (register_id,))
            if cur.fetchone()[0]:
                cur.execute("SELECT pg_advisory_unlock(%s)", (register_id,))
                return
        finally:
            cur.close(); conn.close()
        if time.monotonic() - t0 > timeout:
            raise RuntimeError(f"register_id={register_id}: старый путь не отпустил блокировку за {timeout} с")
        time.sleep(5)


def ch_admin(cfg: str, sql: str) -> None:
    r = subprocess.run(["clickhouse-client", "--config-file", cfg, "--multiquery"], input=sql,
                       capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(r.stderr.strip()[:400])


def cut_register(pg, ch, reg: str, *, apply: bool, ch_cfg: str) -> None:
    from core.tools.ch_ddl import grants
    ps = pairs(pg, reg)
    rid = pg.get_first("SELECT id FROM etl_meta.registers WHERE code = %s", parameters=(reg,))[0]
    header = next(d for d, _ in ps if d.source_params.get("own_id"))
    doc_table = header.source_params["target"]
    shadow_key = header.source_params["state_key"]
    live_key = f"onec_register:{reg}"
    step = (lambda s: print(f"   [plan] {s}")) if not apply else (lambda s: print(f"   ✓ {s}"))

    print(f"\n=== {reg} ===")
    if apply:
        pg.run("UPDATE etl_meta.registers SET pg_fact_write = false, updated_at = now() WHERE id = %s", parameters=(rid,))
    step(f"registers.pg_fact_write = false (id {rid})")
    if apply:
        wait_old_path(pg, rid)
    step("старый путь не держит блокировку регистра")
    if apply:
        n = registry.seed_scope(pg, doc_table)
        pg.run("UPDATE etl_meta.doc_key_scope SET issuer = 'registry', updated_at = now() WHERE doc_table = %s",
               parameters=(doc_table,))
        step(f"реестр документов {doc_table}: засеяно {n}, issuer = registry")
    else:
        step(f"реестр документов {doc_table}: засев из public.{doc_table} с сохранением id, issuer = registry")

    ddl = []
    for d, o in ps:
        ddl += [f"EXCHANGE TABLES {o.fqn} AND {d.fqn}", f"EXCHANGE TABLES {o.stage_fqn} AND {d.stage_fqn}"]
    if apply:
        ch_admin(ch_cfg, ";\n".join(ddl) + ";")
    for x in ddl:
        step(f"ClickHouse: {x}")

    # ch_sync: второй hop замораживается и указывает на свою (теперь *_direct) таблицу,
    # прямой путь — на прежнее имя
    for d, o in ps:
        params = dict(d.source_params); params.update(shadow=False, state_key=live_key)
        sqls = [("UPDATE etl_meta.ch_sync SET is_active = false, sync_group = %s, target_table = %s, "
                 "description = coalesce(description,'') || ' [legacy_frozen: переключено на прямой путь]' "
                 "WHERE code = %s", (LEGACY_GROUP, d.target_table, o.code)),
                ("UPDATE etl_meta.ch_sync SET is_active = true, sync_group = %s, target_table = %s, "
                 "source_params = %s::jsonb WHERE code = %s", (LIVE_GROUP, o.target_table, json.dumps(params), d.code))]
        for q, prm in sqls:
            if apply:
                pg.run(q, parameters=prm)
        step(f"ch_sync: {o.code} → {LEGACY_GROUP} ({d.target_table}); {d.code} → {LIVE_GROUP} ({o.target_table})")
    if apply:
        pg.run("""INSERT INTO etl_meta.ch_source_state (source_key, watermark, last_to_ts, updated_at, details)
                  SELECT %s, watermark, last_to_ts, now(), jsonb_build_object('from', source_key)
                  FROM etl_meta.ch_source_state WHERE source_key = %s
                  ON CONFLICT (source_key) DO NOTHING""", parameters=(live_key, shadow_key))
        live = [load_spec(pg, d.code) for d, _ in ps]
        for s in live:
            if s.needs_raw and not ch.table_exists(s.raw_fqn):
                ch_admin(ch_cfg, s.ddl_raw() + ";")
            ch_admin(ch_cfg, grants(s))
    step(f"watermark {live_key} ← {shadow_key}; raw staging и права etl_writer по новым именам")

    hot = hot_partitions()
    if apply:
        live = [load_spec(pg, d.code) for d, _ in ps]
        rep = onec.run_register(pg, ch, live, mode="rebuild", partitions=hot, shadow=False)
        if rep.get("failed"):
            raise RuntimeError(f"{reg}: пересборка горячего окна не прошла: {rep['failed']}")
    step(f"пересборка горячего окна {hot} боевым прямым путём (заготовки создаются)")


def move_groups(pg, *, apply: bool) -> None:
    left = pg.get_first("SELECT count(*) FROM etl_meta.ch_sync WHERE is_active AND load_mode = 'partitioned' "
                        "AND sync_group = 'core_pg_to_ch'")[0]
    if left:
        print(f"\n   второй hop ещё ведёт {left} фактов — группы остаются у clickhouse_sync")
        return
    q = [("INSERT INTO etl_meta.ch_sync_group (sync_group, dag_id, position, description) "
          "VALUES (%s, 'analytics_sync', 15, 'прямой путь 1С → ClickHouse') "
          "ON CONFLICT (sync_group) DO UPDATE SET dag_id = 'analytics_sync', position = 15, is_active = true, "
          "updated_at = now()", (LIVE_GROUP,)),
         ("UPDATE etl_meta.ch_sync_group SET dag_id = 'analytics_sync', updated_at = now() "
          "WHERE sync_group IN ('core_pg_to_ch', 'retail')", None),
         ("UPDATE etl_meta.ch_sync_group SET is_active = false, updated_at = now() WHERE sync_group = %s", (SHADOW_GROUP,))]
    for sql, prm in q:
        if apply:
            pg.run(sql, parameters=prm)
    print(f"   {'✓' if apply else '[plan]'} группы core_pg_to_ch(10, справочники), {LIVE_GROUP}(15), retail(20) → analytics_sync; "
          f"{SHADOW_GROUP} выключена")
    for cmd in (["dags", "pause", "clickhouse_sync"], ["dags", "unpause", "analytics_sync"]):
        if apply:
            subprocess.run(["airflow", *cmd], check=True, capture_output=True)
        print(f"   {'✓' if apply else '[plan]'} airflow {' '.join(cmd)}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Переключение регистров 1С на прямой путь в ClickHouse")
    ap.add_argument("--register", required=True, help="order,sales")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--ch-config", help="config-file clickhouse-client под ch_admin (для --apply)")
    ap.add_argument("--skip-history", action="store_true", help="не сверять закрытые месяцы с 1С")
    ap.add_argument("--config-conn", default="etl_prod")
    args = ap.parse_args()
    if args.apply and not args.ch_config:
        ap.error("--apply требует --ch-config (EXCHANGE TABLES выполняет ch_admin)")

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    pg = PostgresHook(postgres_conn_id=args.config_conn)
    ch = ClickHouse("clickhouse_etl")
    ms = MsSqlHook(mssql_conn_id="mssql_1c_conn")
    registers = [r.strip() for r in args.register.split(",") if r.strip()]

    print("предусловия:")
    problems, order = preconditions(pg, ch, ms, registers, history=not args.skip_history)
    for p in problems:
        print(f"   ✗ {p}")
    if problems:
        print("\nОСТАНОВ: предусловия не выполнены, ничего не изменено")
        return 1
    print("   ✓ выполнены")

    for reg in order + [r for r in registers if r not in order]:
        cut_register(pg, ch, reg, apply=args.apply, ch_cfg=args.ch_config)
    move_groups(pg, apply=args.apply)

    print("\nоткат (если понадобится): EXCHANGE TABLES обратно; ch_sync — конфигурации местами; "
          "pg_fact_write = true; issuer = pg_facts. Документы, получившие id от реестра за это время, "
          "получат в PostgreSQL новые id от старого пути.")
    if not args.apply:
        print("\nэто план. Выполнить: --apply --ch-config <ch_admin config>")
    return 0


if __name__ == "__main__":
    sys.exit(main())
