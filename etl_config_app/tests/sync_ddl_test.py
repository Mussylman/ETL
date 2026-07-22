"""
Тест Sync-DDL (этап 0.2): compute_sync_plan/apply_sync_plan создают таблицы
с контрактными инвариантами — PK(id), UNIQUE(upsert_keys), updated_at у dim,
FK-колонка fact→dim; на непустой таблице UNIQUE с дублями требует confirm.

Изоляция: конфиг пишется в схему etl_test (dao.SCHEMA подменяется),
таблицы создаются тоже в etl_test. Боевые etl_meta/public не затрагиваются.

Запуск: python3 etl_config_app/tests/sync_ddl_test.py
(после golden_sales_test.py — нужен эталон в etl_test_ref)
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_APP_DIR = os.path.dirname(_HERE)
for p in (_APP_DIR, _HERE):
    if p not in sys.path:
        sys.path.insert(0, p)

import dao  # noqa: E402
from spec_reader import read_spec  # noqa: E402
from spec_writer import write_spec  # noqa: E402

dao.SCHEMA = "etl_test"  # вся мета-запись и чтение — в тестовой схеме

DIM, FACT = "ddl_probe_dim", "ddl_probe_fact"

_failures = []


def check(name, ok, detail=""):
    print(("✅" if ok else "❌"), name + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        _failures.append(name)


def constraints(table):
    return dao._existing_constraints("etl_test", table)


def columns(table):
    return dao._existing_columns("etl_test", table)


def main():
    conn = dao.get_conn()

    # 0. Пробный spec: эталон sales → код ddl_probe, таблицы в схеме etl_test
    spec = read_spec("sales", schema="etl_test_ref", conn=conn)
    spec.code = "ddl_probe"
    spec.name = "DDL probe"
    for t, tbl in zip(spec.targets, (DIM, FACT)):
        t.target_schema = "etl_test"
        t.target_table = tbl
    conn.close()

    for tbl in (FACT, DIM):  # fact первым: FK
        dao.execute(f'DROP TABLE IF EXISTS "etl_test"."{tbl}" CASCADE')

    result = write_spec(spec, schema="etl_test", replace=True)
    dim_id = result["target_ids"][f"etl_test.{DIM}"]
    fact_id = result["target_ids"][f"etl_test.{FACT}"]

    # 1. CREATE TABLE: dim → fact (FK требует dim)
    for tid, tbl in ((dim_id, DIM), (fact_id, FACT)):
        plan = dao.compute_sync_plan(tid)
        applied = dao.apply_sync_plan(plan, confirm=False)
        check(f"{tbl}: создана без ошибок", applied["applied"] and not applied["errors"],
              str(applied.get("errors")))

    dim_cons, fact_cons = constraints(DIM), constraints(FACT)
    dim_cols, fact_cols = columns(DIM), columns(FACT)

    # 2. Инварианты dim
    check("dim: PK(id)", any(c["type"] == "p" and c["cols"] == {"id"} for c in dim_cons))
    check("dim: id BIGSERIAL", dim_cols.get("id", {}).get("data_type") == "bigint")
    check("dim: UNIQUE(recorder)", any(c["type"] == "u" and c["cols"] == {"recorder"} for c in dim_cons))
    check("dim: updated_at есть", "updated_at" in dim_cols)

    # 3. Инварианты fact
    check("fact: PK(id)", any(c["type"] == "p" and c["cols"] == {"id"} for c in fact_cons))
    check("fact: UNIQUE(recorder, line_no)",
          any(c["type"] == "u" and c["cols"] == {"recorder", "line_no"} for c in fact_cons))
    check(f"fact: FK-колонка {DIM}_id", f"{DIM}_id" in fact_cols)
    check("fact: FK-констрейнт на dim", any(c["type"] == "f" for c in fact_cons))
    check("fact: без updated_at (role=fact)", "updated_at" not in fact_cols)

    # 4. Идемпотентность: повторный план — без действий
    for tid, tbl in ((dim_id, DIM), (fact_id, FACT)):
        plan = dao.compute_sync_plan(tid)
        check(f"{tbl}: повторный план пуст (идемпотентно)", plan["actions"] == [],
              str([(a["kind"], a["col"]) for a in plan["actions"]]))

    # 5. UNIQUE на непустой таблице с дублями → destructive + requires_confirm
    uq = next(c["name"] for c in constraints(DIM) if c["type"] == "u")
    dao.execute(f'ALTER TABLE "etl_test"."{DIM}" DROP CONSTRAINT "{uq}"')
    dao.execute(f'''
        INSERT INTO "etl_test"."{DIM}" (period, recorder, line_no)
        VALUES ('2025-01-01', 'a1b2c3d4-e5f6-7788-99aa-bbccddeeff00', 1),
               ('2025-01-02', 'a1b2c3d4-e5f6-7788-99aa-bbccddeeff00', 2)
    ''')
    plan = dao.compute_sync_plan(dim_id)
    uq_action = next((a for a in plan["actions"] if a["kind"] == "add_unique"), None)
    check("дубли: add_unique предложен", uq_action is not None)
    check("дубли: помечен destructive с причиной",
          bool(uq_action) and uq_action["destructive"] and "дубл" in uq_action["reason"],
          str(uq_action))
    applied = dao.apply_sync_plan(plan, confirm=False)
    check("дубли: без confirm не применяется (requires_confirm)",
          applied.get("requires_confirm") is True and not applied["applied"])

    # 6. Уборка
    for tbl in (FACT, DIM):
        dao.execute(f'DROP TABLE IF EXISTS "etl_test"."{tbl}" CASCADE')

    print()
    if _failures:
        print(f"FAILED: {_failures}")
        sys.exit(1)
    print("SYNC-DDL TESTS PASSED ✅")


if __name__ == "__main__":
    main()
