"""
Первичная заливка справочника номенклатуры из retail: retail.products → public.dim_nomenklatura.

ЗАПУСКАЕТСЯ ВРУЧНУЮ. Это не DAG и не часть инкремента.

Механика:
    INSERT ... ON CONFLICT (guid) DO UPDATE — существующие строки обновляются
    ПО guid с сохранением id, новые получают новые id от IDENTITY.
    id НЕ вставляется (колонка GENERATED ALWAYS AS IDENTITY) и не меняется
    никогда — на него ссылаются 174 тыс. строк фактов sales_positions.

Чего скрипт НЕ делает (осознанно):
    • никаких DELETE / TRUNCATE;
    • не трогает и не дропает констрейнты (на dim_nomenklatura внешних FK
      сейчас нет — связь с фактами логическая, но скрипт корректен и при них:
      он только INSERT/UPDATE, ссылочная целостность не нарушается);
    • не удаляет строки, которых нет в retail (в dim живут объекты из 1С,
      их отсутствие в retail — не повод удалять).

Идемпотентность: повторный прогон не плодит дубли (UNIQUE(guid)) и не
переписывает строки, у которых ничего не изменилось (WHERE в DO UPDATE).

Использование:
    source venv/bin/activate
    PYTHONPATH=dags python3 -m core.tools.load_products_from_retail --dry-run
    PYTHONPATH=dags python3 -m core.tools.load_products_from_retail

Параметры:
    --dry-run     ничего не писать: показать план, объёмы и что изменилось бы
    --batch N     размер батча execute_values (default 1000)
    --limit N     обработать только первые N строк retail (для пробного прогона)
    --skip-migration  не применять 009 (если уже применена вручную)
"""

import argparse
import sys
import time
import warnings
from typing import List, Tuple

MIGRATION = "009_dim_retail_updated_at.sql"

# Только валидные uuid: uid в retail проверен (0 невалидных), но фильтр
# оставляем — данные живые, завтра может появиться мусор.
UUID_RE = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'

SELECT_RETAIL = f"""
    SELECT lower(uid)                                                            AS guid,
           btrim(name)                                                           AS name,
           btrim(code)                                                           AS code,
           (updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Almaty')::timestamp AS retail_updated_at
    FROM   public.products
    WHERE  deleted_at IS NULL
      AND  uid IS NOT NULL
      AND  uid ~ '{UUID_RE}'
    ORDER  BY uid
"""

# id НЕ перечисляем: GENERATED ALWAYS AS IDENTITY выдаст его сам.
# is_stub=false — имя есть, иначе load_dim_names будет вечно пытаться его дотянуть.
# WHERE в DO UPDATE — чтобы повторный прогон не переписывал неизменившиеся строки.
# ВНИМАНИЕ: порядок плейсхолдеров в UPSERT_TEMPLATE должен совпадать с порядком
# колонок здесь. is_stub=false — наша константа, её нет в данных retail, поэтому
# она зашита в template, а не тянется 124к раз по сети.
UPSERT = """
    INSERT INTO public.dim_nomenklatura (guid, name, code, is_stub, retail_updated_at)
    VALUES %s
    ON CONFLICT (guid) DO UPDATE SET
        name              = EXCLUDED.name,
        code              = EXCLUDED.code,
        is_stub           = false,
        retail_updated_at = EXCLUDED.retail_updated_at,
        etl_updated_at    = timezone('Asia/Almaty', now())
    WHERE dim_nomenklatura.name              IS DISTINCT FROM EXCLUDED.name
       OR dim_nomenklatura.code              IS DISTINCT FROM EXCLUDED.code
       OR dim_nomenklatura.is_stub           IS DISTINCT FROM false
       OR dim_nomenklatura.retail_updated_at IS DISTINCT FROM EXCLUDED.retail_updated_at
"""

# (guid, name, code) → из retail; false → is_stub; (retail_updated_at) → из retail
UPSERT_TEMPLATE = "(%s, %s, %s, false, %s)"


def _hooks(pg_conn_id: str, retail_conn_id: str):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return (PostgresHook(postgres_conn_id=pg_conn_id),
            PostgresHook(postgres_conn_id=retail_conn_id))


def apply_migration(pg) -> None:
    import os
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "migrations", MIGRATION
    )
    if not os.path.exists(path):
        raise RuntimeError(f"миграция не найдена: {path}")
    with open(path, encoding="utf-8") as f:
        pg.run(f.read())
    print(f"  ✓ {MIGRATION} применена (идемпотентно)")


def snapshot_ids(pg) -> dict:
    """guid → id существующих строк. Нужен, чтобы ДОКАЗАТЬ неизменность id после заливки."""
    return {g: i for g, i in pg.get_records(
        "SELECT guid::text, id FROM public.dim_nomenklatura")}


def state(pg) -> dict:
    row = pg.get_first("""
        SELECT count(*), count(*) FILTER (WHERE is_stub),
               count(*) FILTER (WHERE retail_updated_at IS NOT NULL),
               max(id)
        FROM public.dim_nomenklatura""")
    orphans = pg.get_first("""
        SELECT count(*) FROM public.sales_positions p
        LEFT JOIN public.dim_nomenklatura d ON d.id = p.nomenklatura_id
        WHERE p.nomenklatura_id IS NOT NULL AND d.id IS NULL""")[0]
    return {"rows": row[0], "stub": row[1], "with_retail_dt": row[2],
            "max_id": row[3], "orphan_facts": orphans}


def main() -> None:
    ap = argparse.ArgumentParser(description="Заливка retail.products → dim_nomenklatura")
    ap.add_argument("--dry-run", action="store_true", help="ничего не писать")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--limit", type=int, default=None, help="только первые N строк retail")
    ap.add_argument("--skip-migration", action="store_true")
    ap.add_argument("--pg-conn", required=True)
    ap.add_argument("--retail-conn", default="bd_retail")
    args = ap.parse_args()
    warnings.filterwarnings("ignore")

    from psycopg2.extras import execute_values
    pg, rt = _hooks(args.pg_conn, args.retail_conn)

    print("=" * 78)
    print(f"  ЗАЛИВКА retail.products → dim_nomenklatura{'  [DRY-RUN]' if args.dry_run else ''}")
    print("=" * 78)

    # 1. Миграция
    if args.skip_migration:
        print("  — миграция пропущена (--skip-migration)")
    elif args.dry_run:
        print(f"  — DRY-RUN: {MIGRATION} НЕ применяется")
    else:
        apply_migration(pg)

    has_col = pg.get_first(
        "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' "
        "AND table_name='dim_nomenklatura' AND column_name='retail_updated_at'")[0]
    if not has_col:
        print("  ✗ колонки retail_updated_at нет — примените миграцию 009 и повторите")
        sys.exit(1)

    # 2. Состояние ДО + снимок id
    before = state(pg)
    ids_before = snapshot_ids(pg)
    print(f"\n  ДО: строк {before['rows']}, stub {before['stub']}, "
          f"с retail-датой {before['with_retail_dt']}, max(id) {before['max_id']}, "
          f"orphan-фактов {before['orphan_facts']}")

    # 3. Чтение retail серверным курсором — не держим 124к строк в памяти
    sql = SELECT_RETAIL + (f" LIMIT {int(args.limit)}" if args.limit else "")
    conn_r = rt.get_conn()
    cur_r = conn_r.cursor(name="products_stream")
    cur_r.itersize = args.batch
    cur_r.execute(sql)

    conn_w = pg.get_conn()
    total = touched = batches = 0
    t0 = time.time()
    try:
        while True:
            rows: List[Tuple] = cur_r.fetchmany(args.batch)
            if not rows:
                break
            total += len(rows)
            batches += 1
            if args.dry_run:
                if batches == 1:
                    print("\n  DRY-RUN, первые 3 строки, которые были бы залиты:")
                    for r in rows[:3]:
                        print(f"    guid={r[0]} | name={str(r[1])[:40]!r} | code={r[2]} | retail_dt={r[3]}")
                    # Прогоняем ОДИН батч по-настоящему и откатываем: без этого
                    # dry-run не проверяет сам UPSERT и пропускает ошибки вида
                    # «INSERT has more target columns than expressions».
                    with conn_w.cursor() as cw:
                        execute_values(cw, UPSERT, rows, template=UPSERT_TEMPLATE,
                                       page_size=args.batch)
                        print(f"  ✓ пробный батч выполнен и ОТКАЧЕН "
                              f"({cw.rowcount} строк прошли бы в таблицу)")
                    conn_w.rollback()
                continue
            with conn_w.cursor() as cw:
                execute_values(cw, UPSERT, rows, template=UPSERT_TEMPLATE, page_size=args.batch)
                touched += cw.rowcount if cw.rowcount and cw.rowcount > 0 else 0
            conn_w.commit()
            if batches % 20 == 0:
                print(f"    … батч {batches}: прочитано {total}, затронуто строк {touched}")
    finally:
        cur_r.close(); conn_r.close()
        if not args.dry_run:
            conn_w.commit()
        conn_w.close()

    print(f"\n  прочитано из retail: {total} | батчей: {batches} | {time.time()-t0:.0f} с")
    if args.dry_run:
        print("\n  DRY-RUN: ничего не записано.")
        return
    print(f"  строк вставлено/обновлено (rowcount): {touched}")

    # 4. Состояние ПОСЛЕ + ГЛАВНАЯ ПРОВЕРКА: id существующих не изменились
    after = state(pg)
    print(f"\n  ПОСЛЕ: строк {after['rows']} (+{after['rows']-before['rows']}), "
          f"stub {after['stub']}, с retail-датой {after['with_retail_dt']}, "
          f"max(id) {after['max_id']}, orphan-фактов {after['orphan_facts']}")

    ids_after = snapshot_ids(pg)
    changed = [g for g, i in ids_before.items() if ids_after.get(g) != i]
    lost = [g for g in ids_before if g not in ids_after]
    print("\n  ── контроль целостности ──")
    print(f"  id изменились у:            {len(changed)}  (должно быть 0)")
    print(f"  guid исчезли из справочника:{len(lost)}  (должно быть 0)")
    print(f"  orphan-факты:               {after['orphan_facts']}  (должно быть 0)")
    ok = not changed and not lost and after["orphan_facts"] == 0
    print(f"\n  {'✅ ЗАЛИВКА КОРРЕКТНА — факты не затронуты' if ok else '❌ ЦЕЛОСТНОСТЬ НАРУШЕНА, разбирайтесь до дальнейшей работы'}")
    if not ok:
        if changed[:5]:
            print(f"     примеры смены id: {changed[:5]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
