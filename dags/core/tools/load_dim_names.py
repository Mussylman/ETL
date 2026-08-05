"""
Загрузчик имён справочников: dim_* ← _Reference* из 1С (MSSQL).

Заполняет name/code у строк, которые stub-резолв (post_load_sql) создал
из фактов, и снимает is_stub. Ключ сопоставления — guid (канон проекта):
    dim.guid  ←→  binary_to_uuid(_Reference._IDRRef)

Гарантии:
  • id НИКОГДА не меняется — UPDATE не трогает колонку id;
  • is_stub=false ставится ТОЛЬКО там, где реально пришло непустое имя;
  • строка, которой нет в справочнике 1С (удалена/архив), остаётся stub —
    это сигнал качества данных, а не ошибка;
  • идемпотентно: повторный запуск даёт тот же результат.

Использование:
    cd /home/dev/airflow
    PYTHONPATH=dags python3 -m core.tools.load_dim_names              # только stub-строки
    PYTHONPATH=dags python3 -m core.tools.load_dim_names --all        # обновить и уже известные имена
    PYTHONPATH=dags python3 -m core.tools.load_dim_names --dim dim_sklad dim_kachestvo
    PYTHONPATH=dags python3 -m core.tools.load_dim_names --dry-run

Параметры:
    --dim      список dim-таблиц (по умолчанию все восемь)
    --all      обрабатывать все строки, не только is_stub (подхватит переименования в 1С)
    --dry-run  ничего не писать, показать что было бы обновлено
    --batch    размер батча uuid в IN-списке MSSQL (default 500)
"""

import argparse
import sys
import warnings
from typing import Dict, List, Optional, Tuple

# Карта: dim-таблица → (русское имя справочника 1С, запасной SQL-номер).
# Номер резолвится через meta API по имени; fallback используется, если API
# недоступен. В другой базе 1С номера отличаются — имя надёжнее.
DIM_SOURCES: Dict[str, Tuple[str, str]] = {
    "dim_nomenklatura":  ("Справочник.Номенклатура",           "_Reference123"),
    "dim_sklad":         ("Справочник.Склады",                 "_Reference169"),
    "dim_kontragent":    ("Справочник.Контрагенты",            "_Reference108"),
    "dim_podrazdelenie": ("Справочник.Подразделения",          "_Reference141"),
    "dim_organizatsiya": ("Справочник.Организации",            "_Reference131"),
    "dim_dogovor":       ("Справочник.ДоговорыКонтрагентов",   "_Reference75"),
    "dim_otvetstvennyy": ("Справочник.Пользователи",           "_Reference145"),
    "dim_kachestvo":     ("Справочник.Качество",               "_Reference97"),
}

META_API = "http://192.168.18.224:8090/NikitaBase/hs/meta"


def _resolve_table(onec_name: str, fallback: str) -> str:
    """Русское имя справочника → физическая таблица MSSQL через meta API 1С."""
    try:
        import requests
        resp = requests.get(f"{META_API}/db_structure/{onec_name}", timeout=10)
        resp.raise_for_status()
        for s in resp.json().get("data", []):
            sql_name = s.get("table_name_sql")
            if sql_name:
                return "_" + sql_name.replace(".", "_")
    except Exception as e:
        print(f"    meta API недоступен ({str(e)[:60]}), беру fallback {fallback}")
    return fallback


def _reference_columns(mssql_hook, table: str) -> List[str]:
    """Какие из _Code/_Description физически есть у справочника."""
    rows = mssql_hook.get_records(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s",
        parameters=(table,),
    )
    have = {r[0] for r in rows}
    return [c for c in ("_Code", "_Description") if c in have]


def load_dim(
    dim_table: str,
    onec_name: str,
    fallback_table: str,
    pg_conn_id: str = "postgre_test_base",
    mssql_conn_id: str = "mssql_1c_conn",
    only_stub: bool = True,
    batch: int = 500,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Обогащает один справочник. Возвращает счётчики."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    from ..transform.binary import binary_to_uuid, uuid_to_mssql_hex_1c

    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    ms = MsSqlHook(mssql_conn_id=mssql_conn_id)

    where = "WHERE is_stub" if only_stub else ""
    guids = [r[0] for r in pg.get_records(
        f"SELECT guid::text FROM public.{dim_table} {where}"
    )]
    if not guids:
        print(f"  {dim_table}: нечего обогащать")
        return {"candidates": 0, "found": 0, "updated": 0, "still_stub": 0}

    table = _resolve_table(onec_name, fallback_table)
    cols = _reference_columns(ms, table)
    if "_Description" not in cols:
        print(f"  {dim_table}: у {table} нет _Description — пропускаю")
        return {"candidates": len(guids), "found": 0, "updated": 0, "still_stub": len(guids)}

    code_expr = "_Code" if "_Code" in cols else "NULL"
    found: List[Tuple[str, Optional[str], Optional[str]]] = []

    for i in range(0, len(guids), batch):
        hexes = [uuid_to_mssql_hex_1c(g) for g in guids[i:i + batch]]
        hexes = [h for h in hexes if h]
        if not hexes:
            continue
        rows = ms.get_records(
            f"SELECT _IDRRef, {code_expr}, _Description "
            f"FROM dbo.{table} WITH (NOLOCK) WHERE _IDRRef IN ({','.join(hexes)})"
        )
        for rref, code, descr in rows:
            guid = binary_to_uuid(bytes(rref))
            if guid is None:
                continue
            found.append((
                str(guid),
                (code or "").strip() or None,
                (descr or "").strip() or None,
            ))

    # is_stub снимаем ТОЛЬКО там, где реально есть имя
    named = [(g, c, n) for (g, c, n) in found if n]
    updated = 0
    if named and not dry_run:
        from psycopg2.extras import execute_values
        conn = pg.get_conn()
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    UPDATE public.{dim_table} d
                    SET    name = v.name,
                           code = v.code,
                           is_stub = false,
                           etl_updated_at = timezone('Asia/Almaty', now())
                    FROM (VALUES %s) AS v(guid, code, name)
                    WHERE  d.guid = v.guid::uuid
                      AND  (d.name IS DISTINCT FROM v.name
                            OR d.code IS DISTINCT FROM v.code
                            OR d.is_stub)
                    """,
                    named,
                    page_size=1000,
                )
                updated = cur.rowcount
            conn.commit()
        finally:
            conn.close()
    elif named and dry_run:
        updated = len(named)

    still_stub = pg.get_first(
        f"SELECT count(*) FROM public.{dim_table} WHERE is_stub"
    )[0]
    print(
        f"  {dim_table:20s} ← {table:14s} | кандидатов {len(guids):6d} | "
        f"найдено {len(found):6d} | с именем {len(named):6d} | "
        f"{'БЫЛО БЫ ' if dry_run else ''}обновлено {updated:6d} | осталось stub {still_stub}"
    )
    return {
        "candidates": len(guids),
        "found": len(found),
        "updated": updated,
        "still_stub": still_stub,
    }


def enrich_all(
    pg_conn_id: str = "postgre_test_base",
    mssql_conn_id: str = "mssql_1c_conn",
    only_stub: bool = True,
    dims: Optional[List[str]] = None,
    raise_on_error: bool = True,
) -> Dict[str, int]:
    """
    Обогатить все справочники — общая точка входа для DAG, CLI и оркестратора.

    Дешёвая при пустой работе: если stub-строк нет, load_dim выходит ДО похода
    в meta API и MSSQL (только 8 быстрых SELECT). Поэтому вызов безопасно
    вешать на каждый инкрементальный тик.

    raise_on_error=False — режим «не мешать основному потоку»: ошибки
    логируются и возвращаются в счётчике `failed`, исключение не бросается
    (для DAG, где загрузка фактов уже завершена и не должна страдать).
    """
    totals = {"candidates": 0, "found": 0, "updated": 0, "still_stub": 0, "failed": 0}
    failed: List[str] = []
    for dim_table in (dims or list(DIM_SOURCES)):
        onec_name, fallback = DIM_SOURCES[dim_table]
        try:
            res = load_dim(
                dim_table=dim_table,
                onec_name=onec_name,
                fallback_table=fallback,
                pg_conn_id=pg_conn_id,
                mssql_conn_id=mssql_conn_id,
                only_stub=only_stub,
            )
            for k in ("candidates", "found", "updated", "still_stub"):
                totals[k] += res[k]
        except Exception as e:
            print(f"  ✗ {dim_table}: {str(e)[:200]}")
            failed.append(dim_table)
            totals["failed"] += 1

    if failed and raise_on_error:
        raise RuntimeError(f"обогащение имён не прошло для {failed}")
    return totals


def main():
    parser = argparse.ArgumentParser(description="Загрузка имён справочников из 1С")
    parser.add_argument("--dim", nargs="*", default=None, help="какие dim (default: все)")
    parser.add_argument("--all", action="store_true", help="обновлять и не-stub строки")
    parser.add_argument("--dry-run", action="store_true", help="ничего не писать")
    parser.add_argument("--batch", type=int, default=500, help="размер IN-батча (default 500)")
    parser.add_argument("--pg-conn", default="postgre_test_base")
    parser.add_argument("--mssql-conn", default="mssql_1c_conn")
    args = parser.parse_args()

    warnings.filterwarnings("ignore")

    targets = args.dim or list(DIM_SOURCES)
    unknown = [t for t in targets if t not in DIM_SOURCES]
    if unknown:
        print(f"Неизвестные dim: {unknown}. Доступны: {list(DIM_SOURCES)}")
        sys.exit(2)

    mode = "все строки" if args.all else "только stub"
    print("=" * 78)
    print(f"  ЗАГРУЗКА ИМЁН СПРАВОЧНИКОВ ({mode}{', DRY-RUN' if args.dry_run else ''})")
    print("=" * 78)

    total = {"candidates": 0, "found": 0, "updated": 0, "still_stub": 0}
    failed: List[str] = []
    for dim_table in targets:
        onec_name, fallback = DIM_SOURCES[dim_table]
        try:
            res = load_dim(
                dim_table=dim_table,
                onec_name=onec_name,
                fallback_table=fallback,
                pg_conn_id=args.pg_conn,
                mssql_conn_id=args.mssql_conn,
                only_stub=not args.all,
                batch=args.batch,
                dry_run=args.dry_run,
            )
            for k in ("candidates", "found", "updated"):
                total[k] += res[k]
            total["still_stub"] += res["still_stub"]
        except Exception as e:
            print(f"  {dim_table}: ОШИБКА — {str(e)[:160]}")
            failed.append(dim_table)

    print("-" * 78)
    print(
        f"  ИТОГО: кандидатов {total['candidates']}, найдено {total['found']}, "
        f"обновлено {total['updated']}, осталось stub {total['still_stub']}"
    )
    if failed:
        print(f"  С ОШИБКОЙ: {failed}")
        sys.exit(1)
    print("=" * 78)


if __name__ == "__main__":
    main()
