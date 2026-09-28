"""
Оркестратор полной пересборки витрины продаж — ОДНА команда.

Порядок шагов (важен, менять нельзя):
    1. МИГРАЦИИ СТРУКТУРЫ etl_meta  — 001/004/005/006, идемпотентные DDL
    2. ПРОВЕРКА КОНФИГА             — регистр есть в etl_meta?
    3. SYNC                         — создаёт таблицы фактов из мэппингов
                                      (их DDL не в миграциях, только в конфиге)
    4. МИГРАЦИЯ 007                 — dim-слой + FK-колонки; требует шага 3
    5. TRUNCATE ФАКТОВ              — только при --from-scratch, только факты
    6. FULL_PERIOD                  — загрузка из 1С за период
    7. LOAD_DIM_NAMES               — имена справочников (ОБЯЗАТЕЛЬНЫЙ шаг:
                                      без него витрина соберётся со stub-именами)
    8. SALES_RECON --strict         — КРИТЕРИЙ УСПЕХА: закрытые дни сходятся
                                      с 1С в ноль, иначе сборка ПАДАЕТ

Использование:
    cd /home/dev/airflow
    PYTHONPATH=dags python3 -m core.tools.rebuild_sales --start 2026-06-15
    PYTHONPATH=dags python3 -m core.tools.rebuild_sales --start 2026-06-15 --from-scratch
    PYTHONPATH=dags python3 -m core.tools.rebuild_sales --start 2026-06-15 --dry-run

Параметры:
    --start        начало периода, обычная дата YYYY-MM-DD (конвертация в формат 1С внутри)
    --end          конец периода, ИСКЛЮЧИТЕЛЬНО (default: завтра)
    --register     код регистра (default: sales)
    --from-scratch TRUNCATE таблиц ФАКТОВ перед загрузкой. Справочники dim_*
                   НЕ трогаются никогда — их id необратимы (на них ссылается BI).
    --dry-run      показать план шагов и выйти
    --skip-recon   пропустить финальную сверку (НЕ рекомендуется: теряется критерий успеха)

Коды возврата: 0 — сборка прошла и recon в ноль; 1 — любой шаг упал или recon не в ноль.
"""

import argparse
import os
import subprocess
import sys
import time
import warnings
from contextlib import contextmanager
from datetime import date, timedelta
from typing import List, Optional

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))
MIGRATIONS_DIR = os.path.join(REPO_ROOT, "dags", "core", "migrations")
RECON_SCRIPT = os.path.join(REPO_ROOT, "docs", "audits", "sql", "sales_recon.py")

# Идемпотентные DDL-миграции etl_meta. 002/003 НАМЕРЕННО исключены: это
# seed-данные конфига без ON CONFLICT — на живой базе они продублируют
# регистры. Актуальный конфиг восстанавливается из дампа etl_meta.
STRUCTURE_MIGRATIONS = [
    "001_create_etl_meta_schema.sql",
    "004_period_column.sql",
    "005_pipeline_type_and_target_id.sql",
    "006_etl_audit_columns.sql",
]
DIM_MIGRATION = "007_dim_layer.sql"


class StepFailed(Exception):
    pass


# ──────────────────────────────────────────────────────────────────────
# Тихий вывод: Airflow логирует ПОЛНЫЙ текст каждого SQL, движок печатает
# сгенерированные запросы — на rebuild это сотни строк шума, в которых
# теряются шаги. По умолчанию оставляем только прогресс; --verbose вернёт всё.
# ──────────────────────────────────────────────────────────────────────
_NOISY_LOGGERS = (
    "airflow",
    "airflow.hooks.base",
    "airflow.providers.common.sql.hooks.sql",
    "airflow.models.connection",
    "airflow.task",
)

# строки движка, после которых начинается «полотно» SQL
_SQL_DUMP_START = ("Generated SQL:",)
# маркеры полезного прогресса — на них глушилка выключается обратно
_PROGRESS_MARKERS = (
    "MSSQL execute", "Extracted:", "Filtered to", "Dim dedup", "UPSERT:", "INSERT:",
    "DELETE:", "Processing target", "retail_snapshot_at=", "Post-load", "ETL START",
    "ETL DONE", "load_history", "advisory_lock", "validation", "VALIDATION",
    "Full-period retail snapshot", "  ✓", "  ⚠", "  ✗", "=" * 10,
)


class _QuietStdout:
    """Фильтрует полотна SQL, пропуская строки прогресса."""

    def __init__(self, stream):
        self._s = stream
        self._skipping = False

    def write(self, data: str):
        for line in data.splitlines(True):
            stripped = line.strip()
            if not stripped:
                if not self._skipping:
                    self._s.write(line)
                continue
            if any(stripped.startswith(m) for m in _SQL_DUMP_START):
                self._skipping = True
                continue
            if self._skipping:
                if any(m in line for m in _PROGRESS_MARKERS):
                    self._skipping = False
                else:
                    continue
            self._s.write(line)
        self._s.flush()

    def flush(self):
        self._s.flush()

    def __getattr__(self, name):
        return getattr(self._s, name)


@contextmanager
def quiet_output(enabled: bool):
    if not enabled:
        yield
        return
    import logging
    saved = {}
    for name in _NOISY_LOGGERS:
        lg = logging.getLogger(name)
        saved[name] = lg.level
        lg.setLevel(logging.WARNING)
    old_stdout = sys.stdout
    sys.stdout = _QuietStdout(old_stdout)
    try:
        yield
    finally:
        sys.stdout = old_stdout
        import logging as _l
        for name, lvl in saved.items():
            _l.getLogger(name).setLevel(lvl)


@contextmanager
def register_lock(register_code: str, pg_conn_id: str):
    """
    pg_advisory_lock на время очистки и загрузки — тот же ключ (register_id),
    что берёт инкремент в etl_engine._advisory_lock.

    Зачем: full_period сам lock НЕ берёт, а при --from-scratch между TRUNCATE
    и концом загрузки витрина пуста. Влезший в это окно инкрементальный тик
    увидел бы пустой watermark, откатился на 1970 и потянул всю историю retail.
    С локом тик просто упадёт с понятной ошибкой и переедет на следующий цикл
    (retries=1 в DAG), а окно останется целым.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    reg = pg.get_first(
        "SELECT id FROM etl_meta.registers WHERE code = %s", parameters=(register_code,)
    )
    if not reg:
        raise StepFailed(f"регистр '{register_code}' не найден — нечего блокировать")
    register_id = reg[0]

    conn = pg.get_conn()
    cur = conn.cursor()
    cur.execute("SELECT pg_try_advisory_lock(%s)", (register_id,))
    if not cur.fetchone()[0]:
        conn.close()
        raise StepFailed(
            f"не удалось взять advisory_lock({register_id}): регистр уже грузится "
            f"(идёт инкрементальный тик или другой rebuild). Повторите через минуту."
        )
    conn.commit()
    print(f"  ✓ advisory_lock({register_id}) взят — инкремент не влезет в окно")
    try:
        yield
    finally:
        try:
            cur.execute("SELECT pg_advisory_unlock(%s)", (register_id,))
            conn.commit()
            print(f"  ✓ advisory_lock({register_id}) отпущен")
        finally:
            conn.close()


def _log(step: str, msg: str = "") -> None:
    print(f"\n{'=' * 78}\n  {step}{(' — ' + msg) if msg else ''}\n{'=' * 78}", flush=True)


def _to_1c_date(d: str) -> str:
    """2026-06-15 → 4026-06-15 (в 1С год хранится с офсетом +2000)."""
    dt = date.fromisoformat(d)
    return dt.replace(year=dt.year + 2000).isoformat()


# ──────────────────────────────────────────────────────────────────────
# Шаг 1 и 4: миграции
# ──────────────────────────────────────────────────────────────────────
def run_migrations(files: List[str], pg_conn_id: str) -> None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    for fname in files:
        path = os.path.join(MIGRATIONS_DIR, fname)
        if not os.path.exists(path):
            raise StepFailed(f"миграция не найдена: {path}")
        with open(path, encoding="utf-8") as f:
            sql = f.read()
        try:
            pg.run(sql)
            print(f"  ✓ {fname}")
        except Exception as e:
            raise StepFailed(f"{fname}: {str(e)[:300]}")


# ──────────────────────────────────────────────────────────────────────
# Шаг 2: конфиг на месте?
# ──────────────────────────────────────────────────────────────────────
def check_config(register_code: str, pg_conn_id: str) -> List[dict]:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    reg = pg.get_first(
        "SELECT id FROM etl_meta.registers WHERE code = %s AND is_active",
        parameters=(register_code,),
    )
    if not reg:
        raise StepFailed(
            f"регистр '{register_code}' не найден в etl_meta.registers. "
            f"На чистой базе восстановите конфиг из дампа: "
            f"psql -f dags/core/migrations/etl_meta_dump.sql "
            f"(миграция 002 намеренно НЕ применяется — там устаревший конфиг)"
        )
    rows = pg.get_records(
        "SELECT id, target_table, target_role FROM etl_meta.register_targets "
        "WHERE register_id = %s AND is_active ORDER BY priority, id",
        parameters=(reg[0],),
    )
    if not rows:
        raise StepFailed(f"у регистра '{register_code}' нет активных target-ов")
    targets = [{"id": r[0], "table": r[1], "role": r[2]} for r in rows]
    for t in targets:
        print(f"  ✓ target {t['table']} ({t['role']})")
    return targets


# ──────────────────────────────────────────────────────────────────────
# Шаг 3: Sync — создаёт таблицы фактов
# ──────────────────────────────────────────────────────────────────────
def run_sync(targets: List[dict]) -> None:
    app_dir = os.path.join(REPO_ROOT, "etl_config_app")
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)
    import dao  # noqa: E402  — конфигуратор в том же репозитории

    # dimension раньше fact: у fact есть FK-колонка на dim
    for t in sorted(targets, key=lambda x: 0 if x["role"] == "dimension" else 1):
        plan = dao.compute_sync_plan(t["id"])
        actions = plan.get("actions") or []
        if not actions:
            print(f"  ✓ {t['table']}: структура актуальна")
            continue
        destructive = [a for a in actions if a.get("destructive")]
        if destructive:
            raise StepFailed(
                f"{t['table']}: Sync требует деструктивных действий "
                f"({[a['kind'] + ':' + str(a.get('col')) for a in destructive]}). "
                f"Проверьте руками в конфигураторе — автоматически не применяю."
            )
        res = dao.apply_sync_plan(plan, confirm=False)
        if res.get("errors"):
            raise StepFailed(f"{t['table']}: {res['errors']}")
        print(f"  ✓ {t['table']}: применено {len(res.get('executed_actions') or actions)} действий")


# ──────────────────────────────────────────────────────────────────────
# Шаг 5: TRUNCATE — только факты
# ──────────────────────────────────────────────────────────────────────
def truncate_facts(targets: List[dict], pg_conn_id: str) -> None:
    """
    Очищает ТОЛЬКО таблицы фактов регистра.

    Справочники неприкосновенны: их id раздаются один раз и на них ссылается
    BI-слой — пересоздание id сломало бы отчёты безвозвратно. Защита двойная:
    список берётся исключительно из register_targets (dim там не бывает,
    они не являются target-ами регистра) И дополнительно проверяется префикс.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    tables = [t["table"] for t in targets]
    forbidden = [t for t in tables if t.startswith("dim_")]
    if forbidden:
        raise StepFailed(
            f"ОТКАЗ: в списке на TRUNCATE оказались справочники {forbidden}. "
            f"id справочников необратимы — очистка запрещена."
        )
    # fact раньше dim-таргета регистра: FK sales_positions.sales_id
    ordered = [t["table"] for t in sorted(
        targets, key=lambda x: 0 if x["role"] == "fact" else 1)]
    sql = ", ".join(f"public.{t}" for t in ordered)
    pg = PostgresHook(postgres_conn_id=pg_conn_id)
    pg.run(f"TRUNCATE TABLE {sql} RESTART IDENTITY")
    print(f"  ✓ очищено: {sql}  (справочники dim_* не тронуты)")


# ──────────────────────────────────────────────────────────────────────
# Шаг 6: загрузка
# ──────────────────────────────────────────────────────────────────────
def run_full_period(register_code: str, start_1c: str, end_1c: str, pg_conn: str,
                    attempts: int = 3) -> dict:
    """
    full_period с повтором при гонке с живой 1С.

    Движок читает dim и fact ДВУМЯ отдельными запросами к MSSQL (между ними
    десятки секунд). Если в этом окне в 1С проводят новый документ, его позиции
    попадают во второй запрос, а шапка — нет: валидация видит
    `sales_positions.sales_id IS NULL > 0` и роняет загрузку. Это не дефект
    данных, а гонка сборки на работающей базе — повтор её закрывает, потому
    что upsert идемпотентен, а недостающие шапки приедут следующим проходом.
    Другие ошибки валидации повтором НЕ маскируем — пробрасываем сразу.
    """
    from ..etl_engine import ETLEngine

    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        etl = ETLEngine(
            register_code=register_code,
            mode="full_period",
            start_date=start_1c,
            end_date=end_1c,
            config_conn_id=pg_conn,
            dst_conn_id=pg_conn,
        )
        try:
            result = etl.run() or {}
        except RuntimeError as e:
            if "_id IS NULL" not in str(e) or attempt == attempts:
                raise
            last_err = e
            print(
                f"  ⟳ попытка {attempt}/{attempts}: гонка с живой 1С "
                f"(в окне между запросами провели документ) — повторяю"
            )
            time.sleep(5)
            continue

        for tbl, rows in result.items():
            print(f"  ✓ {tbl}: {rows} строк")
        if not result or not any(result.values()):
            raise StepFailed("full_period не загрузил ни одной строки")
        if attempt > 1:
            print(f"  ✓ гонка закрыта повтором (попытка {attempt})")
        return result

    raise StepFailed(f"full_period не прошёл за {attempts} попыток: {last_err}")


# ──────────────────────────────────────────────────────────────────────
# Шаг 7: имена справочников (обязательный)
# ──────────────────────────────────────────────────────────────────────
def run_dim_names(pg_conn_id: str, mssql_conn_id: str) -> None:
    from .load_dim_names import enrich_all

    try:
        totals = enrich_all(
            pg_conn_id=pg_conn_id, mssql_conn_id=mssql_conn_id,
            only_stub=True, raise_on_error=True,
        )
    except RuntimeError as e:
        raise StepFailed(str(e))
    still_stub_total = totals["still_stub"]
    if still_stub_total:
        print(
            f"  ⚠ осталось {still_stub_total} строк без имени — это объекты, "
            f"которых нет в справочниках 1С (удалены/архив). Сборку не валю: "
            f"на суммы не влияет, сигнал качества данных."
        )


# ──────────────────────────────────────────────────────────────────────
# Шаг 8: КРИТЕРИЙ УСПЕХА
# ──────────────────────────────────────────────────────────────────────
def run_recon(start: str, end: str) -> None:
    if not os.path.exists(RECON_SCRIPT):
        raise StepFailed(f"не найден {RECON_SCRIPT}")
    proc = subprocess.run(
        [sys.executable, RECON_SCRIPT, "--start", start, "--end", end, "--strict"],
        capture_output=True, text=True,
    )
    print(proc.stdout)
    if proc.returncode != 0:
        if proc.stderr:
            print(proc.stderr[-1500:], file=sys.stderr)
        raise StepFailed(
            "СВЕРКА НЕ СОШЛАСЬ: закрытые дни расходятся с 1С. "
            "Сборка считается неуспешной — разбирайте расхождение до передачи."
        )


def _refuse_if_switched(pg_conn: str, register: str, from_scratch: bool) -> None:
    """
    Жёсткий отказ до любого шага (миграции, Sync, TRUNCATE, загрузка) — без молчаливого
    обхода. Инструмент пересобирает витрину ФАКТОВ PostgreSQL; в контуре, переключённом
    на прямой путь 1С → ClickHouse, он разрушителен:
      • pg_fact_write = false — факты PostgreSQL заморожены как копия для отката, писать
        в них нельзя; ClickHouse пересобирается generic runner'ом
        (ch_sync --config-conn etl_prod --group onec_1c --mode rebuild --partition YYYYMM);
      • есть реестр etl_meta.doc_key — --from-scratch сделал бы TRUNCATE ... RESTART
        IDENTITY: документы получили бы новые id, расходящиеся с реестром и ClickHouse,
        а таблицы для отката опустели бы раньше, чем защита ETLEngine остановит загрузку.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=pg_conn)
    r = pg.get_first("SELECT coalesce((to_jsonb(r) ->> 'pg_fact_write')::boolean, true) "
                     "FROM etl_meta.registers r WHERE code = %s", parameters=(register,))
    if r and not r[0]:
        raise SystemExit(f"ОТКАЗ: регистр '{register}' в {pg_conn} переключён на прямой путь 1С → ClickHouse "
                         f"(pg_fact_write=false). Факты PostgreSQL заморожены (ROLLBACK_KEEP). "
                         f"Пересборка ClickHouse: ch_sync --config-conn etl_prod --group onec_1c --mode rebuild --partition YYYYMM --apply")
    if from_scratch and pg.get_first("SELECT to_regclass('etl_meta.doc_key') IS NOT NULL")[0]:
        raise SystemExit(f"ОТКАЗ: --from-scratch в {pg_conn} запрещён — в контуре есть реестр etl_meta.doc_key; "
                         f"TRUNCATE ... RESTART IDENTITY перенумеровал бы документы в обход реестра.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Полная пересборка витрины продаж")
    parser.add_argument("--start", required=True, help="начало периода YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="конец периода YYYY-MM-DD, исключительно")
    parser.add_argument("--register", default="sales")
    parser.add_argument("--from-scratch", action="store_true",
                        help="TRUNCATE фактов перед загрузкой (справочники не трогаются)")
    parser.add_argument("--dry-run", action="store_true", help="показать план и выйти")
    parser.add_argument("--skip-recon", action="store_true",
                        help="без финальной сверки (теряется критерий успеха)")
    parser.add_argument("--pg-conn", required=True)
    parser.add_argument("--mssql-conn", default="mssql_1c_conn")
    parser.add_argument("--verbose", action="store_true",
                        help="полный лог Airflow и текст всех SQL (по умолчанию скрыт)")
    args = parser.parse_args()

    warnings.filterwarnings("ignore")
    _refuse_if_switched(args.pg_conn, args.register, args.from_scratch)
    end = args.end or (date.today() + timedelta(days=1)).isoformat()
    start_1c, end_1c = _to_1c_date(args.start), _to_1c_date(end)

    plan = [
        f"1. Миграции структуры etl_meta ({len(STRUCTURE_MIGRATIONS)} шт., идемпотентные)",
        f"2. Проверка конфига регистра '{args.register}'",
        "3. Sync — создание/актуализация таблиц фактов",
        f"4. Миграция {DIM_MIGRATION} — dim-слой + FK",
        ("5. TRUNCATE ФАКТОВ (справочники НЕ трогаются)" if args.from_scratch
         else "5. TRUNCATE — пропуск (нет --from-scratch)"),
        f"6. full_period {start_1c}..{end_1c}",
        "7. Имена справочников (обязательный шаг)",
        ("8. sales_recon --strict — КРИТЕРИЙ УСПЕХА" if not args.skip_recon
         else "8. sales_recon — ПРОПУЩЕН (--skip-recon)"),
    ]
    print("=" * 78)
    print(f"  REBUILD витрины '{args.register}' за [{args.start}, {end})")
    print("=" * 78)
    for p in plan:
        print(f"   {p}")
    if args.dry_run:
        print("\nDRY-RUN: ничего не выполняю.")
        return

    t0 = time.time()
    try:
        with quiet_output(enabled=not args.verbose):
            _log("ШАГ 1/8", "миграции структуры etl_meta")
            run_migrations(STRUCTURE_MIGRATIONS, args.pg_conn)

            _log("ШАГ 2/8", "проверка конфига")
            targets = check_config(args.register, args.pg_conn)

            _log("ШАГ 3/8", "Sync — таблицы фактов")
            run_sync(targets)

            _log("ШАГ 4/8", f"миграция {DIM_MIGRATION}")
            run_migrations([DIM_MIGRATION], args.pg_conn)

            # Шаги 5-6 под advisory_lock: инкремент не должен видеть пустую витрину
            _log("ШАГ 5/8", "очистка фактов" if args.from_scratch else "очистка пропущена")
            with register_lock(args.register, args.pg_conn):
                if args.from_scratch:
                    truncate_facts(targets, args.pg_conn)
                else:
                    print("  — без --from-scratch: догружаем поверх (upsert идемпотентен)")

                _log("ШАГ 6/8", f"full_period {start_1c}..{end_1c}")
                t6 = time.time()
                run_full_period(args.register, start_1c, end_1c, args.pg_conn)
                print(f"  ✓ загрузка заняла {time.time() - t6:.0f} с")

            _log("ШАГ 7/8", "имена справочников")
            run_dim_names(args.pg_conn, args.mssql_conn)

            _log("ШАГ 8/8", "сверка с 1С (критерий успеха)")
            if args.skip_recon:
                print("  ⚠ пропущено по --skip-recon: критерий успеха не проверен")
            else:
                run_recon(args.start, end)

    except StepFailed as e:
        print(f"\n{'!' * 78}\n  СБОРКА ПРОВАЛЕНА: {e}\n{'!' * 78}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n{'!' * 78}\n  СБОРКА УПАЛА: {type(e).__name__}: {str(e)[:400]}\n{'!' * 78}",
              file=sys.stderr)
        sys.exit(1)

    print(f"\n{'=' * 78}")
    print(f"  ✅ СБОРКА УСПЕШНА за {time.time() - t0:.0f} с — витрина сходится с 1С")
    print(f"{'=' * 78}")


if __name__ == "__main__":
    main()
