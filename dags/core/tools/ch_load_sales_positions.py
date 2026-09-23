"""
Историческая загрузка sales_positions из PostgreSQL в ClickHouse помесячно.

Архитектура намеренно односторонняя: ClickHouse НЕ ходит в PostgreSQL.
Табличная функция postgresql() не используется, привилегия POSTGRES не выдаётся,
креды PostgreSQL в ClickHouse не попадают. Данные тянет этот загрузчик:
читает серверным курсором и отдаёт clickhouse-client'у через stdin.

Состояние загрузки живёт в СУЩЕСТВУЮЩЕМ control layer — etl_meta.load_history
(register_id=62 sales, target_id=81 sales_positions, run_mode='clickhouse_month').
Второго источника правды о состоянии ETL в ClickHouse не заводим.

Идемпотентность и атомарность одного месяца:
    1. TRUNCATE staging
    2. потоковая вставка месяца в staging
    3. сверка staging с PostgreSQL ДО касания целевой таблицы
    4. ALTER TABLE ... REPLACE PARTITION — одна атомарная операция
    5. контроль целевой партиции
    6. TRUNCATE staging
Повторный запуск месяца заменяет партицию целиком, поэтому дублей не бывает,
а «переехавший» между месяцами документ чинится перезаливкой обоих месяцев.

Почему TSV без экранирования: в целевой таблице нет ни одной String-колонки —
только DateTime, UInt*, Decimal и UUID. Табуляций и переводов строки в данных
появиться не может.

Использование:
    PYTHONPATH=dags python -m core.tools.ch_load_sales_positions --month 2026-08 --plan
    PYTHONPATH=dags python -m core.tools.ch_load_sales_positions --month 2026-08 --apply
    ... --from 2012-03 --to 2026-08 --apply    диапазон месяцев
    ... --batch 100000                          строк в одной порции stdin
"""

import argparse
import subprocess
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Iterator, List, Optional, Tuple

CH_DB = "analytics_poc"
FACT = f"{CH_DB}.fact_sales_positions"
STAGE = f"{CH_DB}.fact_sales_positions_stage"

REGISTER_ID = 62      # etl_meta.registers: sales
TARGET_ID = 81        # etl_meta.register_targets: sales_positions
RUN_MODE = "clickhouse_month"

BATCH_DEFAULT = 100_000

# Пороги останова. Диск и память — чужая зона ответственности, поэтому загрузка
# не расширяет ничего сама, а просто отказывается начинать очередной месяц.
MIN_FREE_GB_DEFAULT = 5.0
MIN_RAM_GB_DEFAULT = 2.0

# Порядок колонок один и тот же в SELECT, в TSV и в INSERT. Менять только синхронно.
COLUMNS = [
    "period",
    "podrazdelenie_id", "sklad_id", "kontragent_id", "otvetstvennyy_id", "dogovor_id", "zakaz_id",
    "nomenklatura_id", "line_sklad_id", "line_kachestvo_id",
    "recorder", "recorder_type", "line_no",
    "kolichestvo", "stoimost", "stoimost_bez_skidok", "nds", "akciz",
    "summa", "tsena", "evrika_bonusy", "evrika_spisannye", "summands",
    "etl_updated_at",
]

# COALESCE(...,0) там и только там, где источник допускает NULL, а в ClickHouse
# колонка не-Nullable. Фактические доли NULL измерены на PROD:
#   s.podrazdelenie_id 0.0% | s.sklad_id 2.0% | s.kontragent_id 0.0% | s.otvetstvennyy_id 47.0%
#   s.dogovor_id 0.0% | s.zakaz_id 94.8% | p.nomenklatura_id 0.0% | p.sklad_id 49.0% | p.kachestvo_id 49.0%
# recorder / recorder_type / line_no / etl_updated_at / period — NULL'ов нет ни одного,
# но WHERE ниже всё равно отсекает строки без ключа: UUID нечем заменить.
SELECT_SQL = """
SELECT s.period,
       coalesce(s.podrazdelenie_id, 0)   AS podrazdelenie_id,
       coalesce(s.sklad_id, 0)           AS sklad_id,
       coalesce(s.kontragent_id, 0)      AS kontragent_id,
       coalesce(s.otvetstvennyy_id, 0)   AS otvetstvennyy_id,
       coalesce(s.dogovor_id, 0)         AS dogovor_id,
       coalesce(s.zakaz_id, 0)           AS zakaz_id,
       coalesce(p.nomenklatura_id, 0)    AS nomenklatura_id,
       coalesce(p.sklad_id, 0)           AS line_sklad_id,
       coalesce(p.kachestvo_id, 0)       AS line_kachestvo_id,
       p.recorder, p.recorder_type, p.line_no,
       coalesce(p.kolichestvo, 0)         AS kolichestvo,
       coalesce(p.stoimost, 0)            AS stoimost,
       coalesce(p.stoimost_bez_skidok, 0) AS stoimost_bez_skidok,
       coalesce(p.nds, 0)                 AS nds,
       coalesce(p.akciz, 0)               AS akciz,
       coalesce(p.summa, 0)               AS summa,
       coalesce(p.tsena, 0)               AS tsena,
       coalesce(p.evrika_bonusy, 0)       AS evrika_bonusy,
       coalesce(p.evrika_spisannye, 0)    AS evrika_spisannye,
       coalesce(p.summands, 0)            AS summands,
       p.etl_updated_at
  FROM public.sales_positions p
  JOIN public.sales s ON s.id = p.sales_id
 WHERE s.period >= %(m0)s AND s.period < %(m1)s
   AND p.recorder IS NOT NULL AND p.recorder_type IS NOT NULL
   AND p.line_no IS NOT NULL AND p.etl_updated_at IS NOT NULL
"""

# Метрики сверки. Порядок в PG и в ClickHouse один и тот же — сравнение позиционное,
# поэтому два запроса ниже правятся только вместе с этим списком.
METRICS = ["строк", "документов (recorder,recorder_type)", "SUM(kolichestvo)", "SUM(stoimost)",
           "SUM(stoimost_bez_skidok)", "SUM(nds)", "SUM(akciz)", "MIN(period)", "MAX(period)"]

VERIFY_PG_SQL = """
SELECT count(*), count(DISTINCT (p.recorder, p.recorder_type)),
       coalesce(sum(p.kolichestvo),0), coalesce(sum(p.stoimost),0),
       coalesce(sum(p.stoimost_bez_skidok),0), coalesce(sum(p.nds),0),
       coalesce(sum(p.akciz),0),
       min(s.period), max(s.period)
  FROM public.sales_positions p
  JOIN public.sales s ON s.id = p.sales_id
 WHERE s.period >= %(m0)s AND s.period < %(m1)s
   AND p.recorder IS NOT NULL AND p.recorder_type IS NOT NULL
   AND p.line_no IS NOT NULL AND p.etl_updated_at IS NOT NULL
"""

VERIFY_CH_SQL = """
SELECT count(), uniqExact(recorder, recorder_type),
       sum(kolichestvo), sum(stoimost), sum(stoimost_bez_skidok), sum(nds), sum(akciz),
       min(period), max(period)
  FROM {table}{where}
"""

DUP_CH_SQL = """
SELECT count() FROM (SELECT recorder, recorder_type, line_no
  FROM {table}{where} GROUP BY 1,2,3 HAVING count() > 1)
"""

# Измерено на партиции 202608: 2.17 MiB сжатых на 76 194 строки. Нужно только для
# оценки объёма в --plan, на саму загрузку не влияет.
BYTES_PER_ROW = 29.9


def month_bounds(m: str) -> Tuple[date, date, int]:
    """'2026-08' → (2026-08-01, 2026-09-01, 202608)."""
    y, mo = (int(x) for x in m.split("-"))
    m0 = date(y, mo, 1)
    m1 = date(y + (mo == 12), 1 if mo == 12 else mo + 1, 1)
    return m0, m1, y * 100 + mo


def months_range(a: str, b: str) -> List[str]:
    y0, m0 = (int(x) for x in a.split("-"))
    y1, m1 = (int(x) for x in b.split("-"))
    out, y, m = [], y0, m0
    while (y, m) <= (y1, m1):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


class ClickHouse:
    """Тонкая обёртка над clickhouse-client: без драйверов и без лишних зависимостей."""

    def __init__(self, config_file: str):
        self.base = ["clickhouse-client", "--config-file", config_file]

    def query(self, sql: str, fmt: str = "TSV") -> str:
        r = subprocess.run(self.base + ["--query", f"{sql} FORMAT {fmt}"],
                           capture_output=True, text=True)
        if r.returncode:
            raise RuntimeError(f"ClickHouse: {r.stderr.strip()[:400]}")
        return r.stdout.strip()

    def execute(self, sql: str) -> None:
        r = subprocess.run(self.base + ["--query", sql], capture_output=True, text=True)
        if r.returncode:
            raise RuntimeError(f"ClickHouse: {r.stderr.strip()[:400]}")

    def insert_tsv(self, table: str, cols: List[str], lines: Iterator[str]) -> None:
        sql = f"INSERT INTO {table} ({', '.join(cols)}) FORMAT TSV"
        p = subprocess.Popen(self.base + ["--query", sql],
                             stdin=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            for line in lines:
                p.stdin.write(line)
        finally:
            p.stdin.close()
        if p.wait():
            raise RuntimeError(f"ClickHouse INSERT: {p.stderr.read().strip()[:400]}")


def fmt_value(v) -> str:
    """Значение → TSV-поле. String-колонок в таблице нет, экранировать нечего."""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, Decimal):
        return format(v, "f")
    return str(v)


def norm(i: int, v) -> str:
    """Метрика → канонический вид. PG отдаёт Decimal и datetime, ClickHouse — строки TSV."""
    if i >= 7:                                    # MIN/MAX period
        return str(v).replace("T", " ")[:19]
    if i < 2:                                     # счётчики
        return str(int(v))
    d = Decimal(str(v)).normalize()               # 0E-8, 0.0000 и 0 — одно и то же число
    return format(abs(d) if d == 0 else d, "f")


def diff_metrics(pg_row, ch_row) -> List[Tuple[str, str, str]]:
    """Расхождения PG против ClickHouse. Пустой список = Δ ровно 0 по всем метрикам."""
    out = []
    for i, name in enumerate(METRICS):
        a, b = norm(i, pg_row[i]), norm(i, ch_row[i])
        if a != b:
            out.append((name, a, b))
    return out


def last_checkpoint(pg, part: int) -> str:
    """Последняя запись о месяце в etl_meta.load_history. Своего состояния в ClickHouse нет."""
    r = pg.get_first("""SELECT status, to_char(finished_at,'MM-DD HH24:MI')
              FROM etl_meta.load_history
             WHERE run_mode = %s AND checkpoint_value LIKE %s
             ORDER BY id DESC LIMIT 1""", parameters=(RUN_MODE, f"partition={part};%"))
    return f"{r[0]} {r[1]}" if r else "—"


def resource_guard(min_free_gb: float, min_ram_gb: float) -> Optional[str]:
    """Проверка перед каждым месяцем. Диск и память — чужая зона, поэтому только читаем."""
    import shutil
    try:
        free_gb = shutil.disk_usage("/var/lib/clickhouse").free / 2**30
    except OSError as e:
        return f"не удалось прочитать свободное место: {e}"
    if free_gb < min_free_gb:
        return f"на диске ClickHouse свободно {free_gb:.1f} GB, порог {min_free_gb} GB"
    try:
        meminfo = dict(l.split(":", 1) for l in open("/proc/meminfo"))
        avail_gb = int(meminfo["MemAvailable"].split()[0]) / 2**20
    except (OSError, KeyError, ValueError):
        return None                                # нет метрики — не повод останавливать загрузку
    if avail_gb < min_ram_gb:
        return f"доступно RAM {avail_gb:.1f} GB, порог {min_ram_gb} GB"
    return None


def load_month(pg, ch: ClickHouse, month: str, batch: int, apply: bool) -> dict:
    import psycopg2.extras  # noqa: F401  (нужен для именованного курсора)

    m0, m1, part = month_bounds(month)
    params = {"m0": m0, "m1": m1}

    exp = pg.get_first(VERIFY_PG_SQL, parameters=params)
    rows_pg, docs_pg = int(exp[0]), int(exp[1])
    res = {"month": month, "partition": part, "rows_pg": rows_pg, "docs_pg": docs_pg,
           "pg": list(exp), "rows_ch": 0, "docs_ch": 0, "status": "plan"}
    if not apply:
        cur = ch.query(VERIFY_CH_SQL.format(
            table=FACT, where=f" WHERE toYYYYMM(period) = {part}")).split("\t")
        res["rows_ch"], res["docs_ch"] = int(cur[0]), int(cur[1])
        res["checkpoint"] = last_checkpoint(pg, part)
        return res
    if rows_pg == 0:
        # Пустой месяц: заменять партицию нечем. Если она есть в цели — это расхождение,
        # о нём сообщаем, но молча ничего не удаляем.
        res["status"] = "empty"
        in_ch = int(ch.query(f"SELECT count() FROM {FACT} WHERE toYYYYMM(period) = {part}") or 0)
        res["rows_ch"] = in_ch
        res["note"] = ("в PostgreSQL нет строк за месяц" if in_ch == 0
                       else f"в PostgreSQL строк нет, а в ClickHouse {in_ch} — партиция не тронута")
        return res

    t0 = time.monotonic()
    ch.execute(f"TRUNCATE TABLE {STAGE}")

    conn = pg.get_conn()
    try:
        cur = conn.cursor(name=f"ch_load_{part}")   # серверный курсор: месяц не тянется в память
        cur.itersize = batch
        cur.execute(SELECT_SQL, params)

        def lines():
            while True:
                chunk = cur.fetchmany(batch)
                if not chunk:
                    break
                for row in chunk:
                    yield "\t".join(fmt_value(v) for v in row) + "\n"

        ch.insert_tsv(STAGE, COLUMNS, lines())
        cur.close()
    finally:
        conn.close()
    t_stream = time.monotonic() - t0   # чтение и вставка — один потоковый этап, их не разделить

    # Полная сверка staging с PostgreSQL ДО касания целевой таблицы: все метрики сразу,
    # иначе расхождение в суммах при совпавшем числе строк прошло бы незамеченным.
    t1 = time.monotonic()
    got = ch.query(VERIFY_CH_SQL.format(table=STAGE, where="")).split("\t")
    res["rows_ch"], res["docs_ch"] = int(got[0]), int(got[1])
    res["ch"] = got

    dup = int(ch.query(DUP_CH_SQL.format(table=STAGE, where="")))
    # Контроль «в staging ровно один месяц» — по самим данным, а не по system.parts:
    # у etl_writer доступа к системным таблицам нет и выдавать его незачем.
    parts = int(ch.query(f"SELECT uniqExact(toYYYYMM(period)) FROM {STAGE}") or 0)

    res["diff"] = diff_metrics(exp, got)
    res["dup"] = dup
    problems = [f"{n}: PG {a} против CH {b}" for n, a, b in res["diff"]]
    if dup:
        problems.append(f"дублей business key {dup}")
    if parts != 1:
        problems.append(f"месяцев в staging {parts}, ожидался 1")
    t_verify = time.monotonic() - t1
    res["t_stream"], res["t_verify"] = round(t_stream, 2), round(t_verify, 2)
    if problems:
        res["status"] = "failed"
        res["note"] = "; ".join(problems)
        return res

    # Целевая таблица меняется ровно здесь и одной атомарной операцией.
    #
    # Две ветки — из-за привилегий, а не из-за удобства. ClickHouse требует:
    #   REPLACE PARTITION        → INSERT + ALTER DELETE на целевой таблице
    #   MOVE PARTITION TO TABLE  → ALTER MOVE PARTITION
    # Когда целевая партиция пуста, обе дают одинаковый результат, поэтому берём MOVE:
    # он обходится более узким правом. Когда партиция не пуста (перезаливка месяца),
    # MOVE добавил бы данные к существующим и дал дубли — там обязателен REPLACE,
    # а значит и ALTER DELETE.
    t2 = time.monotonic()
    existing = int(ch.query(f"SELECT count() FROM {FACT} WHERE toYYYYMM(period) = {part}") or 0)
    if existing == 0:
        ch.execute(f"ALTER TABLE {STAGE} MOVE PARTITION {part} TO TABLE {FACT}")
        res["method"] = "MOVE PARTITION TO TABLE"
    else:
        ch.execute(f"ALTER TABLE {FACT} REPLACE PARTITION {part} FROM {STAGE}")
        res["method"] = "REPLACE PARTITION"
    where = f" WHERE toYYYYMM(period) = {part}"
    after = ch.query(VERIFY_CH_SQL.format(table=FACT, where=where)).split("\t")
    dup_after = int(ch.query(DUP_CH_SQL.format(table=FACT, where=where)))
    res["diff_after"], res["dup_after"] = diff_metrics(exp, after), dup_after
    if res["diff_after"] or dup_after:
        res["status"] = "failed"
        res["note"] = "после замены — " + "; ".join(
            [f"{n}: PG {a} против CH {b}" for n, a, b in res["diff_after"]]
            + ([f"дублей business key {dup_after}"] if dup_after else []))
        return res

    res["t_replace"] = round(time.monotonic() - t2, 2)
    ch.execute(f"TRUNCATE TABLE {STAGE}")
    res["t_total"] = round(time.monotonic() - t0, 2)
    res["status"] = "replaced"
    return res


def history(pg, res: dict, started: datetime) -> None:
    """Чекпоинт в существующий control layer. Отдельного состояния в ClickHouse нет."""
    cp = (f"partition={res['partition']}; rows_pg={res['rows_pg']}; rows_ch={res['rows_ch']}; "
          f"docs_pg={res['docs_pg']}; docs_ch={res['docs_ch']}; stoimost_pg={res['pg'][3]}")
    pg.run("""INSERT INTO etl_meta.load_history
              (register_id, target_id, run_mode, status, started_at, finished_at,
               rows_extracted, rows_loaded, checkpoint_value, error_message)
              VALUES (%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s)""",
           parameters=(REGISTER_ID, TARGET_ID, RUN_MODE,
                       "success" if res["status"] in ("replaced", "empty") else "failed",
                       started, res["rows_pg"], res["rows_ch"], cp, res.get("note")))


def plan(pg, ch: ClickHouse, months: List[str]) -> int:
    """Что будет загружено. PostgreSQL и ClickHouse только читаются."""
    print(f"{'месяц':<9} {'партиция':>9} {'строк PG':>13} {'строк CH':>13} {'док PG':>12}  чекпоинт")
    print("-" * 80)
    total_pg = total_ch = total_docs = 0
    nonempty = 0
    for m in months:
        res = load_month(pg, ch, m, 0, False)
        total_pg += res["rows_pg"]
        total_ch += res["rows_ch"]
        total_docs += res["docs_pg"]
        nonempty += res["rows_pg"] > 0
        print(f"{m:<9} {res['partition']:>9} {res['rows_pg']:>13,} {res['rows_ch']:>13,} "
              f"{res['docs_pg']:>12,}  {res['checkpoint']}")
    print("-" * 80)
    print(f"{'ИТОГО':<9} {'':>9} {total_pg:>13,} {total_ch:>13,} {total_docs:>12,}")
    print(f"\nмесяцев всего {len(months)}, из них с данными {nonempty}, пустых {len(months) - nonempty}")
    print(f"ожидаемое число строк: {total_pg:,}")
    est = total_pg * BYTES_PER_ROW
    print(f"оценка объёма ClickHouse: {est / 2**30:.2f} GiB сжатых "
          f"(по {BYTES_PER_ROW} байт/строку, замер на партиции 202608)")
    import shutil
    free = shutil.disk_usage("/var/lib/clickhouse").free
    print(f"свободно на диске ClickHouse: {free / 2**30:.1f} GiB "
          f"→ {'хватает' if free > est * 2 else 'ЗАПАС МЕНЬШЕ ДВУКРАТНОГО — проверить вручную'}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="PostgreSQL → ClickHouse: sales_positions помесячно")
    ap.add_argument("--month", help="один месяц, YYYY-MM")
    ap.add_argument("--from", dest="m_from", help="начало диапазона, YYYY-MM")
    ap.add_argument("--to", dest="m_to", help="конец диапазона включительно, YYYY-MM")
    ap.add_argument("--conn", default="etl_prod", help="Airflow conn_id PostgreSQL")
    ap.add_argument("--ch-config", required=True, help="config-file clickhouse-client (etl_writer)")
    ap.add_argument("--batch", type=int, default=BATCH_DEFAULT)
    ap.add_argument("--min-free-gb", type=float, default=MIN_FREE_GB_DEFAULT,
                    help="остановиться, если на диске ClickHouse свободно меньше")
    ap.add_argument("--min-ram-gb", type=float, default=MIN_RAM_GB_DEFAULT,
                    help="остановиться, если доступной RAM меньше")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true", help="только показать, что будет загружено")
    mode.add_argument("--apply", action="store_true", help="загрузить и заменить партиции")
    args = ap.parse_args()

    if args.month:
        months = [args.month]
    elif args.m_from and args.m_to:
        months = months_range(args.m_from, args.m_to)
    else:
        ap.error("нужен --month либо пара --from/--to")

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg = PostgresHook(postgres_conn_id=args.conn)
    ch = ClickHouse(args.ch_config)

    print(f"месяцев: {len(months)} | режим: {'apply' if args.apply else 'plan'} | батч: {args.batch:,}")
    if args.plan:
        return plan(pg, ch, months)

    print(f"{'месяц':<9} {'партиция':>9} {'строк PG':>12} {'строк CH':>12} {'док PG':>10} {'док CH':>10}  статус")
    print("-" * 82)
    # Месяцы идут строго последовательно, и любое отклонение останавливает весь прогон:
    # частично загруженная история хуже, чем незагруженная — её незаметно примут за полную.
    done: List[dict] = []
    stop: Optional[str] = None
    for m in months:
        guard = resource_guard(args.min_free_gb, args.min_ram_gb)
        if guard:
            stop = f"{m}: ресурсы — {guard}"
            break
        started = datetime.now()
        try:
            res = load_month(pg, ch, m, args.batch, True)
        except Exception as e:
            stop = f"{m}: ошибка — {str(e)[:200]}"
            break
        print(f"{m:<9} {res['partition']:>9} {res['rows_pg']:>12,} {res['rows_ch']:>12,} "
              f"{res['docs_pg']:>10,} {res['docs_ch']:>10,}  {res['status']}"
              + (f" — {res['note']}" if res.get("note") else ""))
        if res.get("t_total"):
            print(f"{'':<9} тайминг: поток(чтение+вставка) {res['t_stream']}s | сверка {res['t_verify']}s"
                  f" | {res.get('method','замена')} {res['t_replace']}s | всего {res['t_total']}s")
        history(pg, res, started)
        done.append(res)
        if res["status"] == "failed":
            stop = f"{m}: сверка не сошлась — {res.get('note')}"
            break

    print("-" * 82)
    ok = [r for r in done if r["status"] in ("replaced", "empty")]
    loaded = sum(r["rows_pg"] for r in ok)
    print(f"месяцев обработано {len(done)} из {len(months)}, строк загружено {loaded:,}")
    if stop:
        print(f"\nОСТАНОВ: {stop}")
        print("Следующие месяцы не загружались. Разберитесь с причиной и запустите остаток "
              "диапазона заново — уже загруженные месяцы перезальются идемпотентно.")
        return 1
    timed = [r for r in ok if r.get("t_total")]
    if timed:
        slowest = max(timed, key=lambda r: r["t_total"])
        print(f"суммарно {sum(r['t_total'] for r in timed):.1f}s | "
              f"в среднем {sum(r['t_total'] for r in timed) / len(timed):.1f}s на месяц | "
              f"медленнее всех {slowest['month']} {slowest['t_total']}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
