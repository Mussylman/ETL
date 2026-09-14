"""
historical_load_prod — историческая загрузка PROD (etl_prod) по месяцам, ручной запуск.

Зачем отдельный DAG, а не скрипт: 168 месяцев × ~4 мин — это ~7 часов; нужны retry на
deadlock/обрыв сети (несколько прогонов в неделю — норма, см. docs/knowledge/debugging),
видимый прогресс по месяцам, остановка цепочки на первой нерешаемой ошибке и сверка в конце.

Что делает каждая таска месяца: ETLEngine(mode=full_period) за [1-е число, 1-е число следующего)
в датах 1С (+2000 к году). Upsert идемпотентен — месяц можно перезапускать. Перед месяцем
проверяется свободное место на диске БД (она на этом же хосте): ниже MIN_FREE_GB — стоп.

Порядок: сначала sales 2012-03 → 2026-03 (март 2026 и дальше уже в PROD). Заказы — отдельной
цепочкой после решения по диску (см. лог сессии 2026-09-14). incremental_prod не останавливаем:
исторические месяцы не пересекаются по документам с инкрементом, конфликты в общих dim_* гасит retry.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DAGS_PATH = "/home/dev/airflow/dags"
CONFIG_CONN_ID = "etl_prod"
PG_DATA_DIR = "/var/lib/postgresql"   # БД PROD живёт на этом же сервере
MIN_FREE_GB = 12                      # ниже — цепочка останавливается, ничего не грузим

# register → (первый месяц включительно, последний месяц исключительно), формат YYYY-MM
CHAINS = {
    "sales": ("2012-03", "2026-03"),
}


def _months(start: str, end: str):
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    while (y, m) < (ey, em):
        ny, nm = (y + (m // 12), (m % 12) + 1)
        yield f"{y:04d}-{m:02d}-01", f"{ny:04d}-{nm:02d}-01"
        y, m = ny, nm


def _c1(d: str) -> str:
    """2026-03-01 → 4026-03-01 (год в 1С хранится с офсетом +2000)."""
    y, rest = d.split("-", 1)
    return f"{int(y) + 2000}-{rest}"


def _load_month(register: str, start: str, end: str):
    import shutil
    import sys
    if DAGS_PATH not in sys.path:
        sys.path.insert(0, DAGS_PATH)
    free_gb = shutil.disk_usage(PG_DATA_DIR).free / 2 ** 30
    print(f"свободно на диске БД: {free_gb:.1f} ГБ (порог {MIN_FREE_GB})")
    if free_gb < MIN_FREE_GB:
        raise RuntimeError(f"на диске БД свободно {free_gb:.1f} ГБ < {MIN_FREE_GB} — историческая загрузка остановлена")
    from core.etl_engine import ETLEngine
    res = ETLEngine(
        register_code=register, mode="full_period",
        config_conn_id=CONFIG_CONN_ID, dst_conn_id=CONFIG_CONN_ID,
        start_date=_c1(start), end_date=_c1(end),
    ).run()
    print(f"{register} {start}..{end} DONE: {res}")
    return res


def _recon_sales_years(start: str, end: str):
    """Сверка с 1С по годам: документы, строки регистра, stoimost. Расхождение — ошибка таски."""
    import sys
    if DAGS_PATH not in sys.path:
        sys.path.insert(0, DAGS_PATH)
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    pg = PostgresHook(postgres_conn_id=CONFIG_CONN_ID)
    ms = MsSqlHook(mssql_conn_id="mssql_1c_conn")
    s1, e1 = _c1(start + "-01"), _c1(end + "-01")
    one = {int(y): (int(d), int(n), float(st or 0)) for y, d, n, st in ms.get_records(
        f"SELECT YEAR(_Period)-2000, COUNT(DISTINCT _RecorderRRef), COUNT(*), SUM(_Fld17855) "
        f"FROM dbo._AccumRg17844 WITH (NOLOCK) WHERE _Period >= '{s1}' AND _Period < '{e1}' GROUP BY YEAR(_Period)")}
    prod = {int(y): (int(d), int(n), float(st or 0)) for y, d, n, st in pg.get_records(
        f"SELECT extract(year FROM s.period)::int, count(DISTINCT s.recorder), count(p.id), coalesce(sum(p.stoimost),0) "
        f"FROM public.sales s LEFT JOIN public.sales_positions p ON p.sales_id = s.id "
        f"WHERE s.period >= '{start}-01' AND s.period < '{end}-01' GROUP BY 1")}
    bad = []
    print(f"{'год':<6}{'1С док':>10}{'PROD док':>10}{'1С строк':>11}{'PROD строк':>11}{'Δ stoimost':>18}")
    for y in sorted(set(one) | set(prod)):
        a, b = one.get(y, (0, 0, 0.0)), prod.get(y, (0, 0, 0.0))
        d = b[2] - a[2]
        ok = a[0] == b[0] and a[1] == b[1] and abs(d) < 0.01
        print(f"{y:<6}{a[0]:>10,}{b[0]:>10,}{a[1]:>11,}{b[1]:>11,}{d:>18,.2f}  {'✅' if ok else '❌'}")
        if not ok:
            bad.append(y)
    if bad:
        raise RuntimeError(f"сверка с 1С не сошлась за годы: {bad}")
    print("ВСЕ ГОДЫ СХОДЯТСЯ С 1С")


with DAG(
    dag_id="historical_load_prod",
    description="Историческая загрузка PROD по месяцам (ручной запуск): sales 2012-03 → 2026-03",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=45),
        "depends_on_past": False,
    },
    tags=["historical", "etl", "prod"],
    doc_md=__doc__,
) as dag:
    for register, (m_start, m_end) in CHAINS.items():
        prev = None
        for start, end in _months(m_start, m_end):
            task = PythonOperator(
                task_id=f"{register}__{start[:7].replace('-', '_')}",
                python_callable=_load_month,
                op_args=[register, start, end],
            )
            if prev is not None:
                prev >> task
            prev = task
        if register == "sales":
            recon = PythonOperator(
                task_id=f"recon__{register}",
                python_callable=_recon_sales_years,
                op_args=[m_start, m_end],
                retries=0,
                execution_timeout=timedelta(minutes=20),
            )
            prev >> recon
