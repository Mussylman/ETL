"""
incremental — единый инкремент для всех активных сущностей etl_meta.

Заменяет два тестовых DAG, доказавших, что оба механизма работают:
  • sales_incremental_5min  → ETLEngine(mode="incremental")
  • dim_incremental_15min   → load_dim_from_config.process(mode="incremental")

DAG намеренно тупой. Он знает ровно три вещи:
  1. какие регистры активны (etl_meta.registers.is_active);
  2. какой у каждого pipeline_type;
  3. какой существующий runner соответствует pipeline_type.

Всё остальное — watermark, окна, ключи, метки, мэппинги, имена таблиц —
живёт в runner'ах. Здесь этого нет и не должно появляться.

Как решается, что сущность можно запускать в incremental: у каждого runner'а
своё предусловие. reference_dim обслуживает любой активный справочник — с
retail-привязкой это retail-инкремент + stub-pass, без неё только stub-pass;
что именно — решает loader по конфигу, DAG не знает. accumrg_with_documents
без retail-привязки падает внутри ETLEngine до первого запроса — для него
таску не создаём и явно показываем пропуск. Нового metadata-флага не нужно.

Сущности читаются из etl_meta при парсинге DAG. Если БД недоступна —
DAG всё равно появляется в UI с одной таской, которая объясняет причину.
Так парсер не роняет DAG молча и не скрывает проблему.

Первая версия — тестовый контур (10.10.1.142/test). Создаётся PAUSED:
пока старые DAG работают, параллельный запуск двоил бы загрузку.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DAGS_PATH = "/home/dev/airflow/dags"
CONFIG_CONN_ID = "postgre_test_base"


# ──────────────────────────────────────────────────────────────────────────────
#  Runners: pipeline_type → существующий механизм. Никакой реализации здесь.
# ──────────────────────────────────────────────────────────────────────────────
def _run_reference_dim(code: str):
    """Справочник → load_dim_from_config.process(mode='incremental')."""
    import sys
    if DAGS_PATH not in sys.path:
        sys.path.insert(0, DAGS_PATH)
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    from core.tools.load_dim_from_config import process

    pg = PostgresHook(postgres_conn_id=CONFIG_CONN_ID)
    rt = PostgresHook(postgres_conn_id="bd_retail")
    ms = MsSqlHook(mssql_conn_id="mssql_1c_conn")
    res = process(code, pg, rt, ms, mode="incremental", batch=500, dry_run=False)
    if res is None:
        raise RuntimeError(f"{code}: runner вернул None — нет конфига или мэппингов")
    print(f"{code} DONE: {res}")
    return res


def _run_accumrg_with_documents(code: str):
    """Регистр накопления с документами → ETLEngine(mode='incremental')."""
    import sys
    if DAGS_PATH not in sys.path:
        sys.path.insert(0, DAGS_PATH)
    from core.etl_engine import ETLEngine

    res = ETLEngine(register_code=code, mode="incremental").run()
    print(f"{code} DONE: {res}")
    return res


# requires_retail — часть контракта runner'а, а не знание о таблицах:
#   reference_dim сам решает по конфигу, есть у справочника retail-часть
#   или он source-only (тогда только stub-pass) — DAG'у это безразлично;
#   accumrg_with_documents без retail-привязки не имеет watermark'а и падает
#   внутри ETLEngine до первого запроса — таску для него не создаём.
RUNNERS = {
    "reference_dim":          {"run": _run_reference_dim,          "requires_retail": False},
    "accumrg_with_documents": {"run": _run_accumrg_with_documents, "requires_retail": True},
}


# ──────────────────────────────────────────────────────────────────────────────
#  Discovery: что запускать. Читается при парсинге DAG.
# ──────────────────────────────────────────────────────────────────────────────
def _discover():
    """
    Возвращает (supported, unsupported, error).

    supported   — [(code, pipeline_type)], для них создаются таски.
    unsupported — [(code, pipeline_type, причина)], попадают в отчёт.
    error       — текст, если etl_meta недоступна при парсинге.
    """
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        rows = PostgresHook(postgres_conn_id=CONFIG_CONN_ID).get_records(
            "SELECT code, pipeline_type, "
            "       (retail_table IS NOT NULL AND retail_uid_column IS NOT NULL) "
            "FROM etl_meta.registers WHERE is_active "
            "ORDER BY pipeline_type NULLS LAST, code")
    except Exception as e:  # noqa: BLE001 — любая ошибка БД одинаково важна
        return [], [], f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"

    supported, unsupported = [], []
    for code, ptype, has_retail in rows:
        if ptype not in RUNNERS:
            unsupported.append((code, ptype, f"нет runner'а для pipeline_type={ptype!r}"))
        elif RUNNERS[ptype]["requires_retail"] and not has_retail:
            unsupported.append((code, ptype, "runner требует retail-привязку, её нет"))
        else:
            supported.append((code, ptype))
    return supported, unsupported, None


def _report_unsupported(items, error):
    """Одна таска на весь список — чтобы пропуски были видны в UI каждый тик."""
    if error:
        raise RuntimeError(
            f"etl_meta недоступна при парсинге DAG: {error}. "
            f"Таски сущностей не созданы — DAG перестроится, когда БД вернётся.")
    if not items:
        print("все активные сущности поддерживают incremental")
        return
    print(f"пропущено как unsupported: {len(items)}")
    for code, ptype, reason in items:
        print(f"  {code:<22} pipeline_type={ptype!s:<26} {reason}")


SUPPORTED, UNSUPPORTED, DISCOVERY_ERROR = _discover()


# ──────────────────────────────────────────────────────────────────────────────
#  DAG
# ──────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="incremental",
    description="Единый инкремент всех активных сущностей etl_meta: runner по pipeline_type",
    start_date=datetime(2026, 9, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
        "depends_on_past": False,
    },
    tags=["incremental", "etl"],
    doc_md=__doc__,
) as dag:
    for _code, _ptype in SUPPORTED:
        PythonOperator(
            task_id=f"{_ptype}__{_code}",
            python_callable=RUNNERS[_ptype]["run"],
            op_args=[_code],
        )

    if UNSUPPORTED or DISCOVERY_ERROR:
        PythonOperator(
            task_id="unsupported_report",
            python_callable=_report_unsupported,
            op_args=[UNSUPPORTED, DISCOVERY_ERROR],
            retries=0,
            execution_timeout=timedelta(minutes=1),
        )
