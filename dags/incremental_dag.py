"""
incremental / incremental_prod — единый инкремент всех активных сущностей etl_meta.

Один файл, одна фабрика build_incremental_dag(), два контура:
  • incremental       — TEST, etl_meta в postgre_test_base (10.10.1.142/test)
  • incremental_prod  — PROD, etl_meta в etl_prod          (10.10.1.142/etl_prod)

Каждый DAG читает ТОЛЬКО свой etl_meta и пишет только в свою витрину:
config_conn_id — одновременно и конфиг, и целевая БД (dst) для runner'ов.
Источники общие: MSSQL 1С (mssql_1c_conn) и retail (bd_retail).

DAG намеренно тупой. Он знает ровно три вещи:
  1. какие регистры активны (etl_meta.registers.is_active);
  2. какой у каждого pipeline_type;
  3. какой существующий runner соответствует pipeline_type.

Всё остальное — watermark, окна, ключи, метки, мэппинги, имена таблиц,
raw_refs — живёт в runner'ах. Здесь этого нет и не должно появляться.

Как решается, что сущность можно запускать в incremental: у каждого runner'а
своё предусловие. reference_dim обслуживает любой активный справочник — с
retail-привязкой это retail-инкремент + stub-pass, без неё только stub-pass;
что именно — решает loader по конфигу, DAG не знает. accumrg_with_documents
без retail-привязки падает внутри ETLEngine до первого запроса — для него
таску не создаём и явно показываем пропуск в unsupported_report.

Сущности читаются из etl_meta при парсинге DAG. Если БД недоступна —
DAG всё равно появляется в UI с одной таской, которая объясняет причину.
Так парсер не роняет DAG молча и не скрывает проблему.

Новый DAG создаётся PAUSED: включение — осознанное действие после ручного
контролируемого прогона.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DAGS_PATH = "/home/dev/airflow/dags"


# ──────────────────────────────────────────────────────────────────────────────
#  Runners: pipeline_type → существующий механизм. Никакой реализации здесь.
#  Все получают conn_id параметрами — контур задаёт фабрика, не runner.
# ──────────────────────────────────────────────────────────────────────────────
def _run_reference_dim(code: str, config_conn_id: str, retail_conn_id: str, mssql_conn_id: str):
    """Справочник → load_dim_from_config.process(mode='incremental')."""
    import sys
    if DAGS_PATH not in sys.path:
        sys.path.insert(0, DAGS_PATH)
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    from core.tools.load_dim_from_config import process

    pg = PostgresHook(postgres_conn_id=config_conn_id)
    rt = PostgresHook(postgres_conn_id=retail_conn_id)
    ms = MsSqlHook(mssql_conn_id=mssql_conn_id)
    res = process(code, pg, rt, ms, mode="incremental", batch=500, dry_run=False)
    if res is None:
        raise RuntimeError(f"{code}: runner вернул None — нет конфига или мэппингов")
    print(f"{code} DONE: {res}")
    return res


def _run_etl_engine(code: str, config_conn_id: str, retail_conn_id: str, mssql_conn_id: str):
    """Регистры движка (accumrg_with_documents, document_with_vt) → ETLEngine(mode='incremental')."""
    import sys
    if DAGS_PATH not in sys.path:
        sys.path.insert(0, DAGS_PATH)
    from core.etl_engine import ETLEngine

    res = ETLEngine(
        register_code=code,
        mode="incremental",
        config_conn_id=config_conn_id,
        dst_conn_id=config_conn_id,      # витрина живёт рядом с etl_meta своего контура
        src_conn_id=mssql_conn_id,
        retail_conn_id=retail_conn_id,
    ).run()
    print(f"{code} DONE: {res}")
    return res


# requires_retail — часть контракта runner'а, а не знание о таблицах:
#   reference_dim сам решает по конфигу, есть у справочника retail-часть
#   или он source-only (тогда только stub-pass) — DAG'у это безразлично;
#   accumrg_with_documents без retail-привязки не имеет watermark'а и падает
#   внутри ETLEngine до первого запроса — таску для него не создаём.
RUNNERS = {
    "reference_dim":          {"run": _run_reference_dim, "requires_retail": False},
    "accumrg_with_documents": {"run": _run_etl_engine,    "requires_retail": True},
    # документ-шапка + UNION табличных частей (order): тот же ETLEngine, watermark/tail/missing-delete
    # — существующий DataChecker по retail-привязке регистра. Никакой order-specific логики.
    "document_with_vt":       {"run": _run_etl_engine,    "requires_retail": True},
}


# ──────────────────────────────────────────────────────────────────────────────
#  Discovery: что запускать. Читается при парсинге DAG, из etl_meta контура.
# ──────────────────────────────────────────────────────────────────────────────
def _discover(config_conn_id: str):
    """
    Возвращает (supported, unsupported, error).

    supported   — [(code, pipeline_type)], для них создаются таски.
    unsupported — [(code, pipeline_type, причина)], попадают в отчёт.
    error       — текст, если etl_meta недоступна при парсинге.
    """
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        rows = PostgresHook(postgres_conn_id=config_conn_id).get_records(
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


# ──────────────────────────────────────────────────────────────────────────────
#  Фабрика: один контур = один DAG. Отличаются только conn_id и dag_id.
# ──────────────────────────────────────────────────────────────────────────────
def build_incremental_dag(
    dag_id: str,
    config_conn_id: str,
    *,
    schedule: str = "*/5 * * * *",
    retail_conn_id: str = "bd_retail",
    mssql_conn_id: str = "mssql_1c_conn",
    start_date: datetime = datetime(2026, 9, 1),
    is_paused_upon_creation: bool = True,
    tags=("incremental", "etl"),
) -> DAG:
    supported, unsupported, error = _discover(config_conn_id)
    conn_kwargs = {
        "config_conn_id": config_conn_id,
        "retail_conn_id": retail_conn_id,
        "mssql_conn_id":  mssql_conn_id,
    }

    dag = DAG(
        dag_id=dag_id,
        description=(f"Единый инкремент всех активных сущностей etl_meta "
                     f"[{config_conn_id}]: runner по pipeline_type"),
        start_date=start_date,
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=is_paused_upon_creation,
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=2),
            "execution_timeout": timedelta(minutes=10),
            "depends_on_past": False,
        },
        tags=list(tags),
        doc_md=f"**Контур:** `{config_conn_id}`\n\n" + (__doc__ or ""),
    )
    with dag:
        for code, ptype in supported:
            PythonOperator(
                task_id=f"{ptype}__{code}",
                python_callable=RUNNERS[ptype]["run"],
                op_args=[code],
                op_kwargs=conn_kwargs,
            )

        if unsupported or error:
            PythonOperator(
                task_id="unsupported_report",
                python_callable=_report_unsupported,
                op_args=[unsupported, error],
                retries=0,
                execution_timeout=timedelta(minutes=1),
            )
    return dag


# TEST — как было: тот же dag_id, те же task_id, тот же conn. Состояние (unpaused) не меняется.
incremental = build_incremental_dag("incremental", "postgre_test_base")

# PROD — новый, создаётся paused. Включается вручную после контролируемого прогона.
incremental_prod = build_incremental_dag(
    "incremental_prod", "etl_prod", tags=("incremental", "etl", "prod"),
)
