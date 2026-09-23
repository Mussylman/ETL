"""
clickhouse_sync — перенос аналитического слоя в ClickHouse после инкремента PostgreSQL.

Второй hop конвейера. Первый (1С → PostgreSQL) живёт в incremental_dag.py и здесь
не дублируется: этот DAG берёт уже загруженный PostgreSQL как источник.

DAG намеренно тупой — ровно как incremental_dag. Он не знает ни одной таблицы:
список объектов, их источники, партиционирование и метрики сверки читаются из
etl_meta.ch_sync. Новый объект появляется в загрузке записью конфигурации, без
правки этого файла и без новой таски.

Две таски вместо таски на таблицу:
    sync__dimensions  — конфигурации load_mode=full  (справочники)
    sync__facts       — конфигурации load_mode=partitioned (факты, cost)
Порядок именно такой: факты ссылаются на справочники, и BI не должен увидеть
факт с id, которого в справочнике ещё нет. Внутри каждой таски движок идёт по
priority из конфигурации.

Гарантия при расхождении: партиция со сверкой, которая не сошлась, НЕ публикуется —
staging просто не переезжает в цель. Существующие данные ClickHouse остаются
прежними, таска падает, причина пишется в etl_meta.ch_sync_history и
ch_sync_partition_state. Полузагруженного состояния не бывает by design.

Состояние живёт в PostgreSQL control plane. В ClickHouse управляющих таблиц нет.
Загрузка идёт под etl_writer; ch_admin в ETL не участвует и в Airflow не заведён.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DAGS_PATH = "/home/dev/airflow/dags"


def _sync(load_mode: str, config_conn_id: str, ch_conn_id: str):
    """Общий прогон движка по активным конфигурациям нужного режима."""
    import sys
    if DAGS_PATH not in sys.path:
        sys.path.insert(0, DAGS_PATH)
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from core.clickhouse import engine as eng
    from core.clickhouse.config import load_active_specs
    from core.clickhouse.source import open_source
    from core.clickhouse.target import ClickHouse

    pg = PostgresHook(postgres_conn_id=config_conn_id)
    ch = ClickHouse(ch_conn_id)
    specs = [s for s in load_active_specs(pg) if s.load_mode == load_mode]
    if not specs:
        print(f"активных конфигураций load_mode={load_mode} нет")
        return

    failed, total_rows = [], 0
    for spec in specs:
        if not ch.table_exists(spec.fqn):
            failed.append(f"{spec.code}: нет таблицы {spec.fqn}")
            print(f"✗ {spec.code}: целевой таблицы нет, пропущено")
            continue
        src = open_source(spec)
        plan = eng.affected_partitions(pg, ch, spec, src)
        if plan.get("orphan"):
            # есть в ClickHouse, нет в источнике — сообщаем, но не удаляем:
            # удаление партиции это решение человека, а не побочный эффект синка
            print(f"⚠ {spec.code}: партиции без источника {plan['orphan']}")
        done = 0
        for key in plan["keys"]:
            res = eng.sync_partition(pg, ch, spec, src, key, apply=True)
            if res["status"] == "failed":
                failed.append(f"{spec.code}/{key}: {res.get('note')}")
                print(f"✗ {spec.code} {key}: {res.get('note')}")
                break
            if res["status"] == "ok":
                done += 1
                total_rows += res.get("rows_target", 0)
        print(f"{'✓' if not failed else '·'} {spec.code}: партиций затронуто "
              f"{len(plan['keys'])}, перезалито {done}")

    if failed:
        # Цель не изменена ни по одной несошедшейся партиции — падаем, чтобы
        # расхождение было видно, а не растворилось в зелёном прогоне.
        raise RuntimeError("ClickHouse sync не сошёлся:\n  " + "\n  ".join(failed))
    print(f"строк опубликовано: {total_rows:,}")


def build_clickhouse_sync_dag(
    dag_id: str,
    config_conn_id: str,
    *,
    ch_conn_id: str = "clickhouse_etl",
    schedule=None,
    start_date: datetime = datetime(2026, 9, 1),
    is_paused_upon_creation: bool = True,
    tags=("clickhouse", "analytics"),
) -> DAG:
    dag = DAG(
        dag_id=dag_id,
        description=f"Аналитический слой ClickHouse по конфигурации etl_meta.ch_sync [{config_conn_id}]",
        start_date=start_date,
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=is_paused_upon_creation,
        default_args={
            "retries": 0,          # повтор не лечит расхождение, только маскирует
            "depends_on_past": False,
        },
        tags=list(tags),
        doc_md=__doc__,
    )
    with dag:
        dims = PythonOperator(
            task_id="sync__dimensions",
            python_callable=_sync,
            op_args=["full", config_conn_id, ch_conn_id],
            execution_timeout=timedelta(minutes=30),
        )
        facts = PythonOperator(
            task_id="sync__facts",
            python_callable=_sync,
            op_args=["partitioned", config_conn_id, ch_conn_id],
            execution_timeout=timedelta(minutes=60),
        )
        dims >> facts
    return dag


# PROD. Расписания нет: запускается после успешного incremental_prod.
clickhouse_sync = build_clickhouse_sync_dag("clickhouse_sync", "etl_prod",
                                            tags=("clickhouse", "analytics", "prod"))
