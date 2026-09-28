"""
Cutover helper — установка/просмотр watermark per-register.

После того как full_period отработал и бэкфилл сошёлся с MSSQL,
выставляем watermark = (backfill_start - 1 час), чтобы инкремент
не тянул всё с 2000 года.

Использование:
    from core.tools.set_watermark import set_watermark, get_watermark

    # Просмотр
    print(get_watermark("sales"))

    # Установка после бэкфилла
    set_watermark(
        register_code="sales",
        checkpoint="2026-06-08 11:00:00",  # backfill_start - 1 hour
    )

CLI:
    python -m core.tools.set_watermark --register sales --checkpoint "2026-06-08 11:00:00"
"""

from datetime import datetime, timedelta
from typing import Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook




def _get_register_id(pg, code: str) -> int:
    rows = pg.get_records(
        "SELECT id FROM etl_meta.registers WHERE code=%s AND is_active=TRUE",
        parameters=(code,),
    )
    if not rows:
        raise ValueError(f"Register '{code}' not found or inactive")
    return rows[0][0]


def get_watermark(register_code: str, config_conn_id: str) -> Optional[str]:
    """Текущий watermark per-register (последний success checkpoint)."""
    pg = PostgresHook(postgres_conn_id=config_conn_id)
    reg_id = _get_register_id(pg, register_code)
    rows = pg.get_records(
        """
        SELECT checkpoint_value, finished_at
        FROM etl_meta.load_history
        WHERE register_id = %s
          AND status = 'success'
          AND checkpoint_value IS NOT NULL
        ORDER BY finished_at DESC NULLS LAST, id DESC
        LIMIT 1
        """,
        parameters=(reg_id,),
    )
    if not rows:
        return None
    return str(rows[0][0])


def set_watermark(
    register_code: str,
    checkpoint: str,
    note: str = "cutover",
    config_conn_id: str = None,
) -> int:
    """
    Установить watermark per-register, вставив success-строку в load_history.

    Args:
        register_code: код регистра
        checkpoint: ISO timestamp (например '2026-06-08 11:00:00')
        note: причина — сохранится в run_mode для аудита (по умолчанию 'cutover')

    Returns:
        ID добавленной строки в load_history.
    """
    from core.conn import require_conn
    require_conn("config_conn_id", config_conn_id)
    # Валидация timestamp
    try:
        datetime.fromisoformat(checkpoint.replace("Z", "+00:00"))
    except ValueError as e:
        raise ValueError(f"checkpoint must be ISO timestamp, got {checkpoint!r}: {e}")

    pg = PostgresHook(postgres_conn_id=config_conn_id)
    reg_id = _get_register_id(pg, register_code)

    rows = pg.get_records(
        """
        INSERT INTO etl_meta.load_history
            (register_id, run_mode, status, started_at, finished_at,
             checkpoint_value, rows_extracted, rows_loaded)
        VALUES (%s, %s, 'success', NOW(), NOW(), %s, 0, 0)
        RETURNING id
        """,
        parameters=(reg_id, note, checkpoint),
    )
    run_id = rows[0][0]
    print(f"✅ Watermark set: register={register_code} (id={reg_id}) "
          f"checkpoint={checkpoint} run_id={run_id}")
    return run_id


def set_watermark_from_backfill(
    register_code: str,
    backfill_start: str,
    safety_gap: timedelta = timedelta(hours=1),
    config_conn_id: str = None,
) -> int:
    """
    Удобный cutover после full_period: watermark = backfill_start - safety_gap.
    Гарантирует что инкремент НЕ пропустит границу запуска бэкфилла.

    backfill_start — реальный момент старта вашего full_period (NOW() на запуске).
    """
    from core.conn import require_conn
    require_conn("config_conn_id", config_conn_id)
    start_dt = datetime.fromisoformat(backfill_start.replace("Z", "+00:00"))
    cp = (start_dt - safety_gap).strftime("%Y-%m-%d %H:%M:%S.%f")
    return set_watermark(register_code, checkpoint=cp, note="cutover_from_full", config_conn_id=config_conn_id)


if __name__ == "__main__":
    import argparse, sys
    p = argparse.ArgumentParser(description="ETL incremental watermark helper")
    p.add_argument("--pg-conn", required=True, help="conn_id PostgreSQL — только явно")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_get = sub.add_parser("get", help="Показать текущий watermark")
    p_get.add_argument("--register", required=True)

    p_set = sub.add_parser("set", help="Установить watermark вручную")
    p_set.add_argument("--register", required=True)
    p_set.add_argument("--checkpoint", required=True,
                       help="ISO timestamp, например 2026-06-08T11:00:00")
    p_set.add_argument("--note", default="cutover_manual")

    p_cut = sub.add_parser("cutover", help="После full_period: watermark = backfill_start - 1ч")
    p_cut.add_argument("--register", required=True)
    p_cut.add_argument("--backfill-start", required=True,
                       help="Когда стартовал full_period (NOW при запуске)")
    p_cut.add_argument("--gap-hours", type=int, default=1)

    args = p.parse_args()

    if args.cmd == "get":
        wm = get_watermark(args.register, config_conn_id=args.pg_conn)
        print(wm if wm else "(не установлен — fallback to 2000-01-01)")
    elif args.cmd == "set":
        set_watermark(args.register, args.checkpoint, args.note, config_conn_id=args.pg_conn)
    elif args.cmd == "cutover":
        set_watermark_from_backfill(
            args.register, args.backfill_start,
            safety_gap=timedelta(hours=args.gap_hours), config_conn_id=args.pg_conn,
        )
