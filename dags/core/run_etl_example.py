"""
Пример использования нового ETLEngine.

Запуск:
    cd /home/dev/airflow/dags
    python -m core.run_etl_example

Этот файл НЕ является DAG-ом.
"""

try:
    from .etl_engine import ETLEngine
except ImportError:
    from core.etl_engine import ETLEngine


def run_full_period():
    """Полная загрузка регистра продаж."""
    etl = ETLEngine(
        register_code="sales_register",
        mode="full_period",
        start_date="4025-10-01",
        end_date="4025-11-01",
    )
    results = etl.run()
    print(f"Results: {results}")


def run_incremental():
    """Инкрементальная загрузка."""
    etl = ETLEngine(
        register_code="sales_register",
        mode="incremental",
    )
    results = etl.run()
    print(f"Results: {results}")


def run_single_target():
    """Загрузка только в одну таблицу."""
    etl = ETLEngine(
        register_code="sales_register",
        mode="full_period",
        start_date="4025-10-01",
        end_date="4025-11-01",
        target_tables=["sales_daily"],  # только агрегат
    )
    results = etl.run()
    print(f"Results: {results}")


if __name__ == "__main__":
    # Выберите нужный режим:
    run_full_period()
    # run_incremental()
    # run_single_target()
