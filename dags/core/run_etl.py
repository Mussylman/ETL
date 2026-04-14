from .etl_core import ETLCore

etl = ETLCore(
    mssql_table="_AccumRg17844",
    target_table="sales_register",
    mode="full_period",
    start_date="4025-10-01",
    end_date="4025-11-01",
)
etl.run()
