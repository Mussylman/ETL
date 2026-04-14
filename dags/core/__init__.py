"""
Core ETL Module.

Универсальная ETL-система для загрузки данных из 1С в PostgreSQL.

Использование:
    from core import ETLEngine

    etl = ETLEngine(
        register_code="sales_register",
        mode="full_period",
        start_date="4025-10-01",
        end_date="4025-11-01"
    )
    etl.run()
"""

from .etl_engine import ETLEngine
from .etl_core import ETLCore  # legacy
from .sales_etl import SalesETL

from .config import (
    ConfigLoader,
    RegisterConfig,
    SourceConfig,
    TargetConfig,
    UnionConfig,
    UnionMember,
    ColumnMapping,
)
from .builder import QueryBuilder
from .extract import StorageConnector, DataChecker, TableInspector
from .transform import TransformUtils
from .load import Loaders

__all__ = [
    # Main
    "ETLEngine",
    "ETLCore",  # legacy
    "SalesETL",

    # Config
    "ConfigLoader",
    "RegisterConfig",
    "SourceConfig",
    "TargetConfig",
    "UnionConfig",
    "UnionMember",
    "ColumnMapping",

    # Components
    "QueryBuilder",
    "StorageConnector",
    "DataChecker",
    "TableInspector",
    "TransformUtils",
    "Loaders",
]
