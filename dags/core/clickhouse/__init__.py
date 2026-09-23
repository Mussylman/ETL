"""
Синхронизация PostgreSQL/MSSQL → ClickHouse по конфигурации из etl_meta.

Движок не знает ни одной таблицы: что грузить, откуда и как сверять — читается из
etl_meta.ch_sync и etl_meta.ch_sync_columns. Новый объект подключается записью
конфигурации, а не новым загрузчиком.
"""
from .config import SyncSpec, load_spec, load_active_specs          # noqa: F401
from .target import ClickHouse                                      # noqa: F401
