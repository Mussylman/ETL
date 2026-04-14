"""
Универсальный ETL-движок на основе конфигурации из PostgreSQL.

Использование:
    from core.etl_engine import ETLEngine

    # Полная загрузка
    etl = ETLEngine(
        register_code="sales_register",
        mode="full_period",
        start_date="4025-10-01",
        end_date="4025-11-01"
    )
    etl.run()

    # Инкрементальная загрузка
    etl = ETLEngine(register_code="sales_register", mode="incremental")
    etl.run()
"""

from typing import Optional, List, Dict
from datetime import datetime

from .config import ConfigLoader, RegisterConfig, TargetConfig
from .builder import QueryBuilder
from .extract.storage_connector import StorageConnector
from .extract.data_checker import DataChecker
from .transform.transform_utils import TransformUtils
from .load.loaders import Loaders


class ETLEngine:
    """
    Универсальный ETL-движок.

    Работает на основе конфигурации из PostgreSQL (схема etl_meta):
      1. Загружает конфигурацию регистра
      2. Генерирует SQL-запросы (с JOIN и UNION)
      3. Извлекает данные из MSSQL
      4. Трансформирует (binary->UUID, даты, маппинг)
      5. Загружает в одну или несколько целевых таблиц

    Поддерживаемые режимы:
      - full_period  : полная загрузка по периоду
      - incremental  : инкрементальная загрузка по изменениям
      - consistency  : проверка консистентности
    """

    def __init__(
        self,
        register_code: str,
        mode: Optional[str] = None,

        # Connections
        config_conn_id: str = "postgre_test_base",
        src_conn_id: str = "mssql_1c_conn",
        dst_conn_id: str = "postgre_test_base",
        retail_conn_id: str = "bd_retail",

        # Database
        database: str = "UPP_JAN",

        # For full_period mode
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,

        # For specific targets
        target_tables: Optional[List[str]] = None,
    ):
        self.register_code = register_code
        self.start_date = start_date
        self.end_date = end_date
        self.target_tables = target_tables

        # Connection IDs
        self.config_conn_id = config_conn_id
        self.src_conn_id = src_conn_id
        self.dst_conn_id = dst_conn_id
        self.retail_conn_id = retail_conn_id

        # Load config
        self.config_loader = ConfigLoader(config_conn_id)
        self.config: RegisterConfig = self.config_loader.load_register(register_code)

        # Override mode if specified
        self.mode = mode or self.config.default_mode

        # Initialize components
        self.query_builder = QueryBuilder(database=database)
        self.storage = StorageConnector(src_conn_id=src_conn_id, database=database)
        self.transform = TransformUtils(pg_conn_id=dst_conn_id)
        self.loader = Loaders(dst_conn_id=dst_conn_id)

    def run(self) -> Dict[str, int]:
        """
        Главная точка входа.

        Returns:
            Словарь {target_table: rows_loaded}
        """
        print(f"ETL START: register={self.register_code}, mode={self.mode}")
        started_at = datetime.now()

        results = {}

        if self.mode == "full_period":
            results = self._run_full_period()
        elif self.mode in ("incremental", "consistency"):
            results = self._run_incremental()
        else:
            raise ValueError(f"Unknown mode: {self.mode}")

        elapsed = (datetime.now() - started_at).total_seconds()
        total_rows = sum(results.values())
        print(f"ETL DONE: {self.register_code} | {total_rows} rows | {elapsed:.1f}s")

        return results

    def _get_active_targets(self) -> List[TargetConfig]:
        """Возвращает список активных целевых таблиц."""
        targets = [t for t in self.config.targets if t.is_active]

        if self.target_tables:
            targets = [t for t in targets if t.target_table in self.target_tables]

        return targets

    # ======================================================================
    #  FULL PERIOD MODE
    # ======================================================================
    def _run_full_period(self) -> Dict[str, int]:
        """Полная загрузка по периоду."""
        if not self.start_date or not self.end_date:
            raise ValueError("start_date and end_date required for full_period mode")

        results = {}
        targets = self._get_active_targets()

        for target in targets:
            rows = self._process_target(
                target=target,
                period_start=self.start_date,
                period_end=self.end_date,
            )
            results[target.target_table] = rows

        return results

    # ======================================================================
    #  INCREMENTAL MODE
    # ======================================================================
    def _run_incremental(self) -> Dict[str, int]:
        """Инкрементальная загрузка по изменениям."""
        # Получаем список изменённых документов
        if not self.config.retail_table or not self.config.retail_uid_column:
            raise ValueError("retail_table and retail_uid_column required for incremental mode")

        checker = DataChecker(
            retail_table=self.config.retail_table,
            retail_conn_id=self.retail_conn_id,
            key_column=self.config.retail_uid_column,
        )

        changed_df = checker.get_changed_uids()

        if changed_df.empty:
            print("No changes detected")
            return {}

        uids = changed_df["uid"].tolist()
        print(f"Found {len(uids)} changed documents")

        results = {}
        targets = self._get_active_targets()

        for target in targets:
            rows = self._process_target(
                target=target,
                key_values=uids,
            )
            results[target.target_table] = rows

        return results

    # ======================================================================
    #  PROCESS TARGET
    # ======================================================================
    def _process_target(
        self,
        target: TargetConfig,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        key_values: Optional[List[str]] = None,
    ) -> int:
        """
        Обрабатывает одну целевую таблицу.

        Returns:
            Количество загруженных строк
        """
        print(f"Processing target: {target.full_table_name}")

        # 1. Генерируем SQL
        sql = self._build_sql_for_target(
            target=target,
            period_start=period_start,
            period_end=period_end,
            key_values=key_values,
        )

        print(f"Generated SQL:\n{sql[:500]}...")

        # 2. Извлекаем данные
        df, binary_columns = self.storage.execute_query(sql)

        if df.empty:
            print(f"No data for {target.target_table}")
            return 0

        print(f"Extracted: {len(df)} rows, binary columns: {binary_columns}")

        # 3. Трансформируем
        column_transforms = self._build_column_transforms(target)
        df = self.transform.transform_dataframe_by_config(
            df=df,
            binary_columns=binary_columns,
            column_transforms=column_transforms,
        )

        # 4. Pre-load SQL (агрегация, если задана)
        if target.pre_load_sql:
            df = self._apply_pre_load_sql(df, target.pre_load_sql)

        # 5. Загружаем
        rows = self.loader.load(
            df=df,
            table_name=target.full_table_name,
            mode=target.load_mode,
            upsert_keys=target.upsert_keys,
        )

        return rows

    def _build_sql_for_target(
        self,
        target: TargetConfig,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        key_values: Optional[List[str]] = None,
    ) -> str:
        """Генерирует SQL для целевой таблицы."""

        # Определяем ключевую колонку для incremental
        key_column = None
        if key_values:
            # Ищем в источниках колонку для фильтрации по UID
            for source in self.config.sources:
                if source.source_type == "header":
                    # Предполагаем _IDRRef как стандартный ключ
                    key_column = "_IDRRef"
                    break

        if target.union_config:
            # UNION нескольких источников
            return self.query_builder.build_union_query(
                union=target.union_config,
                register=self.config,
                period_start=period_start,
                period_end=period_end,
                key_column=key_column,
                key_values=key_values,
            )
        elif target.source_config:
            # Один источник
            return self.query_builder.build_source_query(
                source=target.source_config,
                period_start=period_start,
                period_end=period_end,
                key_column=key_column,
                key_values=key_values,
            )
        else:
            raise ValueError(f"Target {target.target_table} has no source or union configured")

    def _build_column_transforms(self, target: TargetConfig) -> Dict[str, Dict]:
        """
        Собирает трансформации колонок из конфигурации.

        Returns:
            {column_name: {"type": transform_type, "params": {...}}}
        """
        transforms = {}

        # Собираем из всех источников, связанных с target
        sources = []
        if target.union_config:
            for member in target.union_config.members:
                if member.source:
                    sources.append(member.source)
                    if member.source.parent_source:
                        sources.append(member.source.parent_source)
        elif target.source_config:
            sources.append(target.source_config)
            if target.source_config.parent_source:
                sources.append(target.source_config.parent_source)

        for source in sources:
            for col in source.columns:
                if col.transform_type:
                    transforms[col.target_column] = {
                        "type": col.transform_type,
                        "params": col.transform_params,
                    }

        return transforms

    def _apply_pre_load_sql(self, df, pre_load_sql: str):
        """
        Применяет pre_load_sql к DataFrame.

        Placeholder __df__ заменяется на временную таблицу.
        """
        import duckdb

        # Используем DuckDB для SQL над DataFrame
        result = duckdb.query(pre_load_sql.replace("__df__", "df")).df()
        print(f"Pre-load SQL applied: {len(df)} -> {len(result)} rows")
        return result
