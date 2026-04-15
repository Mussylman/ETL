"""
DEPRECATED: Используйте ETLEngine вместо ETLCore.

ETLEngine читает конфигурацию из etl_meta схемы (через ETL Config App),
поддерживает UNION, JOIN, column mappings и все transform types.

Migration path:
    # Было:
    etl = ETLCore(mssql_table="_AccumRg17844", target_table="sales", ...)
    # Стало:
    etl = ETLEngine(register_code="sales", mode="full_period", ...)
"""

import warnings
from typing import Optional, List

from .extract.data_checker import DataChecker
from .extract.storage_connector import StorageConnector
from .transform.transform_utils import TransformUtils
from .load.loaders import Loaders


class ETLCore:
    """
    DEPRECATED: Универсальный ETL-оркестратор для таблиц 1С → Postgres.
    Используйте ETLEngine для новых регистров.

    Поддерживаемые режимы:
      - full_period  : полная загрузка по периоду из 1С (_Period)
      - incremental  : обмен по UIDs из retail (каждые 5 минут)
      - consistency  : обмен по "задним" изменениям из retail (каждые 4 часа)
    """

    def __init__(
        self,
        mssql_table: str,
        target_table: str,                # целевая таблица в Postgres, напр. 'sales_register'
        mode: str = "full_period",        # full_period / incremental / consistency

        # --- коннекты ---
        src_conn_id: str = "mssql_1c_conn",        # 1С (MSSQL)
        dst_conn_id: str = "postgre_test_base",    # ETL-база (Postgres)
        retail_conn_id: str = "bd_retail",         # retail (Postgres)

        # --- для full_period ---
        start_date: Optional[str] = None,          # '4025-10-01'
        end_date: Optional[str] = None,            # '4025-11-01'

        # --- ключи и таблицы для обмена ---
        retail_table: str = "sales",               # таблица в bd_retail
        retail_uid_column: str = "document_uid",   # UID в retail
        mssql_key_column: str = "_RecorderRRef",   # ключ в 1С (binary(16))
        target_key_column: str = "Регистратор",    # ключ в целевой таблице Postgres
    ):
        warnings.warn(
            "ETLCore is deprecated. Use ETLEngine(register_code=...) instead.",
            DeprecationWarning, stacklevel=2,
        )

        self.mssql_table = mssql_table
        self.target_table = target_table
        self.mode = mode

        self.src_conn_id = src_conn_id
        self.dst_conn_id = dst_conn_id
        self.retail_conn_id = retail_conn_id

        self.start_date = start_date
        self.end_date = end_date

        self.retail_table = retail_table
        self.retail_uid_column = retail_uid_column
        self.mssql_key_column = mssql_key_column
        self.target_key_column = target_key_column

        # --- слои ETL ---
        self.storage = StorageConnector(src_conn_id=self.src_conn_id)
        self.transform = TransformUtils(pg_conn_id=self.dst_conn_id)
        self.loader = Loaders(dst_conn_id=self.dst_conn_id)

    # ==========================================================
    #  ПУБЛИЧНЫЙ ЗАПУСК
    # ==========================================================
    def run(self):
        print(f"🚀 ETL START: mode={self.mode} | 1C={self.mssql_table} → PG={self.target_table}")

        if self.mode == "full_period":
            self._run_full_period()

        elif self.mode in ("incremental", "consistency"):
            self._run_change_based()

        else:
            raise ValueError("❌ mode должен быть одним из: 'full_period', 'incremental', 'consistency'")

        print(f"✅ ETL DONE: {self.mssql_table} → {self.target_table} (mode={self.mode})")

    # ==========================================================
    #  1) ПОЛНАЯ ЗАГРУЗКА ПО ПЕРИОДУ (ПЕРВАЯ / REBUILD)
    # ==========================================================
    def _run_full_period(self):
        """
        Берём данные только из 1С по полю _Period (без retail),
        трансформируем и грузим INSERT ONLY.
        """
        if not self.start_date or not self.end_date:
            raise ValueError("Для режима 'full_period' нужно указать start_date и end_date")

        # 1️⃣ Чтение из MSSQL по периоду
        df_raw, binary_columns = self.storage.read_from_mssql(
            table_name=self.mssql_table,
            start_date=self.start_date,
            end_date=self.end_date,
        )

        if df_raw.empty:
            print("⚠️ Нет данных для загрузки (full_period)")
            return

        # 2️⃣ Трансформация типов (binary → UUID, даты, etl_loaded_at)
        df_transformed = self.transform.transform_dataframe(df_raw, binary_columns)

        # 3️⃣ Маппинг колонок (source → target)
        column_map = self.transform.get_column_map(self.mssql_table)
        if column_map:
            df_transformed = df_transformed.rename(columns=column_map)

        # 4️⃣ Загрузка в целевую таблицу (первая загрузка / rebuild)
        self.loader.insert_only(df_transformed, self.target_table)

    # ==========================================================
    #  2) ОБМЕН НА ОСНОВЕ ИЗМЕНЕНИЙ (INCREMENTAL / CONSISTENCY)
    # ==========================================================
    def _run_change_based(self):
        """
        Логика обмена на основе изменений в retail:
          1. Берём список UID изменённых документов из retail (DataChecker)
          2. По этим UID читаем полные записи из 1С (StorageConnector)
          3. Трансформируем (TransformUtils)
          4. Делаем UPSERT в витрину (Loaders.upsert_by_key)
        """
        # 1️⃣ Получаем список UID из retail
        checker = DataChecker(
            retail_table=self.retail_table,
            retail_conn_id=self.retail_conn_id,
            etl_table=self.target_table,
            etl_conn_id=self.dst_conn_id,
            key_column=self.retail_uid_column,
        )
        changed_df = checker.detect_changes()

        if changed_df.empty:
            print("No changed documents, sync not required.")
            return

        uids: List[str] = changed_df["uid"].tolist()
        print(f"UIDs to update: {len(uids)}")

        # 2️⃣ Читаем из MSSQL по списку ключей (UID → _RecorderRRef)
        df_raw, binary_columns = self.storage.read_from_mssql(
            table_name=self.mssql_table,
            key_column=self.mssql_key_column,
            key_values=uids,
        )

        if df_raw.empty:
            print("⚠️ В 1С не найдено данных по переданным UID.")
            return

        # 3️⃣ Трансформация типов
        df_transformed = self.transform.transform_dataframe(df_raw, binary_columns)

        # 4️⃣ Маппинг колонок
        column_map = self.transform.get_column_map(self.mssql_table)
        if column_map:
            df_transformed = df_transformed.rename(columns=column_map)

        # Проверим, что целевой ключ после маппинга присутствует
        if self.target_key_column not in df_transformed.columns:
            raise ValueError(
                f"❌ В трансформированном DataFrame нет столбца ключа '{self.target_key_column}'. "
                f"Проверь маппинг etl_columns для таблицы {self.mssql_table}."
            )

        # 5️⃣ UPSERT в целевую таблицу
        self.loader.upsert_by_key(
            df=df_transformed,
            table_name=self.target_table,
            key_column=self.target_key_column,
        )
