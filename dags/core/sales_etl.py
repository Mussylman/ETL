"""
ETL-оркестратор для пары таблиц: sales (шапка) + sales_positions (позиции).

Поток данных:
  1. Retail DB (bd_retail) — обнаружение изменений по updated_at
  2. Целевая DB (postgre_test_base) — сравнение с последней загруженной updated_at
  3. 1С (mssql_1c_conn) — чтение полных данных по изменённым document_uid
  4. Трансформация (binary→UUID, fix_year и т.д.)
  5. UPSERT шапок / DELETE+INSERT позиций в целевую PostgreSQL

Использование:
    from core import SalesETL

    # Инкрементальная загрузка
    etl = SalesETL(
        mssql_header_table="_Document123",
        mssql_positions_table="_Document123_VT456",
    )
    etl.run()

    # Полная загрузка по периоду
    etl.run_full(start_date="4025-10-01", end_date="4025-11-01")
"""

from typing import Optional, List

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from .extract.data_checker import DataChecker
from .extract.storage_connector import StorageConnector
from .transform.transform_utils import TransformUtils
from .load.loaders import Loaders


class SalesETL:
    """
    Оркестратор ETL для пары таблиц sales + sales_positions.

    Шапки (sales) — UPSERT по document_uid.
    Позиции (sales_positions) — DELETE по document_uid + INSERT
    (у позиций нет уникального ключа строки).
    """

    def __init__(
        self,
        # MSSQL-таблицы 1С
        mssql_header_table: str,                    # напр. "_Document123"
        mssql_positions_table: str,                 # напр. "_Document123_VT456"
        mssql_key_column: str = "_IDRRef",          # ключ документа в MSSQL (binary(16))

        # Целевые таблицы PostgreSQL
        target_header_table: str = "sales",
        target_positions_table: str = "sales_positions",

        # Retail (для обнаружения изменений)
        retail_table: str = "sales",
        retail_uid_column: str = "document_uid",

        # Connections
        src_conn_id: str = "mssql_1c_conn",
        dst_conn_id: str = None,              # PostgreSQL — только явно
        retail_conn_id: str = "bd_retail",
        database: str = "UPP_JAN",
    ):
        self.mssql_header_table = mssql_header_table
        self.mssql_positions_table = mssql_positions_table
        self.mssql_key_column = mssql_key_column

        self.target_header_table = target_header_table
        self.target_positions_table = target_positions_table

        self.retail_table = retail_table
        self.retail_uid_column = retail_uid_column

        self.src_conn_id = src_conn_id
        self.dst_conn_id = dst_conn_id
        self.retail_conn_id = retail_conn_id
        self.database = database

        # ETL-слои
        self.storage = StorageConnector(src_conn_id=self.src_conn_id, database=self.database)
        self.transform = TransformUtils(pg_conn_id=self.dst_conn_id)
        self.loader = Loaders(dst_conn_id=self.dst_conn_id)

    # ==================================================================
    #  ПУБЛИЧНЫЕ МЕТОДЫ
    # ==================================================================

    def run(self):
        """
        Инкрементальная загрузка: обнаружение изменений через retail,
        чтение из 1С, трансформация, загрузка в PostgreSQL.
        """
        print(f"SalesETL START (incremental): "
              f"1C=[{self.mssql_header_table}, {self.mssql_positions_table}] "
              f"-> PG=[{self.target_header_table}, {self.target_positions_table}]")

        # 1. Обнаружение изменений
        changed_df = self._detect_changes()

        if changed_df.empty:
            print("No changed documents, sync not required.")
            return

        uids: List[str] = changed_df["uid"].tolist()
        print(f"UIDs to update: {len(uids)}")

        # 2. Извлечение шапок из 1С (read_from_mssql конвертирует UUID→hex сам)
        df_header, header_binary = self._extract_header(uids)

        # 3. Извлечение позиций из 1С
        df_positions, positions_binary = self._extract_positions(uids)

        # 4. Трансформация
        if not df_header.empty:
            df_header = self._transform(df_header, header_binary)
        if not df_positions.empty:
            df_positions = self._transform(df_positions, positions_binary)

        # 5. Загрузка шапок (UPSERT)
        self._load_header(df_header)

        # 6. Загрузка позиций (DELETE + INSERT)
        self._load_positions(df_positions, uids)

        print(f"SalesETL DONE (incremental): "
              f"headers={len(df_header)}, positions={len(df_positions)}")

    def run_full(self, start_date: str, end_date: str):
        """
        Полная загрузка по периоду из 1С (без retail).
        Использует INSERT ONLY — предполагает, что целевые таблицы пустые
        или данные за этот период ещё не загружены.
        """
        print(f"SalesETL START (full_period): {start_date} - {end_date}")

        # 1. Шапки по периоду
        df_header, header_binary = self.storage.read_from_mssql(
            table_name=self.mssql_header_table,
            start_date=start_date,
            end_date=end_date,
        )

        # 2. Позиции: нужно извлечь UID из шапок, чтобы загрузить связанные позиции
        if not df_header.empty and self.mssql_key_column in df_header.columns:
            # Берём binary-ключи из шапок и конвертируем в UUID-строки
            from .transform.binary import binary_to_uuid
            raw_keys = df_header[self.mssql_key_column].dropna().unique()
            uid_strings = []
            for k in raw_keys:
                if isinstance(k, (bytes, bytearray, memoryview)):
                    uid = binary_to_uuid(k)
                    if uid:
                        uid_strings.append(str(uid))

            if uid_strings:
                df_positions, positions_binary = self._extract_positions(uid_strings)
            else:
                df_positions = pd.DataFrame()
                positions_binary = []
        else:
            df_positions = pd.DataFrame()
            positions_binary = []

        # 3. Трансформация
        if not df_header.empty:
            df_header = self._transform(df_header, header_binary)
        if not df_positions.empty:
            df_positions = self._transform(df_positions, positions_binary)

        # 4. Загрузка (INSERT ONLY для полной загрузки)
        if not df_header.empty:
            self.loader.insert_only(df_header, self.target_header_table)
            print(f"Full load headers: {len(df_header)} rows")

        if not df_positions.empty:
            self.loader.insert_only(df_positions, self.target_positions_table)
            print(f"Full load positions: {len(df_positions)} rows")

        print(f"SalesETL DONE (full_period): "
              f"headers={len(df_header)}, positions={len(df_positions)}")

    # ==================================================================
    #  ПРИВАТНЫЕ МЕТОДЫ
    # ==================================================================

    def _detect_changes(self) -> pd.DataFrame:
        """Обнаружение изменений через DataChecker (retail vs target)."""
        checker = DataChecker(
            retail_table=self.retail_table,
            retail_conn_id=self.retail_conn_id,
            etl_table=self.target_header_table,
            etl_conn_id=self.dst_conn_id,
            key_column=self.retail_uid_column,
        )
        return checker.detect_changes()

    def _extract_header(self, uids: List[str]):
        """Чтение шапок из MSSQL по списку UUID-строк."""
        return self.storage.read_from_mssql(
            table_name=self.mssql_header_table,
            key_column=self.mssql_key_column,
            key_values=uids,
        )

    def _extract_positions(self, uids: List[str]):
        """Чтение позиций из MSSQL по списку UUID-строк."""
        return self.storage.read_from_mssql(
            table_name=self.mssql_positions_table,
            key_column=self.mssql_key_column,
            key_values=uids,
        )

    def _transform(self, df: pd.DataFrame, binary_columns: List[str]) -> pd.DataFrame:
        """Трансформация: binary→UUID, fix_year, etl_loaded_at."""
        df = self.transform.transform_dataframe(df, binary_columns)

        # Маппинг колонок (если настроен)
        column_map = self.transform.get_column_map(self.mssql_header_table)
        if column_map:
            df = df.rename(columns=column_map)

        return df

    def _load_header(self, df: pd.DataFrame):
        """UPSERT шапок по document_uid."""
        if df.empty:
            print(f"No header data to load into {self.target_header_table}")
            return

        # Определяем имя ключевой колонки после маппинга
        key_column = self._resolve_key_column()

        if key_column not in df.columns:
            raise ValueError(
                f"Key column '{key_column}' not found in header DataFrame. "
                f"Available columns: {list(df.columns)}"
            )

        self.loader.upsert_by_key(
            df=df,
            table_name=self.target_header_table,
            key_column=key_column,
        )

    def _load_positions(self, df: pd.DataFrame, uids: Optional[List[str]] = None):
        """
        Загрузка позиций: DELETE старых + INSERT новых.

        У позиций нет уникального ключа строки,
        поэтому UPSERT невозможен — удаляем все позиции
        по document_uid и вставляем заново.
        """
        if df.empty:
            print(f"No positions data to load into {self.target_positions_table}")
            return

        key_column = self._resolve_key_column()
        pg = PostgresHook(postgres_conn_id=self.dst_conn_id)

        # Определяем UIDs для удаления
        if uids:
            delete_uids = uids
        elif key_column in df.columns:
            delete_uids = df[key_column].dropna().unique().tolist()
        else:
            delete_uids = []

        # DELETE старых позиций по document_uid
        if delete_uids:
            placeholders = ", ".join([f"'{uid}'" for uid in delete_uids])
            delete_sql = (
                f'DELETE FROM {self.target_positions_table} '
                f'WHERE "{key_column}" IN ({placeholders})'
            )

            with pg.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(delete_sql)
                conn.commit()

            print(f"Deleted positions for {len(delete_uids)} documents")

        # INSERT новых позиций
        self.loader.insert_only(df, self.target_positions_table)

    def _resolve_key_column(self) -> str:
        """
        Определяет имя ключевой колонки после маппинга.

        Если есть маппинг для mssql_key_column — возвращает замапленное имя,
        иначе — оригинальное имя mssql_key_column.
        """
        column_map = self.transform.get_column_map(self.mssql_header_table)

        if column_map and self.mssql_key_column in column_map:
            return column_map[self.mssql_key_column]

        return self.mssql_key_column
