from typing import List, Union, Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


class Loaders:
    """
    Загрузчик данных в PostgreSQL.

    Поддерживаемые режимы:
      - insert  : простой INSERT (для первичной загрузки)
      - upsert  : INSERT ... ON CONFLICT (для инкрементала)
      - replace : DELETE + INSERT (для полной перезагрузки)
    """

    def __init__(self, dst_conn_id: str = "postgre_test_base"):
        self.dst_conn_id = dst_conn_id

    def _normalize_datetimes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Конвертирует datetime64 в python datetime."""
        df = df.copy()
        for col in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[col].dtype):
                continue
            # dtype=object обязателен: иначе pandas выведет datetime64 обратно и
            # None снова станет NaT, который psycopg2 отдаёт как 'NaT'::timestamp.
            df[col] = pd.Series(
                [x.to_pydatetime() if pd.notna(x) else None for x in df[col]],
                index=df.index, dtype=object,
            )
        return df

    def _normalize_uuids(self, df: pd.DataFrame) -> pd.DataFrame:
        """Конвертирует UUID в строки для PostgreSQL."""
        df = df.copy()
        for col in df.columns:
            sample = df[col].dropna().head(1)
            if not sample.empty:
                from uuid import UUID
                if isinstance(sample.iloc[0], UUID):
                    df[col] = df[col].apply(lambda x: str(x) if x else None)
        return df

    def _normalize_json(self, df: pd.DataFrame) -> pd.DataFrame:
        """dict/list в ячейках (raw_refs) → psycopg2 Json, иначе адаптер не знает dict."""
        from psycopg2.extras import Json
        df = df.copy()
        for col in df.columns:
            sample = df[col].dropna().head(1)
            if not sample.empty and isinstance(sample.iloc[0], (dict, list)):
                df[col] = df[col].apply(lambda x: Json(x) if isinstance(x, (dict, list)) else None)
        return df

    @staticmethod
    def _is_null_scalar(v) -> bool:
        """
        Пустота pandas/numpy, которую PostgreSQL не должен увидеть как значение.
        Строки, UUID, dict/Json, datetime и bytes здесь никогда не трогаем.
        """
        if v is None or v is pd.NaT:
            return True
        if isinstance(v, float):
            return v != v or v in (float("inf"), float("-inf"))
        from decimal import Decimal
        if isinstance(v, Decimal):
            return v.is_nan() or v.is_infinite()
        # numpy-скаляры (np.float64 — подкласс float, но np.datetime64('NaT') — нет)
        if hasattr(v, "dtype") and getattr(v, "shape", None) == ():
            try:
                return bool(pd.isna(v))
            except Exception:  # noqa: BLE001
                return False
        return False

    def _normalize_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        NaN / NaT / ±inf → None перед записью.

        Мера, которой нет у части источников (COALESCE по VT даёт NULL), в pandas
        становится float('nan'), а psycopg2 адаптирует его как 'NaN'::numeric —
        для PostgreSQL это легальное значение numeric, INSERT проходит, а SUM()
        по колонке возвращает NaN (аудит PROD sales_positions 2026-09-03:
        408 497 строк с evrika_bonusy = 'NaN'). Единая точка нормализации для
        всех режимов загрузки: nullable-значение в БД — только NULL.
        Реальные 0 остаются 0, строки/UUID/JSON не меняются.
        """
        import numpy as np
        df = df.copy()
        for col in df.columns:
            s = df[col]
            if pd.api.types.is_float_dtype(s.dtype):
                values = s.to_numpy(dtype=float, na_value=np.nan)
                bad = ~np.isfinite(values)
                if bad.any():
                    df[col] = s.astype(object).where(~bad, None)
            elif s.dtype == object:
                # dtype=object явно — Series.map/apply включают вывод типа, и колонка
                # из datetime/None снова стала бы datetime64 с NaT вместо None.
                df[col] = pd.Series(
                    [None if self._is_null_scalar(v) else v for v in s],
                    index=s.index, dtype=object,
                )
        return df

    def _prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Подготовка DataFrame для загрузки."""
        df = self._normalize_datetimes(df)
        df = self._normalize_uuids(df)
        df = self._normalize_json(df)
        df = self._normalize_nulls(df)
        return df

    # ======================================================================
    #  INSERT (простая вставка)
    # ======================================================================
    def insert_only(self, df: pd.DataFrame, table_name: str, batch_size: int = 3000):
        """Простой INSERT без обработки конфликтов."""
        if df.empty:
            print(f"INSERT: no data for {table_name}")
            return 0

        pg = PostgresHook(postgres_conn_id=self.dst_conn_id)
        df = self._prepare_df(df)

        pg.insert_rows(
            table=table_name,
            rows=df.itertuples(index=False, name=None),
            target_fields=list(df.columns),
            commit_every=batch_size,
        )

        print(f"INSERT: {len(df)} rows -> {table_name}")
        return len(df)

    # ======================================================================
    #  UPSERT с одним ключом (legacy)
    # ======================================================================
    def upsert_by_key(self, df: pd.DataFrame, table_name: str, key_column: str):
        """UPSERT по одному ключу (обратная совместимость)."""
        return self.upsert_by_keys(df, table_name, [key_column])

    # ======================================================================
    #  UPSERT с составным ключом (НОВОЕ)
    # ======================================================================
    def upsert_by_keys(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: Union[str, List[str]],
        batch_size: int = 1000,
    ) -> int:
        """
        UPSERT по одному или нескольким ключам.

        Args:
            df: DataFrame с данными
            table_name: целевая таблица
            key_columns: колонка или список колонок для ON CONFLICT
            batch_size: размер батча для commit

        Returns:
            Количество обработанных строк
        """
        if df.empty:
            print(f"UPSERT: no data for {table_name}")
            return 0

        # Нормализуем key_columns в список
        if isinstance(key_columns, str):
            key_columns = [key_columns]

        # Проверяем наличие ключевых колонок
        for key in key_columns:
            if key not in df.columns:
                raise ValueError(f"Key column '{key}' not found in DataFrame")

        pg = PostgresHook(postgres_conn_id=self.dst_conn_id)
        df = self._prepare_df(df)

        columns = list(df.columns)
        update_columns = [c for c in columns if c not in key_columns]

        # Формируем SQL
        cols_sql = ", ".join([f'"{c}"' for c in columns])
        placeholders = ", ".join([f"%({c})s" for c in columns])

        # ON CONFLICT для составного ключа
        conflict_cols = ", ".join([f'"{k}"' for k in key_columns])

        # SET clause для UPDATE
        if update_columns:
            set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_columns])
            sql = f"""
                INSERT INTO {table_name} ({cols_sql})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {set_clause};
            """
        else:
            # Если нет колонок для обновления (только ключи) — DO NOTHING
            sql = f"""
                INSERT INTO {table_name} ({cols_sql})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_cols})
                DO NOTHING;
            """

        rows = df.to_dict(orient="records")

        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                # ETL audit-колонки в Asia/Almaty TZ (см. docs/sales_load_modes.md)
                # Влияет на DEFAULT now() для etl_loaded_at.
                cur.execute("SET TIME ZONE 'Asia/Almaty'")
                for i, r in enumerate(rows):
                    cur.execute(sql, r)
                    if (i + 1) % batch_size == 0:
                        conn.commit()
                conn.commit()

        print(f"UPSERT: {len(rows)} rows -> {table_name} (keys: {key_columns})")
        return len(rows)

    # ======================================================================
    #  REPLACE (DELETE + INSERT)
    # ======================================================================
    def replace_all(
        self,
        df: pd.DataFrame,
        table_name: str,
        where_clause: Optional[str] = None,
    ) -> int:
        """
        Полная замена данных: DELETE + INSERT.

        Args:
            df: DataFrame с данными
            table_name: целевая таблица
            where_clause: условие для DELETE (если None — очищает всю таблицу)

        Returns:
            Количество вставленных строк
        """
        if df.empty:
            print(f"REPLACE: no data for {table_name}")
            return 0

        pg = PostgresHook(postgres_conn_id=self.dst_conn_id)

        # DELETE
        if where_clause:
            delete_sql = f"DELETE FROM {table_name} WHERE {where_clause}"
        else:
            delete_sql = f"TRUNCATE TABLE {table_name}"

        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(delete_sql)
            conn.commit()

        # INSERT
        return self.insert_only(df, table_name)

    # ======================================================================
    #  DELETE-INSERT BY RECORDER (для инкрементального fact-таргета)
    # ======================================================================
    def delete_insert_by_recorder(
        self,
        df: pd.DataFrame,
        table_name: str,
        recorder_column: str = "recorder",
    ) -> int:
        """
        Удаляет из target все строки с recorder из DataFrame, потом INSERT.

        Для инкрементальной загрузки FACT-таблицы (sales_positions),
        у которой нет стабильного составного ключа — например, документ
        перепровели и количество позиций изменилось.
        Сценарий:
          1. Триггер инкремента даёт список изменённых recorder (через retail)
          2. По этим recorder читаем из 1С полные позиции
          3. DELETE FROM target WHERE recorder IN (...) — убираем старые позиции
          4. INSERT — кладём новые

        Args:
            df: DataFrame с новыми позициями (должен содержать recorder_column)
            table_name: целевая таблица
            recorder_column: имя колонки recorder в df и target (по умолчанию "recorder")

        Returns:
            Количество вставленных строк

        Включение в register_targets: load_mode = "delete_insert_by_recorder".
        Сейчас неактивен — для первой волны используем full_period+upsert.
        Для перехода на инкремент: смени load_mode в etl_meta.register_targets.
        """
        if df.empty:
            print(f"DELETE_INSERT: no data for {table_name}")
            return 0

        if recorder_column not in df.columns:
            raise ValueError(
                f"Column '{recorder_column}' not in DataFrame — "
                f"delete_insert_by_recorder requires it"
            )

        recorders = df[recorder_column].dropna().unique().tolist()
        if not recorders:
            print(f"DELETE_INSERT: no recorders to delete for {table_name}")
            return self.insert_only(df, table_name)

        pg = PostgresHook(postgres_conn_id=self.dst_conn_id)
        placeholders = ", ".join(["%s"] * len(recorders))
        delete_sql = f'DELETE FROM {table_name} WHERE "{recorder_column}" IN ({placeholders})'

        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(delete_sql, [str(r) for r in recorders])
                deleted = cur.rowcount
            conn.commit()

        print(f"DELETE: {deleted} rows from {table_name} (recorders: {len(recorders)})")
        return self.insert_only(df, table_name)

    # ======================================================================
    #  Универсальный метод load() для ETLEngine
    # ======================================================================
    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = "upsert",
        upsert_keys: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        recorder_column: str = "recorder",
    ) -> int:
        """
        Универсальный метод загрузки.

        Args:
            df: DataFrame с данными
            table_name: целевая таблица
            mode: 'insert' / 'upsert' / 'replace' / 'delete_insert_by_recorder'
            upsert_keys: ключи для upsert
            where_clause: условие для replace
            recorder_column: имя колонки recorder для delete_insert_by_recorder

        Returns:
            Количество обработанных строк
        """
        if mode == "insert":
            return self.insert_only(df, table_name)

        elif mode == "upsert":
            if not upsert_keys:
                raise ValueError("upsert_keys required for 'upsert' mode")
            return self.upsert_by_keys(df, table_name, upsert_keys)

        elif mode == "replace":
            return self.replace_all(df, table_name, where_clause)

        elif mode == "delete_insert_by_recorder":
            return self.delete_insert_by_recorder(df, table_name, recorder_column)

        else:
            raise ValueError(f"Unknown load mode: {mode}")
