"""
Основной класс трансформации данных.

Использует функции из:
  - binary.py : работа с binary-данными
  - dates.py  : работа с датами
  - cast.py   : приведение типов
"""

from typing import List, Dict, Any, Optional, Callable
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Импорт функций из подмодулей
from .binary import (
    binary_to_uuid,
    binary_to_int,
    binary_to_bool,
    process_binary_auto,
    binary_to_hex,
)
from .dates import fix_year, parse_1c_date
from .cast import cast
from .custom import apply_custom, CUSTOM_TRANSFORMS


class TransformUtils:
    """
    Утилиты для трансформации данных.

    Поддерживаемые transform_type:
      - binary_to_uuid : binary(16) → UUID
      - binary_to_int  : binary(4) → int
      - binary_to_bool : binary(1) → bool
      - fix_year       : 4025 → 2025
      - cast           : приведение типа (int, float, decimal, str)
      - constant       : константное значение
    """

    # Реестр трансформеров
    TRANSFORMERS: Dict[str, Callable] = {}

    def __init__(self, pg_conn_id: str = "postgre_test_base"):
        self.pg_conn_id = pg_conn_id
        self._register_transformers()

    def _register_transformers(self):
        """Регистрация стандартных трансформеров."""
        self.TRANSFORMERS = {
            "binary_to_uuid": lambda v, p: binary_to_uuid(v),
            "binary_to_int": lambda v, p: binary_to_int(v),
            "binary_to_bool": lambda v, p: binary_to_bool(v),
            "fix_year": lambda v, p: fix_year(v),
            "cast": lambda v, p: cast(v, (p or {}).get("type", "str")),
            "constant": lambda v, p: (p or {}).get("value"),
            "recorder_type_lookup": self._recorder_type_lookup,
        }

    def _recorder_type_lookup(self, value, params):
        """
        binary(4) → int → название документа из lookup-словаря.
        params = {"map": {"476": "ЧекККМ", "254": "Возврат", ...}}
        Словарь загружается один раз из register.recorder_type_map.
        """
        if not params or "map" not in params:
            return binary_to_int(value) if isinstance(value, (bytes, bytearray)) else value
        type_int = binary_to_int(value) if isinstance(value, (bytes, bytearray)) else value
        if type_int is None:
            return None
        lookup = params["map"]
        return lookup.get(str(type_int), str(type_int))

    # =========================================================================
    # Публичные методы (обёртки для совместимости)
    # =========================================================================

    def convert_idrref_to_guid(self, b: bytes):
        """Конвертация binary(16) → UUID (формат 1С)."""
        return binary_to_uuid(b)

    def process_binary_value(self, value):
        """binary(16) → UUID, binary(4) → int, binary(1) → bool."""
        return process_binary_auto(value)

    def fix_high_year_exact(self, val):
        """Коррекция: 4025 → 2025."""
        return fix_year(val)

    def fix_high_year(self, val: str):
        """Коррекция года в строке."""
        return fix_year(val) if isinstance(val, str) else val

    # =========================================================================
    # JSONB binary_raw
    # =========================================================================

    def build_binary_json(self, df, binary_columns):
        """Создаёт JSONB-словарь: {col: hex_value} для бинарных колонок."""
        json_list = []

        for _, row in df[binary_columns].iterrows():
            entry = {col: binary_to_hex(row[col]) for col in binary_columns}
            json_list.append(entry)

        return pd.Series(json_list)

    # =========================================================================
    # Главная трансформация DataFrame
    # =========================================================================

    def transform_dataframe(self, df, binary_columns):
        """Базовая трансформация DataFrame."""
        df = df.copy()
        loaded_at = datetime.utcnow()

        # 1. Binary → UUID/int/bool
        for col in binary_columns:
            if col in df.columns:
                df[col] = df[col].apply(process_binary_auto)

        # 2. Конвертация _Period
        for col in df.columns:
            if col.lower() in ("_period", "период"):
                df[col] = df[col].apply(fix_year)
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # 3. JSONB для бинарных значений
        if binary_columns:
            df["binary_fields_raw"] = self.build_binary_json(df, binary_columns)
        else:
            df["binary_fields_raw"] = [{}] * len(df)

        # 4. Дата загрузки
        df["etl_loaded_at"] = loaded_at

        return df

    # =========================================================================
    # Маппинг колонок
    # =========================================================================

    def get_column_map(self, table_name):
        """Загружает маппинг source_column → target_column из etl_columns."""
        pg = PostgresHook(postgres_conn_id=self.pg_conn_id)

        sql = f"""
            SELECT c.source_column, c.target_column
            FROM etl_columns c
            JOIN etl_config t ON t.id = c.config_id
            WHERE t.source_mssql_table = '{table_name}'
              AND c.is_active = TRUE
        """

        df = pg.get_pandas_df(sql)

        if df.empty:
            return {}

        return dict(zip(df["source_column"], df["target_column"]))

    # =========================================================================
    # Конфигурируемые трансформации
    # =========================================================================

    def apply_transform(
        self,
        value: Any,
        transform_type: Optional[str],
        transform_params: Optional[Dict] = None,
    ) -> Any:
        """Применяет трансформацию к одному значению."""
        if not transform_type:
            return value

        transformer = self.TRANSFORMERS.get(transform_type)
        if transformer:
            return transformer(value, transform_params)

        return value

    def transform_dataframe_by_config(
        self,
        df: pd.DataFrame,
        binary_columns: List[str],
        column_transforms: Optional[Dict[str, Dict]] = None,
    ) -> pd.DataFrame:
        """
        Трансформирует DataFrame по конфигурации колонок.

        Args:
            df: исходный DataFrame
            binary_columns: список binary-колонок для автотрансформации
            column_transforms: словарь {column: {"type": "...", "params": {...}}}

        Returns:
            Трансформированный DataFrame
        """
        df = df.copy()
        loaded_at = datetime.utcnow()
        column_transforms = column_transforms or {}

        # 1. Автоматическая трансформация binary-колонок
        for col in binary_columns:
            if col in df.columns and col not in column_transforms:
                df[col] = df[col].apply(process_binary_auto)

        # 2. Применение конфигурируемых трансформаций (column-level)
        custom_columns = {}
        for col, config in column_transforms.items():
            transform_type = config.get("type")

            # custom_python обрабатывается отдельно (row-level)
            if transform_type == "custom_python":
                custom_columns[col] = config
                continue

            if col not in df.columns:
                continue

            transform_params = config.get("params")

            if transform_type:
                df[col] = df[col].apply(
                    lambda v, tt=transform_type, tp=transform_params: self.apply_transform(v, tt, tp)
                )

        # 3. Коррекция дат
        for col in df.columns:
            if col.lower() in ("_period", "period", "document_date"):
                if col not in column_transforms:
                    df[col] = df[col].apply(fix_year)
                    df[col] = pd.to_datetime(df[col], errors="coerce")

        # 4. Вычисляемые колонки (row-level custom_python)
        if custom_columns:
            df = self._apply_custom_transforms(df, custom_columns)

        # 5. Добавляем etl_loaded_at
        df["etl_loaded_at"] = loaded_at

        return df

    def _apply_custom_transforms(
        self,
        df: pd.DataFrame,
        custom_columns: Dict[str, Dict],
    ) -> pd.DataFrame:
        """
        Применяет custom_python трансформации (row-level).

        Каждая функция получает строку как dict и возвращает значение.
        """
        for col, config in custom_columns.items():
            func_name = (config.get("params") or {}).get("function")
            if not func_name:
                continue
            df[col] = df.apply(
                lambda row, fn=func_name: apply_custom(fn, row.to_dict()),
                axis=1,
            )
        return df
