"""
Загрузка и применение маппинга колонок из etl_columns.
"""

from typing import Dict, List, Optional, Tuple
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ..transform import (
    binary_to_uuid,
    binary_to_int,
    binary_to_bool,
    process_binary_auto,
    fix_year,
    cast,
)


class ColumnMapper:
    """
    Загружает маппинг колонок из etl_columns и применяет трансформации.
    """

    # Реестр трансформаций
    TRANSFORMS = {
        'binary_to_uuid': binary_to_uuid,
        'binary_to_int': binary_to_int,
        'binary_to_bool': binary_to_bool,
        'binary_auto': process_binary_auto,
        'fix_year': fix_year,
    }

    def __init__(self, pg_conn_id: str = "postgre_test_base"):
        self.pg_conn_id = pg_conn_id
        self._cache: Dict[int, List[dict]] = {}

    def load_mappings(self, entity_id: int) -> List[dict]:
        """
        Загружает маппинг колонок для entity.

        Returns:
            [{source_column, target_column, transform_type, is_key}, ...]
        """
        if entity_id in self._cache:
            return self._cache[entity_id]

        pg = PostgresHook(postgres_conn_id=self.pg_conn_id)
        df = pg.get_pandas_df(f'''
            SELECT source_column, target_column, transform_type,
                   transform_params, is_key, default_value
            FROM etl_columns
            WHERE entity_id = {entity_id} AND active = TRUE
            ORDER BY ordinal
        ''')

        mappings = df.to_dict(orient='records')
        self._cache[entity_id] = mappings
        return mappings

    def get_source_columns(self, entity_id: int) -> List[str]:
        """Список исходных колонок для SELECT."""
        mappings = self.load_mappings(entity_id)
        return [m['source_column'] for m in mappings]

    def get_key_columns(self, entity_id: int) -> List[str]:
        """Список ключевых колонок (target names)."""
        mappings = self.load_mappings(entity_id)
        return [m['target_column'] for m in mappings if m.get('is_key')]

    def get_rename_map(self, entity_id: int) -> Dict[str, str]:
        """Маппинг для переименования: {source: target}."""
        mappings = self.load_mappings(entity_id)
        return {m['source_column']: m['target_column'] for m in mappings}

    def get_transform_map(self, entity_id: int) -> Dict[str, str]:
        """Маппинг трансформаций: {target_column: transform_type}."""
        mappings = self.load_mappings(entity_id)
        return {
            m['target_column']: m['transform_type']
            for m in mappings
            if m.get('transform_type')
        }

    def apply_transforms(self, df: pd.DataFrame, entity_id: int) -> pd.DataFrame:
        """
        Применяет трансформации к DataFrame.

        1. Переименовывает колонки (source → target)
        2. Применяет трансформации (binary_to_uuid и т.д.)
        """
        mappings = self.load_mappings(entity_id)
        df = df.copy()

        # 1. Применяем трансформации (до переименования)
        for m in mappings:
            src = m['source_column']
            transform_type = m.get('transform_type')

            if src not in df.columns:
                continue

            if transform_type and transform_type in self.TRANSFORMS:
                transform_fn = self.TRANSFORMS[transform_type]
                df[src] = df[src].apply(transform_fn)

        # 2. Переименовываем колонки
        rename_map = {m['source_column']: m['target_column'] for m in mappings}
        df = df.rename(columns=rename_map)

        # 3. Оставляем только нужные колонки
        target_columns = [m['target_column'] for m in mappings]
        existing = [c for c in target_columns if c in df.columns]
        df = df[existing]

        return df

    def build_select_sql(
        self,
        entity_id: int,
        table_name: str,
        database: str = "UPP_JAN",
        schema: str = "dbo",
    ) -> str:
        """
        Генерирует SELECT SQL на основе маппинга.
        """
        columns = self.get_source_columns(entity_id)
        cols_sql = ",\n    ".join([f"[{c}]" for c in columns])

        return f"""
SELECT
    {cols_sql}
FROM [{database}].[{schema}].[{table_name}]
""".strip()


def load_column_mapper(pg_conn_id: str = "postgre_test_base") -> ColumnMapper:
    """Создаёт ColumnMapper."""
    return ColumnMapper(pg_conn_id)
