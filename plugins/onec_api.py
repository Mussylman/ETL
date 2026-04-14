"""
Клиент HTTP-сервиса 1С для получения метаданных (таблицы и колонки).
"""

import requests
from typing import List, Optional
from airflow.models import Variable


class OneCMetaClient:
    """Клиент для работы с HTTP-сервисом метаданных 1С."""

    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or Variable.get(
            "onec_meta_url",
            default_var="http://192.168.18.224:8090/NikitaBase/hs/meta",
        )

    def search(self, query: str) -> List[dict]:
        """
        Поиск объектов 1С по имени (для autocomplete).

        Returns:
            [{"table_name": "Документ.ЧекККМ", "table_name_sql": "Document476"}, ...]
        """
        url = f"{self.base_url}/search"
        resp = requests.get(url, params={"q": query}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])

    def get_structure(self, table_names: List[str]) -> List[dict]:
        """
        Получение структуры таблиц 1С.

        Args:
            table_names: ["Документ.ЧекККМ", "Справочник.Номенклатура"]

        Returns:
            [
                {
                    "table_name": "Документ.ЧекККМ",
                    "table_name_sql": "Document476",
                    "fields": [
                        {"field_name": "Ссылка", "field_name_sql": "ID"},
                        ...
                    ]
                }
            ]
        """
        names_param = ",".join(table_names)
        url = f"{self.base_url}/db_structure/{names_param}"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])
