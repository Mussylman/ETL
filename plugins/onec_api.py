"""
Клиент HTTP-сервиса 1С для получения метаданных.

DEPRECATED: Thin wrapper. Основная реализация в etl_config_app/onec_client.py.
Этот модуль оставлен для совместимости с Airflow plugin views.
"""

import sys
import os

# Добавляем etl_config_app в path для импорта
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'etl_config_app'))

from typing import List, Optional

try:
    import onec_client as _client
except ImportError:
    _client = None


class OneCMetaClient:
    """Обёртка над onec_client для совместимости с plugin views."""

    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or "http://192.168.18.224:8090/NikitaBase/hs/meta"

    def search(self, query: str) -> List[dict]:
        if _client:
            return _client.search_1c(query)
        return []

    def get_structure(self, table_names: List[str]) -> List[dict]:
        if _client:
            return _client.get_structure(table_names)
        return []
