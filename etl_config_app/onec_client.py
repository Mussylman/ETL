"""
1C HTTP meta API client — standalone (no Airflow dependency).
"""

import requests
from typing import List

BASE_URL = "http://192.168.18.224:8090/NikitaBase/hs/meta"


def search_1c(q: str) -> List[dict]:
    """
    Search 1C objects by name.
    Returns: [{"table_name": "Документ.ЧекККМ", "table_name_sql": "Document476"}, ...]
    """
    try:
        resp = requests.get(f"{BASE_URL}/search", params={"q": q}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])
    except Exception:
        return []


def get_structure(table_names: List[str]) -> List[dict]:
    """
    Get table structure from 1C.
    table_names can be Russian names ("Документ.ЧекККМ") or SQL names ("_Document476", "Document476").
    Returns: [{"table_name": ..., "table_name_sql": ..., "fields": [...]}, ...]
    """
    try:
        names = ",".join(table_names)
        resp = requests.get(f"{BASE_URL}/db_structure/{names}", timeout=15)
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception:
        return []


def resolve_document_names(type_numbers: List[int]) -> dict:
    """
    Given a list of document type numbers (e.g. [254, 476]),
    call 1C API to resolve their Russian names.
    Returns: {476: "Документ.ЧекККМ", 254: None, ...}
    """
    result = {}
    # Try to get structure for all at once (API expects names without _ prefix)
    sql_names = [f"Document{n}" for n in type_numbers]
    try:
        structures = get_structure(sql_names)
        # Build lookup by table_name_sql
        found = {}
        for s in structures:
            found[s.get("table_name_sql", "")] = s.get("table_name", "")
        for n in type_numbers:
            key = f"Document{n}"
            result[n] = found.get(key)
    except Exception:
        for n in type_numbers:
            result[n] = None
    return result


def discover_document_with_vt(type_numbers: List[int]) -> dict:
    """
    For each document type number, search 1C API to get the document name
    AND all its tabular parts (VT tables).

    search?q=Document476 returns:
      Document476           → Документ.ЧекККМ          (шапка)
      Document476.VT13626   → Документ.ЧекККМ.Товары   (табличная часть)
      ...

    Returns: {
      476: {
        "onec_name": "Документ.ЧекККМ",
        "vt_tables": [
          {"vt_number": "13626",
           "table_name_sql": "Document476.VT13626",
           "mssql_table": "_Document476_VT13626",
           "onec_name": "Документ.ЧекККМ.Товары"},
          ...
        ]
      },
      ...
    }
    """
    result = {}
    for n in type_numbers:
        doc_prefix = f"Document{n}"
        items = search_1c(doc_prefix)
        onec_name = None
        vt_tables = []
        for item in items:
            sql_name = item.get("table_name_sql", "")
            if sql_name == doc_prefix:
                # Main document
                onec_name = item.get("table_name")
            elif sql_name.startswith(f"{doc_prefix}.VT"):
                # Tabular part: Document476.VT13626
                vt_part = sql_name.split(".VT")[-1]  # "13626"
                mssql_table = f"_Document{n}_VT{vt_part}"
                vt_tables.append({
                    "vt_number": vt_part,
                    "table_name_sql": sql_name,
                    "mssql_table": mssql_table,
                    "onec_name": item.get("table_name"),
                })
        result[n] = {
            "onec_name": onec_name,
            "vt_tables": vt_tables,
        }
    return result
