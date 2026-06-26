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
    Для каждого номера документа отдать русское имя шапки + VT-таблицы.

    Источники:
      • русское имя шапки — `/db_structure/Document{N}` (1С API)
      • список VT — MSSQL `INFORMATION_SCHEMA.TABLES` LIKE `_Document{N}_VT%`
      • русское имя каждой VT — `/db_structure/Document{N}.VT{M}` (1С API)

    Раньше использовали `/search`, но он по SQL-имени отдаёт пусто.

    Returns: тот же контракт, что и раньше:
      {476: {"onec_name": "Документ.ЧекККМ",
             "vt_tables": [{"vt_number": "13626", "table_name_sql": "Document476.VT13626",
                            "mssql_table": "_Document476_VT13626",
                            "onec_name": "Документ.ЧекККМ.Товары"}, ...]},
       ...}
    """
    # MSSQL: найти физические VT-таблицы для каждого документа
    vt_by_doc = _list_vt_tables_in_mssql(type_numbers)

    # Собираем имена для одного batch-запроса к 1С API
    all_names = []
    for n in type_numbers:
        all_names.append(f"Document{n}")
        for vt_n in vt_by_doc.get(n, []):
            all_names.append(f"Document{n}.VT{vt_n}")

    structures = get_structure(all_names) if all_names else []
    name_map = {s.get("table_name_sql", ""): s.get("table_name", "") for s in structures}

    result = {}
    for n in type_numbers:
        doc_sql = f"Document{n}"
        vt_tables = []
        for vt_n in vt_by_doc.get(n, []):
            vt_sql = f"Document{n}.VT{vt_n}"
            onec = name_map.get(vt_sql) or None
            # Orphan-таблицы (физически есть в MSSQL, но в 1С Configurator
            # реквизит удалён → API не возвращает имя) — отфильтровываем.
            if not onec:
                continue
            vt_tables.append({
                "vt_number": vt_n,
                "table_name_sql": vt_sql,
                "mssql_table": f"_Document{n}_VT{vt_n}",
                "onec_name": onec,
            })
        result[n] = {
            "onec_name": name_map.get(doc_sql) or None,
            "vt_tables": vt_tables,
        }
    return result


def _list_vt_tables_in_mssql(type_numbers: List[int]) -> dict:
    """
    Из MSSQL INFORMATION_SCHEMA — список VT-номеров для каждого Document{N}.
    Returns: {476: ["13626", "13671", ...], 254: [...], ...}
    Пустой словарь при недоступности MSSQL — не валим discover.
    """
    try:
        import mssql_client
    except Exception:
        return {n: [] for n in type_numbers}

    result = {n: [] for n in type_numbers}
    conn = None
    try:
        conn = mssql_client.get_conn()
        cur = conn.cursor()
        like_clauses = " OR ".join([f"TABLE_NAME LIKE '_Document{n}_VT%'" for n in type_numbers])
        if not like_clauses:
            return result
        cur.execute(f"""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE='BASE TABLE' AND ({like_clauses})
        """)
        for (name,) in cur.fetchall():
            # _Document476_VT13626 → 476, 13626
            try:
                rest = name[len("_Document"):]
                doc_part, vt_part = rest.split("_VT", 1)
                doc_n = int(doc_part)
                if doc_n in result:
                    result[doc_n].append(vt_part)
            except Exception:
                continue
        cur.close()
    except Exception:
        pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    # стабильный порядок
    for k in result:
        result[k].sort()
    return result
