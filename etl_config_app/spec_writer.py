"""
spec_writer — ЕДИНСТВЕННАЯ точка записи RegisterSpec в etl_meta.

Отличия от dao.*:
  • вся запись регистра — ОДНА psycopg2-транзакция: упавший на середине
    wizard не оставляет полузаписанный конфиг (dao.execute коммитит каждый
    statement по отдельности);
  • перед записью — обязательный валидатор контракта (register_spec.assert_valid);
  • схема параметризуется (etl_meta боевая / etl_test для тестов).

Существующие CRUD-пути dao/app не трогаем — они будут переведены на writer
на следующих этапах.
"""

import re
from typing import Optional, Dict

import psycopg2
import psycopg2.extras

from register_spec import RegisterSpec, assert_valid
from spec_reader import read_spec  # re-export для удобства: единая точка входа

__all__ = ["write_spec", "read_spec", "SpecWriteError"]

_SCHEMA_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class SpecWriteError(RuntimeError):
    pass


def _check_schema(schema: str) -> str:
    if not _SCHEMA_RE.match(schema):
        raise SpecWriteError(f"Недопустимое имя схемы: {schema!r}")
    return schema


def _jsonb(value):
    """dict/list → psycopg2 Json (для jsonb-колонок), None → None."""
    if value is None:
        return None
    return psycopg2.extras.Json(value)


def _delete_register_tx(cur, schema: str, register_id: int) -> None:
    """Каскадное удаление конфига регистра ВНУТРИ открытой транзакции."""
    cur.execute(f"DELETE FROM {schema}.load_history WHERE register_id=%s", [register_id])
    cur.execute(f"DELETE FROM {schema}.register_targets WHERE register_id=%s", [register_id])
    cur.execute(f"""
        DELETE FROM {schema}.source_union_members
        WHERE union_id IN (SELECT id FROM {schema}.source_unions WHERE register_id=%s)
    """, [register_id])
    cur.execute(f"DELETE FROM {schema}.source_unions WHERE register_id=%s", [register_id])
    cur.execute(f"""
        DELETE FROM {schema}.column_mappings
        WHERE source_id IN (SELECT id FROM {schema}.register_sources WHERE register_id=%s)
    """, [register_id])
    cur.execute(f"UPDATE {schema}.register_sources SET parent_source_id=NULL WHERE register_id=%s", [register_id])
    cur.execute(f"DELETE FROM {schema}.register_sources WHERE register_id=%s", [register_id])
    cur.execute(f"DELETE FROM {schema}.registers WHERE id=%s", [register_id])


def write_spec(
    spec: RegisterSpec,
    schema: str = "etl_meta",
    conn=None,
    replace: bool = False,
) -> Dict:
    """
    Пишет RegisterSpec в указанную схему одной транзакцией.

    Args:
        spec:    валидируется assert_valid() до любой записи
        schema:  целевая схема ('etl_meta' / 'etl_test')
        conn:    готовое psycopg2-соединение (иначе берётся dao.get_conn());
                 commit/rollback делает writer
        replace: True — существующий регистр с тем же code каскадно
                 удаляется и записывается заново (в той же транзакции);
                 False — конфликт по code = ошибка

    Returns:
        {"register_id": int,
         "source_ids": {source_code: id},
         "union_ids": {union_code: id},
         "target_ids": {"schema.table": id},
         "mapping_count": int}
    """
    assert_valid(spec)
    schema = _check_schema(schema)

    own_conn = conn is None
    if own_conn:
        import dao
        conn = dao.get_conn()

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 0. Конфликт по code
            cur.execute(f"SELECT id FROM {schema}.registers WHERE code=%s", [spec.code])
            existing = cur.fetchone()
            if existing:
                if not replace:
                    raise SpecWriteError(
                        f"Регистр code='{spec.code}' уже существует в {schema} "
                        f"(id={existing['id']}); используйте replace=True"
                    )
                _delete_register_tx(cur, schema, existing["id"])

            # 1. Регистр
            cur.execute(f"""
                INSERT INTO {schema}.registers
                    (code, name, description, default_mode,
                     retail_table, retail_uid_column, recorder_type_map, is_active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, [
                spec.code, spec.name, spec.description, spec.default_mode,
                spec.retail_table, spec.retail_uid_column,
                _jsonb(spec.recorder_type_map), spec.is_active,
            ])
            register_id = cur.fetchone()["id"]

            # 2. Источники: сначала без parent (родители), затем с parent —
            #    чтобы parent_source_id всегда резолвился по уже вставленным.
            source_ids: Dict[str, int] = {}
            ordered = sorted(spec.sources, key=lambda s: bool(s.parent_source_code))
            for s in ordered:
                parent_id = None
                if s.parent_source_code:
                    parent_id = source_ids.get(s.parent_source_code)
                    if parent_id is None:
                        # валидатор это исключает, но защищаемся от рассинхрона
                        raise SpecWriteError(
                            f"source '{s.source_code}': parent '{s.parent_source_code}' ещё не вставлен"
                        )
                cur.execute(f"""
                    INSERT INTO {schema}.register_sources
                        (register_id, source_code, source_type, mssql_schema, mssql_table,
                         onec_name, parent_source_id, join_type, join_key_source, join_key_parent,
                         where_clause, priority, period_column, fields_cache, is_active)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                """, [
                    register_id, s.source_code, s.source_type, s.mssql_schema, s.mssql_table,
                    s.onec_name, parent_id, s.join_type, s.join_key_source, s.join_key_parent,
                    s.where_clause, s.priority,
                    # None (без фильтра) хранится как '' — NULL зарезервирован
                    # под legacy-default '_Period'
                    "" if s.period_column is None else s.period_column,
                    _jsonb(s.fields_cache), s.is_active,
                ])
                source_ids[s.source_code] = cur.fetchone()["id"]

                # 3. Маппинги источника
                for c in s.columns:
                    cur.execute(f"""
                        INSERT INTO {schema}.column_mappings
                            (source_id, source_column, target_column, is_expression,
                             target_type, transform_type, transform_params,
                             default_value, is_nullable, onec_name, is_active)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, [
                        source_ids[s.source_code], c.source_column, c.target_column,
                        c.is_expression, c.target_type, c.transform_type,
                        _jsonb(c.transform_params), c.default_value,
                        c.is_nullable, c.onec_name, c.is_active,
                    ])

            # 4. UNION-ы + члены
            union_ids: Dict[str, int] = {}
            for u in spec.unions:
                cur.execute(f"""
                    INSERT INTO {schema}.source_unions
                        (register_id, union_code, description, output_columns, is_active)
                    VALUES (%s,%s,%s,%s,%s)
                    RETURNING id
                """, [register_id, u.union_code, u.description, u.output_columns, u.is_active])
                union_ids[u.union_code] = cur.fetchone()["id"]

                for m in u.members:
                    cur.execute(f"""
                        INSERT INTO {schema}.source_union_members
                            (union_id, source_id, priority, where_clause, is_active)
                        VALUES (%s,%s,%s,%s,%s)
                    """, [
                        union_ids[u.union_code], source_ids[m.source_code],
                        m.priority, m.where_clause, m.is_active,
                    ])

            # 5. Targets
            target_ids: Dict[str, int] = {}
            for t in spec.targets:
                cur.execute(f"""
                    INSERT INTO {schema}.register_targets
                        (register_id, target_schema, target_table, union_id, source_id,
                         load_mode, upsert_keys, pre_load_sql, post_load_sql,
                         include_columns, priority, target_role, is_active)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                """, [
                    register_id, t.target_schema, t.target_table,
                    union_ids[t.union_code] if t.union_code else None,
                    source_ids[t.source_code] if t.source_code else None,
                    t.load_mode, t.upsert_keys or None,
                    t.pre_load_sql, t.post_load_sql,
                    t.include_columns or None, t.priority, t.target_role, t.is_active,
                ])
                target_ids[f"{t.target_schema}.{t.target_table}"] = cur.fetchone()["id"]

        conn.commit()
        return {
            "register_id": register_id,
            "source_ids": source_ids,
            "union_ids": union_ids,
            "target_ids": target_ids,
            "mapping_count": sum(len(s.columns) for s in spec.sources),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()
