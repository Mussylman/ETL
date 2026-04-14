"""
Data Access Layer для etl_meta схемы в PostgreSQL.
Все CRUD операции через PostgresHook.
"""

import json
from typing import List, Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook


class EtlMetaDAO:
    CONN_ID = "postgre_test_base"
    SCHEMA = "etl_meta"

    def _hook(self):
        return PostgresHook(postgres_conn_id=self.CONN_ID)

    def _query(self, sql: str, params=None) -> List[dict]:
        """SELECT → list of dicts."""
        hook = self._hook()
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params or [])
                if cur.description is None:
                    return []
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    def _query_one(self, sql: str, params=None) -> Optional[dict]:
        rows = self._query(sql, params)
        return rows[0] if rows else None

    def _execute(self, sql: str, params=None):
        """INSERT/UPDATE/DELETE."""
        hook = self._hook()
        hook.run(sql, parameters=params, autocommit=True)

    def _insert_returning(self, sql: str, params=None) -> int:
        """INSERT RETURNING id."""
        row = self._query_one(sql, params)
        return row["id"] if row else 0

    # ================================================================
    #  REGISTERS
    # ================================================================

    def list_registers(self, include_inactive=False) -> List[dict]:
        sql = f"""
            SELECT id, code, name, description, default_mode,
                   retail_table, retail_uid_column, is_active,
                   created_at, updated_at
            FROM {self.SCHEMA}.registers
            {"" if include_inactive else "WHERE is_active = TRUE"}
            ORDER BY code
        """
        return self._query(sql)

    def get_register(self, register_id: int) -> Optional[dict]:
        sql = f"""
            SELECT id, code, name, description, default_mode,
                   retail_table, retail_uid_column, is_active,
                   created_at, updated_at
            FROM {self.SCHEMA}.registers WHERE id = %s
        """
        return self._query_one(sql, [register_id])

    def create_register(self, data: dict) -> int:
        sql = f"""
            INSERT INTO {self.SCHEMA}.registers
                (code, name, description, default_mode, retail_table, retail_uid_column)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        return self._insert_returning(sql, [
            data["code"], data["name"], data.get("description"),
            data.get("default_mode", "incremental"),
            data.get("retail_table"), data.get("retail_uid_column"),
        ])

    def update_register(self, register_id: int, data: dict):
        sql = f"""
            UPDATE {self.SCHEMA}.registers SET
                code = %s, name = %s, description = %s, default_mode = %s,
                retail_table = %s, retail_uid_column = %s,
                updated_at = NOW()
            WHERE id = %s
        """
        self._execute(sql, [
            data["code"], data["name"], data.get("description"),
            data.get("default_mode", "incremental"),
            data.get("retail_table"), data.get("retail_uid_column"),
            register_id,
        ])

    def delete_register(self, register_id: int):
        self._execute(
            f"DELETE FROM {self.SCHEMA}.registers WHERE id = %s",
            [register_id],
        )

    def toggle_register(self, register_id: int, is_active: bool):
        self._execute(
            f"UPDATE {self.SCHEMA}.registers SET is_active = %s, updated_at = NOW() WHERE id = %s",
            [is_active, register_id],
        )

    # ================================================================
    #  SOURCES
    # ================================================================

    def list_sources_for_register(self, register_id: int) -> List[dict]:
        sql = f"""
            SELECT s.*, ps.source_code as parent_source_code,
                   (SELECT COUNT(*) FROM {self.SCHEMA}.column_mappings cm
                    WHERE cm.source_id = s.id AND cm.is_active = TRUE) as column_count
            FROM {self.SCHEMA}.register_sources s
            LEFT JOIN {self.SCHEMA}.register_sources ps ON ps.id = s.parent_source_id
            WHERE s.register_id = %s
            ORDER BY s.priority, s.id
        """
        return self._query(sql, [register_id])

    def get_source(self, source_id: int) -> Optional[dict]:
        sql = f"""
            SELECT s.*, r.code as register_code, r.id as register_id,
                   ps.source_code as parent_source_code
            FROM {self.SCHEMA}.register_sources s
            JOIN {self.SCHEMA}.registers r ON r.id = s.register_id
            LEFT JOIN {self.SCHEMA}.register_sources ps ON ps.id = s.parent_source_id
            WHERE s.id = %s
        """
        return self._query_one(sql, [source_id])

    def create_source(self, data: dict) -> int:
        sql = f"""
            INSERT INTO {self.SCHEMA}.register_sources
                (register_id, source_code, source_type, mssql_schema, mssql_table,
                 parent_source_id, join_type, join_key_source, join_key_parent,
                 where_clause, priority)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        return self._insert_returning(sql, [
            data["register_id"], data["source_code"], data["source_type"],
            data.get("mssql_schema", "dbo"), data["mssql_table"],
            data.get("parent_source_id") or None,
            data.get("join_type") or None,
            data.get("join_key_source") or None,
            data.get("join_key_parent") or None,
            data.get("where_clause") or None,
            data.get("priority", 0),
        ])

    def update_source(self, source_id: int, data: dict):
        sql = f"""
            UPDATE {self.SCHEMA}.register_sources SET
                source_code = %s, source_type = %s,
                mssql_schema = %s, mssql_table = %s,
                parent_source_id = %s, join_type = %s,
                join_key_source = %s, join_key_parent = %s,
                where_clause = %s, priority = %s
            WHERE id = %s
        """
        self._execute(sql, [
            data["source_code"], data["source_type"],
            data.get("mssql_schema", "dbo"), data["mssql_table"],
            data.get("parent_source_id") or None,
            data.get("join_type") or None,
            data.get("join_key_source") or None,
            data.get("join_key_parent") or None,
            data.get("where_clause") or None,
            data.get("priority", 0),
            source_id,
        ])

    def delete_source(self, source_id: int):
        self._execute(
            f"DELETE FROM {self.SCHEMA}.register_sources WHERE id = %s",
            [source_id],
        )

    # ================================================================
    #  COLUMN MAPPINGS
    # ================================================================

    def list_mappings_for_source(self, source_id: int) -> List[dict]:
        sql = f"""
            SELECT * FROM {self.SCHEMA}.column_mappings
            WHERE source_id = %s
            ORDER BY id
        """
        return self._query(sql, [source_id])

    def get_mapping(self, mapping_id: int) -> Optional[dict]:
        sql = f"""
            SELECT cm.*, s.source_code, s.register_id
            FROM {self.SCHEMA}.column_mappings cm
            JOIN {self.SCHEMA}.register_sources s ON s.id = cm.source_id
            WHERE cm.id = %s
        """
        return self._query_one(sql, [mapping_id])

    def create_mapping(self, data: dict) -> int:
        transform_params = data.get("transform_params")
        if isinstance(transform_params, dict):
            transform_params = json.dumps(transform_params)
        elif isinstance(transform_params, str) and transform_params.strip():
            json.loads(transform_params)  # validate
        else:
            transform_params = None

        sql = f"""
            INSERT INTO {self.SCHEMA}.column_mappings
                (source_id, source_column, target_column, is_expression,
                 target_type, transform_type, transform_params,
                 default_value, is_nullable)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        return self._insert_returning(sql, [
            data["source_id"], data["source_column"], data["target_column"],
            data.get("is_expression", False),
            data.get("target_type") or None,
            data.get("transform_type") or None,
            transform_params,
            data.get("default_value") or None,
            data.get("is_nullable", True),
        ])

    def create_mappings_batch(self, source_id: int, mappings: List[dict]):
        """Batch-создание маппингов (при загрузке колонок из API 1С)."""
        for m in mappings:
            m["source_id"] = source_id
            self.create_mapping(m)

    def update_mapping(self, mapping_id: int, data: dict):
        transform_params = data.get("transform_params")
        if isinstance(transform_params, dict):
            transform_params = json.dumps(transform_params)
        elif isinstance(transform_params, str) and transform_params.strip():
            json.loads(transform_params)  # validate
        else:
            transform_params = None

        sql = f"""
            UPDATE {self.SCHEMA}.column_mappings SET
                source_column = %s, target_column = %s, is_expression = %s,
                target_type = %s, transform_type = %s, transform_params = %s,
                default_value = %s, is_nullable = %s
            WHERE id = %s
        """
        self._execute(sql, [
            data["source_column"], data["target_column"],
            data.get("is_expression", False),
            data.get("target_type") or None,
            data.get("transform_type") or None,
            transform_params,
            data.get("default_value") or None,
            data.get("is_nullable", True),
            mapping_id,
        ])

    def delete_mapping(self, mapping_id: int):
        self._execute(
            f"DELETE FROM {self.SCHEMA}.column_mappings WHERE id = %s",
            [mapping_id],
        )

    # ================================================================
    #  UNIONS
    # ================================================================

    def list_unions_for_register(self, register_id: int) -> List[dict]:
        sql = f"""
            SELECT u.*,
                   (SELECT COUNT(*) FROM {self.SCHEMA}.source_union_members m
                    WHERE m.union_id = u.id AND m.is_active = TRUE) as member_count
            FROM {self.SCHEMA}.source_unions u
            WHERE u.register_id = %s
            ORDER BY u.id
        """
        return self._query(sql, [register_id])

    def get_union(self, union_id: int) -> Optional[dict]:
        sql = f"""
            SELECT u.*, r.code as register_code, r.id as register_id
            FROM {self.SCHEMA}.source_unions u
            JOIN {self.SCHEMA}.registers r ON r.id = u.register_id
            WHERE u.id = %s
        """
        return self._query_one(sql, [union_id])

    def create_union(self, data: dict) -> int:
        output_columns = data.get("output_columns", [])
        if isinstance(output_columns, str):
            output_columns = [c.strip() for c in output_columns.split(",") if c.strip()]

        sql = f"""
            INSERT INTO {self.SCHEMA}.source_unions
                (register_id, union_code, description, output_columns)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        return self._insert_returning(sql, [
            data["register_id"], data["union_code"],
            data.get("description") or None,
            output_columns,
        ])

    def update_union(self, union_id: int, data: dict):
        output_columns = data.get("output_columns", [])
        if isinstance(output_columns, str):
            output_columns = [c.strip() for c in output_columns.split(",") if c.strip()]

        sql = f"""
            UPDATE {self.SCHEMA}.source_unions SET
                union_code = %s, description = %s, output_columns = %s
            WHERE id = %s
        """
        self._execute(sql, [
            data["union_code"], data.get("description") or None,
            output_columns, union_id,
        ])

    def delete_union(self, union_id: int):
        self._execute(
            f"DELETE FROM {self.SCHEMA}.source_unions WHERE id = %s",
            [union_id],
        )

    # ================================================================
    #  UNION MEMBERS
    # ================================================================

    def list_members_for_union(self, union_id: int) -> List[dict]:
        sql = f"""
            SELECT m.*, s.source_code, s.mssql_table
            FROM {self.SCHEMA}.source_union_members m
            JOIN {self.SCHEMA}.register_sources s ON s.id = m.source_id
            WHERE m.union_id = %s
            ORDER BY m.priority, m.id
        """
        return self._query(sql, [union_id])

    def get_member(self, member_id: int) -> Optional[dict]:
        sql = f"""
            SELECT m.*, u.union_code, u.register_id
            FROM {self.SCHEMA}.source_union_members m
            JOIN {self.SCHEMA}.source_unions u ON u.id = m.union_id
            WHERE m.id = %s
        """
        return self._query_one(sql, [member_id])

    def create_member(self, data: dict) -> int:
        sql = f"""
            INSERT INTO {self.SCHEMA}.source_union_members
                (union_id, source_id, priority, where_clause)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        return self._insert_returning(sql, [
            data["union_id"], data["source_id"],
            data.get("priority", 0), data.get("where_clause") or None,
        ])

    def update_member(self, member_id: int, data: dict):
        sql = f"""
            UPDATE {self.SCHEMA}.source_union_members SET
                source_id = %s, priority = %s, where_clause = %s
            WHERE id = %s
        """
        self._execute(sql, [
            data["source_id"], data.get("priority", 0),
            data.get("where_clause") or None, member_id,
        ])

    def delete_member(self, member_id: int):
        self._execute(
            f"DELETE FROM {self.SCHEMA}.source_union_members WHERE id = %s",
            [member_id],
        )

    # ================================================================
    #  TARGETS
    # ================================================================

    def list_targets_for_register(self, register_id: int) -> List[dict]:
        sql = f"""
            SELECT t.*,
                   u.union_code,
                   s.source_code
            FROM {self.SCHEMA}.register_targets t
            LEFT JOIN {self.SCHEMA}.source_unions u ON u.id = t.union_id
            LEFT JOIN {self.SCHEMA}.register_sources s ON s.id = t.source_id
            WHERE t.register_id = %s
            ORDER BY t.id
        """
        return self._query(sql, [register_id])

    def get_target(self, target_id: int) -> Optional[dict]:
        sql = f"""
            SELECT t.*, r.code as register_code, r.id as register_id
            FROM {self.SCHEMA}.register_targets t
            JOIN {self.SCHEMA}.registers r ON r.id = t.register_id
            WHERE t.id = %s
        """
        return self._query_one(sql, [target_id])

    def create_target(self, data: dict) -> int:
        upsert_keys = data.get("upsert_keys", [])
        if isinstance(upsert_keys, str):
            upsert_keys = [k.strip() for k in upsert_keys.split(",") if k.strip()]

        sql = f"""
            INSERT INTO {self.SCHEMA}.register_targets
                (register_id, target_schema, target_table,
                 union_id, source_id, load_mode, upsert_keys, pre_load_sql)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        return self._insert_returning(sql, [
            data["register_id"],
            data.get("target_schema", "public"), data["target_table"],
            data.get("union_id") or None, data.get("source_id") or None,
            data.get("load_mode", "upsert"),
            upsert_keys, data.get("pre_load_sql") or None,
        ])

    def update_target(self, target_id: int, data: dict):
        upsert_keys = data.get("upsert_keys", [])
        if isinstance(upsert_keys, str):
            upsert_keys = [k.strip() for k in upsert_keys.split(",") if k.strip()]

        sql = f"""
            UPDATE {self.SCHEMA}.register_targets SET
                target_schema = %s, target_table = %s,
                union_id = %s, source_id = %s,
                load_mode = %s, upsert_keys = %s, pre_load_sql = %s
            WHERE id = %s
        """
        self._execute(sql, [
            data.get("target_schema", "public"), data["target_table"],
            data.get("union_id") or None, data.get("source_id") or None,
            data.get("load_mode", "upsert"),
            upsert_keys, data.get("pre_load_sql") or None,
            target_id,
        ])

    def delete_target(self, target_id: int):
        self._execute(
            f"DELETE FROM {self.SCHEMA}.register_targets WHERE id = %s",
            [target_id],
        )

    # ================================================================
    #  LOAD HISTORY
    # ================================================================

    def list_load_history(self, register_id: int, limit: int = 20) -> List[dict]:
        sql = f"""
            SELECT h.*, t.target_table
            FROM {self.SCHEMA}.load_history h
            LEFT JOIN {self.SCHEMA}.register_targets t ON t.id = h.target_id
            WHERE h.register_id = %s
            ORDER BY h.started_at DESC
            LIMIT %s
        """
        return self._query(sql, [register_id, limit])
