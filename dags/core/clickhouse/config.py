"""
Чтение конфигурации синхронизации из PostgreSQL control plane.

Таблиц в коде нет: всё приходит из etl_meta.ch_sync + etl_meta.ch_sync_columns.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Column:
    ordinal: int
    source_expr: str
    target_column: str
    target_type: str
    codec: Optional[str] = None

    def ddl(self) -> str:
        c = f" CODEC({self.codec})" if self.codec else ""
        return f"    {self.target_column:<22} {self.target_type}{c}"


@dataclass
class SyncSpec:
    id: int
    code: str
    description: Optional[str]

    source_conn_id: str
    source_type: str                      # postgres | mssql
    source_schema: Optional[str]
    source_object: Optional[str]
    source_query: Optional[str]

    target_database: str
    target_table: str
    partition_expr: str                   # выражение ClickHouse, на источнике не выполняется
    order_by: List[str]

    load_mode: str                        # full | partitioned
    partition_column: Optional[str]
    partition_granularity: Optional[str]  # month | day
    watermark_column: Optional[str]
    business_key: List[str]
    batch_size: int
    empty_partition_policy: str           # fail | clear

    hot_window: int
    sweep_interval_min: int

    checksum_columns: List[str]
    measure_columns: List[str]
    reconcile_metrics: dict
    lookup: dict
    source_params: dict
    priority: int = 100

    columns: List[Column] = field(default_factory=list)

    # --- производные имена -------------------------------------------------
    @property
    def fqn(self) -> str:
        return f"{self.target_database}.{self.target_table}"

    @property
    def stage_fqn(self) -> str:
        return f"{self.target_database}.{self.target_table}_stage"

    @property
    def target_columns(self) -> List[str]:
        return [c.target_column for c in self.columns]

    # Колонки, приходящие из lookup, в источнике отсутствуют: их значение
    # подставляется join'ом уже внутри ClickHouse, а не стримится.
    LOOKUP_MARK = "@lookup"

    @property
    def stream_columns(self) -> List["Column"]:
        return [c for c in self.columns if not c.source_expr.startswith(self.LOOKUP_MARK)]

    @property
    def needs_raw(self) -> bool:
        """Сырой staging нужен для lookup и для патча документов."""
        return bool(self.lookup) or self.load_mode == "document_patch"

    @property
    def raw_fqn(self) -> str:
        return f"{self.target_database}.{self.target_table}_raw"

    def ddl_raw(self) -> str:
        """Сырой staging: ровно то, что приходит из источника, без обогащённых колонок."""
        cols = ",\n".join(c.ddl() for c in self.stream_columns)
        return (f"CREATE TABLE IF NOT EXISTS {self.raw_fqn}\n(\n{cols}\n)\n"
                f"ENGINE = MergeTree\nPARTITION BY {self.partition_expr}\n"
                f"ORDER BY ({', '.join(c.target_column for c in self.stream_columns[:1])})")

    @property
    def is_partitioned(self) -> bool:
        return self.load_mode == "partitioned"

    def ddl(self, table: str) -> str:
        """CREATE TABLE из конфигурации: колонки, движок, партиционирование, порядок."""
        cols = ",\n".join(c.ddl() for c in self.columns)
        return (f"CREATE TABLE IF NOT EXISTS {table}\n(\n{cols}\n)\n"
                f"ENGINE = MergeTree\n"
                f"PARTITION BY {self.partition_expr}\n"
                f"ORDER BY ({', '.join(self.order_by)})")


_SELECT = """
    SELECT s.id, s.code, s.description, s.source_conn_id, s.source_type, s.source_schema,
           s.source_object, s.source_query, s.target_database, s.target_table,
           s.partition_expr, s.order_by, s.load_mode, s.partition_column,
           s.partition_granularity, s.watermark_column, s.business_key, s.batch_size,
           s.empty_partition_policy, s.hot_window, s.sweep_interval_min,
           s.checksum_columns, s.measure_columns, s.reconcile_metrics, s.lookup,
           s.source_params, s.priority
      FROM etl_meta.ch_sync s
"""


def _build(pg, row) -> SyncSpec:
    spec = SyncSpec(
        id=row[0], code=row[1], description=row[2],
        source_conn_id=row[3], source_type=row[4], source_schema=row[5],
        source_object=row[6], source_query=row[7],
        target_database=row[8], target_table=row[9],
        partition_expr=row[10], order_by=list(row[11] or []),
        load_mode=row[12], partition_column=row[13], partition_granularity=row[14],
        watermark_column=row[15], business_key=list(row[16] or []), batch_size=row[17],
        empty_partition_policy=row[18], hot_window=row[19], sweep_interval_min=row[20],
        checksum_columns=list(row[21] or []), measure_columns=list(row[22] or []),
        reconcile_metrics=row[23] or {},
        lookup=row[24] or {},
        source_params=row[25] or {},
        priority=row[26] if row[26] is not None else 100,
    )
    spec.columns = [
        Column(ordinal=c[0], source_expr=c[1], target_column=c[2], target_type=c[3], codec=c[4])
        for c in pg.get_records(
            "SELECT ordinal, source_expr, target_column, target_type, codec "
            "FROM etl_meta.ch_sync_columns WHERE sync_id = %s ORDER BY ordinal",
            parameters=(spec.id,))
    ]
    if not spec.columns:
        raise RuntimeError(f"{spec.code}: в etl_meta.ch_sync_columns нет ни одной колонки")
    return spec


def load_spec(pg, code: str) -> SyncSpec:
    rows = pg.get_records(_SELECT + " WHERE s.code = %s", parameters=(code,))
    if not rows:
        raise RuntimeError(f"в etl_meta.ch_sync нет конфигурации с code={code!r}")
    return _build(pg, rows[0])


def load_active_specs(pg) -> List[SyncSpec]:
    """
    Активные конфигурации обобщённых источников (postgres / mssql) — для второго hop'а
    и инструментов, которые умеют только их (ch_report, ch_sync --all, ch_ddl --all).
    Регистры 1С (onec_register) сюда не входят: их оркеструет только runner по группам
    (load_group_specs) — одним извлечением на регистр, со своей сверкой с 1С.
    """
    rows = pg.get_records(_SELECT + " WHERE s.is_active AND s.source_type <> 'onec_register' "
                                    "ORDER BY s.priority, s.code")
    return [_build(pg, r) for r in rows]


def load_group_specs(pg, group: str, include_inactive: bool = False) -> List[SyncSpec]:
    """Конфигурации группы в порядке priority. Группа — единица оркестрации."""
    cond = " WHERE s.sync_group = %s" + ("" if include_inactive else " AND s.is_active")
    rows = pg.get_records(_SELECT + cond + " ORDER BY s.priority, s.code", parameters=(group,))
    return [_build(pg, r) for r in rows]
