"""
Dataclasses для конфигурации ETL-системы.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class ColumnMapping:
    """Маппинг одной колонки source -> target."""
    source_column: str
    target_column: str
    is_expression: bool = False
    target_type: Optional[str] = None
    transform_type: Optional[str] = None
    transform_params: Optional[Dict[str, Any]] = None
    default_value: Optional[str] = None
    is_nullable: bool = True


@dataclass
class SourceConfig:
    """Конфигурация одного источника данных (таблицы 1С)."""
    id: int
    source_code: str
    source_type: str  # 'header', 'detail', 'standalone'
    mssql_schema: str
    mssql_table: str

    # JOIN config (для detail таблиц)
    parent_source_id: Optional[int] = None
    join_type: Optional[str] = None
    join_key_source: Optional[str] = None
    join_key_parent: Optional[str] = None

    where_clause: Optional[str] = None
    priority: int = 0

    columns: List[ColumnMapping] = field(default_factory=list)

    # Ссылка на родительский source (заполняется при загрузке)
    parent_source: Optional["SourceConfig"] = field(default=None, repr=False)


@dataclass
class UnionMember:
    """Член UNION-а."""
    source_id: int
    priority: int = 0
    where_clause: Optional[str] = None

    # Ссылка на source (заполняется при загрузке)
    source: Optional[SourceConfig] = field(default=None, repr=False)


@dataclass
class UnionConfig:
    """Конфигурация UNION ALL нескольких источников."""
    id: int
    union_code: str
    output_columns: List[str]
    description: Optional[str] = None

    members: List[UnionMember] = field(default_factory=list)


@dataclass
class TargetConfig:
    """Конфигурация целевой таблицы."""
    id: int
    target_schema: str
    target_table: str
    load_mode: str  # 'insert', 'upsert', 'replace'
    upsert_keys: List[str] = field(default_factory=list)
    pre_load_sql: Optional[str] = None
    is_active: bool = True

    # Источник данных (один из двух)
    union_id: Optional[int] = None
    source_id: Optional[int] = None

    # Ссылки (заполняются при загрузке)
    union_config: Optional[UnionConfig] = field(default=None, repr=False)
    source_config: Optional[SourceConfig] = field(default=None, repr=False)

    @property
    def full_table_name(self) -> str:
        """Полное имя таблицы schema.table."""
        return f"{self.target_schema}.{self.target_table}"


@dataclass
class RegisterConfig:
    """Полная конфигурация регистра."""
    id: int
    code: str
    name: str
    default_mode: str
    description: Optional[str] = None

    # Для инкрементальной загрузки
    retail_table: Optional[str] = None
    retail_uid_column: Optional[str] = None

    # Компоненты
    sources: List[SourceConfig] = field(default_factory=list)
    unions: List[UnionConfig] = field(default_factory=list)
    targets: List[TargetConfig] = field(default_factory=list)

    def get_source_by_id(self, source_id: int) -> Optional[SourceConfig]:
        """Найти источник по ID."""
        return next((s for s in self.sources if s.id == source_id), None)

    def get_source_by_code(self, source_code: str) -> Optional[SourceConfig]:
        """Найти источник по коду."""
        return next((s for s in self.sources if s.source_code == source_code), None)

    def get_union_by_id(self, union_id: int) -> Optional[UnionConfig]:
        """Найти UNION по ID."""
        return next((u for u in self.unions if u.id == union_id), None)

    def get_target_by_table(self, table_name: str) -> Optional[TargetConfig]:
        """Найти target по имени таблицы."""
        return next((t for t in self.targets if t.target_table == table_name), None)
