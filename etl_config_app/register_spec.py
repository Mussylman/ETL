"""
RegisterSpec — канонический декларативный config-model регистра.

Зеркало контракта движка (то, что реально читает dags/core/config/config_loader.py)
+ UI-поля конфигуратора (onec_name, fields_cache, recorder_type_map).

Принципы:
  • Все внутренние ссылки — по code (source_code/union_code), не по id:
    spec самодостаточен и переносим между схемами (etl_meta / etl_test).
  • Spec собирается целиком и пишется одним вызовом spec_writer.write().
    Никакой записи по шагам.
  • validate_spec() — единый валидатор контракта, вызывается до любой записи.
"""

from typing import List, Optional, Literal, Dict, Any
from pydantic import BaseModel, Field, field_validator

# Колонки, которые существуют в целевой таблице помимо маппингов
# (создаются Sync'ом / движком, см. dao.compute_sync_plan и ETLEngine).
SYSTEM_COLUMNS = {"id", "etl_loaded_at", "etl_hash", "updated_at"}

VALID_JOIN_TYPES = {"INNER", "LEFT", "RIGHT"}
VALID_SOURCE_TYPES = {"header", "detail", "standalone"}
VALID_LOAD_MODES = {"insert", "upsert", "replace", "delete_insert_by_recorder"}
# 'full' — режим полной выгрузки без периода (этап 0.2, справочники)
VALID_DEFAULT_MODES = {"full_period", "incremental", "consistency", "full"}
VALID_TARGET_ROLES = {"dimension", "fact"}


def _normalize_join_type(value: Optional[str]) -> Optional[str]:
    """
    'INNER JOIN' / 'inner' / ' LEFT  JOIN ' → 'INNER' / 'INNER' / 'LEFT'.

    Исторический баг: wizard писал join_type='INNER JOIN', а QueryBuilder
    добавляет ' JOIN' сам → 'INNER JOIN JOIN'. Spec хранит только чистый тип.
    """
    if value is None:
        return None
    v = " ".join(str(value).upper().split())
    if v.endswith(" JOIN"):
        v = v[: -len(" JOIN")].strip()
    return v or None


class ColumnSpec(BaseModel):
    """Маппинг одной колонки source → target (etl_meta.column_mappings)."""
    source_column: str
    target_column: str
    is_expression: bool = False
    target_type: Optional[str] = None
    transform_type: Optional[str] = None
    transform_params: Optional[Dict[str, Any]] = None
    default_value: Optional[str] = None
    is_nullable: bool = True
    is_active: bool = True
    # UI-поле (движок не читает)
    onec_name: Optional[str] = None


class SourceSpec(BaseModel):
    """Источник данных 1С (etl_meta.register_sources)."""
    source_code: str
    source_type: str  # header / detail / standalone
    mssql_schema: str = "dbo"
    mssql_table: str
    parent_source_code: Optional[str] = None
    join_type: Optional[str] = None  # INNER / LEFT / RIGHT (нормализуется)
    join_key_source: Optional[str] = None
    join_key_parent: Optional[str] = None
    where_clause: Optional[str] = None
    priority: int = 0
    # Колонка периода: '_Period' (default), '_Date_Time' (документы),
    # None — без периодного фильтра (справочники). В БД None хранится как ''.
    period_column: Optional[str] = "_Period"
    is_active: bool = True
    # UI-поля (движок не читает)
    onec_name: Optional[str] = None
    fields_cache: Optional[List[Any]] = None

    columns: List[ColumnSpec] = Field(default_factory=list)

    @field_validator("join_type", mode="before")
    @classmethod
    def _norm_join(cls, v):
        return _normalize_join_type(v)


class UnionMemberSpec(BaseModel):
    """Член UNION-а (etl_meta.source_union_members), ссылка по source_code."""
    source_code: str
    priority: int = 0
    where_clause: Optional[str] = None
    is_active: bool = True


class UnionSpec(BaseModel):
    """UNION ALL нескольких источников (etl_meta.source_unions)."""
    union_code: str
    description: Optional[str] = None
    output_columns: List[str] = Field(default_factory=list)
    is_active: bool = True
    members: List[UnionMemberSpec] = Field(default_factory=list)


class TargetSpec(BaseModel):
    """Целевая таблица (etl_meta.register_targets); source XOR union по code."""
    target_schema: str = "public"
    target_table: str
    source_code: Optional[str] = None
    union_code: Optional[str] = None
    load_mode: str = "upsert"
    upsert_keys: List[str] = Field(default_factory=list)
    pre_load_sql: Optional[str] = None
    post_load_sql: Optional[str] = None
    include_columns: List[str] = Field(default_factory=list)
    priority: int = 0
    target_role: Optional[str] = None  # dimension / fact
    is_active: bool = True


class RegisterSpec(BaseModel):
    """Полное декларативное описание регистра."""
    code: str
    name: str
    description: Optional[str] = None
    default_mode: str = "full_period"
    retail_table: Optional[str] = None
    retail_uid_column: Optional[str] = None
    is_active: bool = True
    # UI-поле (движок читает только копию в transform_params маппинга)
    recorder_type_map: Optional[Dict[str, Any]] = None

    sources: List[SourceSpec] = Field(default_factory=list)
    unions: List[UnionSpec] = Field(default_factory=list)
    targets: List[TargetSpec] = Field(default_factory=list)

    def get_source(self, source_code: str) -> Optional[SourceSpec]:
        return next((s for s in self.sources if s.source_code == source_code), None)

    def get_union(self, union_code: str) -> Optional[UnionSpec]:
        return next((u for u in self.unions if u.union_code == union_code), None)


class SpecValidationError(ValueError):
    """Spec нарушает контракт движка. .errors — список нарушений."""

    def __init__(self, errors: List[str]):
        self.errors = errors
        super().__init__("Spec contract violations:\n" + "\n".join(f"  - {e}" for e in errors))


def _target_effective_columns(spec: RegisterSpec, target: TargetSpec) -> set:
    """
    Эффективные колонки target: target_column активных маппингов его
    источника(ов) ∪ SYSTEM_COLUMNS. Для union-target — output_columns union-а.
    """
    cols = set()
    if target.union_code:
        union = spec.get_union(target.union_code)
        if union:
            cols |= set(union.output_columns)
    elif target.source_code:
        source = spec.get_source(target.source_code)
        if source:
            for col in source.columns:
                if col.is_active:
                    cols.add(col.target_column)
            parent = spec.get_source(source.parent_source_code) if source.parent_source_code else None
            if parent:
                cols |= {c.target_column for c in parent.columns if c.is_active}
    return cols | SYSTEM_COLUMNS


def validate_spec(spec: RegisterSpec) -> List[str]:
    """
    Валидатор контракта. Возвращает список нарушений (пустой = spec валиден).
    Единый для всех путей записи — wizard'ы, формы, импорт.
    """
    errors: List[str] = []

    # --- регистр ---
    if spec.default_mode not in VALID_DEFAULT_MODES:
        errors.append(f"register: default_mode '{spec.default_mode}' ∉ {sorted(VALID_DEFAULT_MODES)}")
    if spec.default_mode == "incremental":
        if not spec.retail_table or not spec.retail_uid_column:
            errors.append("register: default_mode=incremental требует retail_table и retail_uid_column")

    # --- источники ---
    source_codes = [s.source_code for s in spec.sources]
    dupes = {c for c in source_codes if source_codes.count(c) > 1}
    if dupes:
        errors.append(f"sources: дубли source_code {sorted(dupes)}")

    for s in spec.sources:
        prefix = f"source '{s.source_code}'"
        if s.source_type not in VALID_SOURCE_TYPES:
            errors.append(f"{prefix}: source_type '{s.source_type}' ∉ {sorted(VALID_SOURCE_TYPES)}")
        if s.join_type is not None and s.join_type not in VALID_JOIN_TYPES:
            errors.append(f"{prefix}: join_type '{s.join_type}' ∉ {sorted(VALID_JOIN_TYPES)}")
        if s.parent_source_code:
            if s.parent_source_code not in source_codes:
                errors.append(f"{prefix}: parent_source_code '{s.parent_source_code}' не найден")
            elif s.parent_source_code == s.source_code:
                errors.append(f"{prefix}: parent_source_code ссылается сам на себя")
        if s.source_type == "detail":
            if not s.parent_source_code:
                errors.append(f"{prefix}: source_type=detail требует parent_source_code")
            if not s.join_key_source or not s.join_key_parent:
                errors.append(f"{prefix}: source_type=detail требует join_key_source и join_key_parent")
        # дубли target_column внутри источника
        tcols = [c.target_column for c in s.columns if c.is_active]
        cdupes = {c for c in tcols if tcols.count(c) > 1}
        if cdupes:
            errors.append(f"{prefix}: дубли target_column {sorted(cdupes)}")

    # --- union-ы ---
    union_codes = [u.union_code for u in spec.unions]
    udupes = {c for c in union_codes if union_codes.count(c) > 1}
    if udupes:
        errors.append(f"unions: дубли union_code {sorted(udupes)}")

    for u in spec.unions:
        prefix = f"union '{u.union_code}'"
        if not u.output_columns:
            errors.append(f"{prefix}: пустой output_columns")
        if not u.members:
            errors.append(f"{prefix}: нет members")
        for m in u.members:
            if m.source_code not in source_codes:
                errors.append(f"{prefix}: member ссылается на несуществующий source '{m.source_code}'")

    # --- targets ---
    tkeys = [f"{t.target_schema}.{t.target_table}" for t in spec.targets]
    tdupes = {k for k in tkeys if tkeys.count(k) > 1}
    if tdupes:
        errors.append(f"targets: дубли таблиц {sorted(tdupes)}")

    has_incremental = spec.default_mode == "incremental"

    for t in spec.targets:
        prefix = f"target '{t.target_schema}.{t.target_table}'"

        # source XOR union
        if bool(t.source_code) == bool(t.union_code):
            errors.append(f"{prefix}: должен быть ровно один из source_code / union_code")
        if t.source_code and t.source_code not in source_codes:
            errors.append(f"{prefix}: source_code '{t.source_code}' не найден")
        if t.union_code and t.union_code not in union_codes:
            errors.append(f"{prefix}: union_code '{t.union_code}' не найден")

        if t.load_mode not in VALID_LOAD_MODES:
            errors.append(f"{prefix}: load_mode '{t.load_mode}' ∉ {sorted(VALID_LOAD_MODES)}")
        if t.target_role is not None and t.target_role not in VALID_TARGET_ROLES:
            errors.append(f"{prefix}: target_role '{t.target_role}' ∉ {sorted(VALID_TARGET_ROLES)}")

        effective = _target_effective_columns(spec, t)

        # upsert ⇒ ключи непусты и ⊆ колонок target
        if t.load_mode == "upsert":
            if not t.upsert_keys:
                errors.append(f"{prefix}: load_mode=upsert требует upsert_keys")
            else:
                target_cols = (set(t.include_columns) | SYSTEM_COLUMNS) if t.include_columns else effective
                missing = set(t.upsert_keys) - target_cols
                if missing:
                    errors.append(f"{prefix}: upsert_keys {sorted(missing)} нет среди колонок target")

        # include_columns ⊆ (маппинги ∪ системные)
        if t.include_columns:
            missing = set(t.include_columns) - effective
            if missing:
                errors.append(f"{prefix}: include_columns {sorted(missing)} не порождаются маппингами")

        # incremental ⇒ у dim-target есть updated_at (источник watermark)
        if has_incremental and t.target_role == "dimension":
            if t.include_columns and "updated_at" not in t.include_columns:
                errors.append(f"{prefix}: incremental-регистр требует 'updated_at' в include_columns dim-target")

    return errors


def assert_valid(spec: RegisterSpec) -> None:
    """Бросает SpecValidationError, если spec нарушает контракт."""
    errors = validate_spec(spec)
    if errors:
        raise SpecValidationError(errors)
