"""
Негативные тесты валидатора контракта (register_spec.validate_spec).
Запуск: python3 etl_config_app/tests/validator_test.py
"""

import os
import sys

_APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

from register_spec import (  # noqa: E402
    RegisterSpec, SourceSpec, ColumnSpec, TargetSpec, UnionSpec, UnionMemberSpec,
    validate_spec,
)

_failures = []


def check(name, ok):
    print(("✅" if ok else "❌"), name)
    if not ok:
        _failures.append(name)


def base_spec(**overrides) -> RegisterSpec:
    """Минимальный валидный spec: 1 standalone источник → 1 upsert-target."""
    data = dict(
        code="t", name="t", default_mode="full_period",
        sources=[SourceSpec(
            source_code="s1", source_type="standalone", mssql_table="_AccumRg1",
            columns=[
                ColumnSpec(source_column="_RecorderRRef", target_column="recorder",
                           target_type="uuid", transform_type="binary_to_uuid"),
                ColumnSpec(source_column="_LineNo", target_column="line_no",
                           target_type="integer"),
            ],
        )],
        targets=[TargetSpec(
            target_table="t", source_code="s1",
            load_mode="upsert", upsert_keys=["recorder"],
        )],
    )
    data.update(overrides)
    return RegisterSpec(**data)


def errs(spec):
    return validate_spec(spec)


# 0. базовый spec валиден
check("базовый spec валиден", errs(base_spec()) == [])

# 1. join_type нормализуется: 'INNER JOIN' → 'INNER' (фикс «JOIN JOIN»)
s = SourceSpec(source_code="x", source_type="detail", mssql_table="_D1_VT1",
               parent_source_code="s1", join_type="INNER JOIN",
               join_key_source="_D1_IDRRef", join_key_parent="_IDRRef")
check("join_type 'INNER JOIN' нормализован в 'INNER'", s.join_type == "INNER")

s2 = SourceSpec(source_code="x", source_type="standalone", mssql_table="_T", join_type="CROSS")
spec = base_spec()
spec.sources.append(s2)
check("join_type 'CROSS' отклонён", any("join_type" in e for e in errs(spec)))

# 2. upsert без ключей
spec = base_spec()
spec.targets[0].upsert_keys = []
check("upsert без upsert_keys отклонён", any("upsert_keys" in e for e in errs(spec)))

# 3. upsert_keys вне колонок target
spec = base_spec()
spec.targets[0].upsert_keys = ["no_such_col"]
check("upsert_keys вне колонок отклонён", any("no_such_col" in e for e in errs(spec)))

# 4. include_columns вне маппингов
spec = base_spec()
spec.targets[0].include_columns = ["recorder", "ghost"]
check("include_columns с фантомной колонкой отклонён", any("ghost" in e for e in errs(spec)))

# 5. системные колонки разрешены в include_columns
spec = base_spec()
spec.targets[0].include_columns = ["recorder", "updated_at", "id"]
check("системные колонки в include_columns разрешены", errs(spec) == [])

# 6. source XOR union
spec = base_spec()
spec.targets[0].union_code = "u1"  # и source_code, и union_code
check("target с source И union отклонён", any("ровно один" in e for e in errs(spec)))

spec = base_spec()
spec.targets[0].source_code = None  # ни source, ни union
check("target без source и union отклонён", any("ровно один" in e for e in errs(spec)))

# 7. incremental без retail
spec = base_spec(default_mode="incremental")
spec.targets[0].target_role = "dimension"
spec.targets[0].include_columns = ["recorder", "updated_at"]
e = errs(spec)
check("incremental без retail_table отклонён", any("retail_table" in x for x in e))

# 8. incremental: dim без updated_at в include_columns
spec = base_spec(default_mode="incremental", retail_table="sales", retail_uid_column="uid")
spec.targets[0].target_role = "dimension"
spec.targets[0].include_columns = ["recorder", "line_no"]
check("incremental dim без updated_at отклонён", any("updated_at" in x for x in errs(spec)))

spec.targets[0].include_columns = ["recorder", "line_no", "updated_at"]
check("incremental dim с updated_at валиден", errs(spec) == [])

# 9. detail без parent / без ключей JOIN
spec = base_spec()
spec.sources.append(SourceSpec(source_code="d1", source_type="detail", mssql_table="_VT"))
e = errs(spec)
check("detail без parent отклонён", any("parent_source_code" in x for x in e))
check("detail без join-ключей отклонён", any("join_key" in x for x in e))

# 10. union: member на несуществующий source, пустые output_columns
spec = base_spec()
spec.unions.append(UnionSpec(union_code="u1", output_columns=[],
                             members=[UnionMemberSpec(source_code="nope")]))
e = errs(spec)
check("union с пустыми output_columns отклонён", any("output_columns" in x for x in e))
check("union member на несуществующий source отклонён", any("nope" in x for x in e))

# 11. дубли source_code
spec = base_spec()
spec.sources.append(SourceSpec(source_code="s1", source_type="standalone", mssql_table="_T2"))
check("дубль source_code отклонён", any("дубли source_code" in x for x in errs(spec)))

print()
if _failures:
    print(f"FAILED: {_failures}")
    sys.exit(1)
print("VALIDATOR TESTS PASSED ✅")
