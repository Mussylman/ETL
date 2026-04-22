---
tags: [баг, discover, etl-config, решено]
date: 2026-04-10
resolved: 2026-04-22
---
# Discover падает с duplicate key при повторном запуске

## Статус
**РЕШЕНО** (подтверждено 2026-04-22 по коду).

## Ошибка (была)
```
duplicate key value violates unique constraint "idx_register_source_code"
DETAIL: Key (register_id, source_code)=(17, doc_415) already exists.
```

## Причина
`batch_create_document_sources` в dao.py не проверяла существующие source_code перед INSERT.

## Решение
`batch_create_document_sources` теперь идемпотентна. В `etl_config_app/dao.py:844-861`:
```python
existing_sources = {s["source_code"]: s for s in list_sources_for_register(register_id)}

for dt in doc_types:
    ...
    source_code = f"doc_{type_int}"
    if source_code in existing_sources:
        created_sources.append({..., "existed": True})
        continue
    ...
```

## Ссылки
- [[Текущие приоритеты]]
- [[ETL Config App управляет конфигурацией на порту 5555]]
