---
tags: [баг, discover, etl-config]
date: 2026-04-10
---
# Discover падает с duplicate key при повторном запуске

## Ошибка
```
duplicate key value violates unique constraint "idx_register_source_code"
DETAIL: Key (register_id, source_code)=(17, doc_415) already exists.
```

## Причина
`batch_create_document_sources` в dao.py не проверяет существующие source_code перед INSERT.

## Статус
TODO — нужен idempotent batch_create: проверять существование перед вставкой, пропускать уже созданные.

## Решение (план)
В `batch_create_document_sources`:
```python
existing = list_sources_for_register(register_id)
existing_codes = {s['source_code'] for s in existing}
for dt in doc_types:
    code = f"doc_{dt['type_int']}"
    if code in existing_codes:
        continue  # skip
    ...
```

## Ссылки
- [[Текущие приоритеты]]
- [[ETL Config App управляет конфигурацией на порту 5555]]
