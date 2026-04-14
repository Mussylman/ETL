---
tags: [паттерн, ETL, JOIN]
date: 2026-04-10
---
# Header-detail загрузка через JOIN

## Паттерн
Документ 1С = шапка + табличные части (VT). Пример:
- `_Document476` — ЧекККМ (шапка)
- `_Document476_VT13626` — ЧекККМ.Товары (строки)

## SQL
```sql
SELECT h._IDRRef, h._Date_Time, d._Fld13628RRef, d._Fld13629
FROM _Document476 h
LEFT JOIN _Document476_VT13626 d
  ON h._IDRRef = d._Document476_IDRRef
```

## Особенность
- VT join key = `_Document{N}_IDRRef` (содержит номер документа в имени)
- Шапка join key = `_IDRRef`

## Стратегия загрузки (SalesETL)
- Шапка: UPSERT по document_uid
- Строки: DELETE WHERE document_uid IN (...) → INSERT (нет уникального ключа у строк)

## В ETL Config App
При Discover автоматически:
1. Находит VT таблицы через MSSQL `INFORMATION_SCHEMA`
2. Создаёт source с `parent_source_id` и join keys
3. `source_type = 'detail'`

## Ссылки
- [[Три режима загрузки — full incremental consistency]]
- [[Union объединяет документы в одну витрину]]
