---
tags: [баг, решено, 1С-API]
date: 2026-04-10
---
# 1С API не находит документ с префиксом подчёркивания

## Ошибка
`get_structure(["_Document476"])` возвращал пустой результат.

## Причина
1С API ожидает имена БЕЗ префикса `_`. Правильно: `Document476`.

## Решение
В `onec_client.py` функция `resolve_document_names` использует `Document{n}` без `_`:
```python
sql_names = [f"Document{n}" for n in type_numbers]
```

## Обратная ситуация
MSSQL таблицы наоборот — С префиксом `_`: `_Document476`.
В `mssql_client.py` добавлена автоподстановка:
```python
mssql_table = table if table.startswith("_") else f"_{table}"
```

## Ссылки
- [[1С загружается через HTTP meta API]]
