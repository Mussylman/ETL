---
tags: [решение, трансформация, binary]
date: 2026-04-10
---
# Binary данные конвертируются по длине байтов

## Проблема
1С хранит данные в binary-формате. Нужно конвертировать для PostgreSQL.

## Решение
Файл: `dags/core/transform/binary.py`

| MSSQL тип | Длина | PostgreSQL | Функция | Transform |
|---|---|---|---|---|
| binary(16) | 16 байт | uuid | binary_to_uuid | binary_auto |
| binary(4) | 4 байта | integer | binary_to_int | binary_auto |
| binary(1) | 1 байт | boolean | binary_to_bool | binary_auto |

## Важно
- `binary_to_uuid` — 1С хранит UUID в нестандартном порядке байтов. Функция переворачивает правильно.
- `binary_to_int` — big-endian (для _RecorderTRef, _RTRef)
- `binary_to_bool` — `b'\x00'` = false, всё остальное = true
- `process_binary_auto` — автоопределение по длине

## В ETL Config App
Правила в `app.py` `_MSSQL_TYPE_RULES`:
```python
("binary", 16): ("uuid", "binary_to_uuid")
("binary", 4):  ("integer", "binary_to_int")
("binary", 1):  ("boolean", "binary_to_bool")
```

## Ссылки
- [[Год 4025 исправляется на 2025 в датах 1С]]
- [[1С загружается через HTTP meta API]]
