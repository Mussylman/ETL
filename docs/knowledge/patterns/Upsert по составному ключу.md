---
tags: [паттерн, ETL, загрузка]
date: 2026-04-10
---
# Upsert по составному ключу

## Паттерн
`INSERT ON CONFLICT (key1, key2) DO UPDATE` — обновление при совпадении ключей.

## Реализация
Файл: `dags/core/load/loaders.py`

```python
def upsert_by_keys(df, table_name, key_columns):
    # INSERT ON CONFLICT (key1, key2, ...) DO UPDATE
    # Обновляет только не-ключевые колонки
```

## Режимы загрузки
| Режим | SQL | Когда |
|---|---|---|
| insert | INSERT INTO | Первая загрузка, append-only |
| upsert | INSERT ON CONFLICT DO UPDATE | Инкрементальная загрузка |
| replace | DELETE + INSERT | Полная перезагрузка периода |

## Особенности
- Batch commit каждые 3000 строк
- datetime64 → python datetime (для psycopg2)
- UUID → str (для PostgreSQL)

## Ссылки
- [[Три режима загрузки — full incremental consistency]]
