---
tags: [решение, архитектура, union]
date: 2026-04-10
---
# Union объединяет документы в одну витрину

## Проблема
Регистр накопления `_AccumRg17844` (Продажи) ссылается на 6 типов документов через `_RecorderTRef`. Каждый документ — отдельная таблица в MSSQL. Нужно объединить в одну target таблицу.

## Решение
UNION ALL через конфигурацию:

```
_Document476 (ЧекККМ)          ─┐
_Document415 (Реализация)       ─┤
_Document254 (Возврат)          ─┤── UNION ALL → sales_positions
_Document352 (Отчёт)            ─┤
_Document443 (Сторнирование)    ─┤
_Document316 (Корректировка)    ─┘
```

## Как работает
1. Каждый документ = отдельный source в etl_meta
2. Union объединяет все sources с одинаковыми output_columns
3. Каждый source добавляет `doc_type` expression = имя документа 1С
4. Target таблица получает данные через union

## Связь с parent
- Parent register: `sales` (таблица `_AccumRg17844`)
- Child register: `sales_positions`
- Join: `sales.recorder = sales_positions.id_ref`
- `_RecorderTRef` определяет тип документа (254, 415, 476...)
- `_RecorderRRef` = UUID конкретного документа

## Discover
Автоматическое обнаружение:
1. `SELECT DISTINCT _RecorderTRef FROM _AccumRg17844`
2. Конвертация binary(4) → int (254, 316, 352...)
3. 1С API: `Document254` → `Документ.ВозвратТоваровОтПокупателя`
4. Batch create: sources + union + target

## Ссылки
- [[Схема базы данных etl_meta]]
- [[1С загружается через HTTP meta API]]
