---
tags: [решение, трансформация, recorder]
date: 2026-04-14
---
# Recorder type хранится как название документа

## Проблема
В регистре накопления `_RecorderTRef` = binary(4) = номер типа документа (476, 254...).
Нужно хранить как читаемое название: "ЧекККМ", "РеализацияТоваровУслуг".

## Решение
Новый transform `recorder_type_lookup`:
1. binary(4) → int (binary_to_int)
2. int → lookup по словарю → "ЧекККМ"
3. Словарь из `register.recorder_type_map` (JSONB), заполняется при Discover

## Реализация
- `dags/core/transform/transform_utils.py` — метод `_recorder_type_lookup`
- `etl_config_app/dao.py` — `auto_create_mappings` подставляет map в transform_params
- Discover сохраняет type_map на parent и child регистры

## Пример
```
_RecorderTRef = 0x000001DC → 476 → "ЧекККМ"
recorder_type_map = {"476": {"onec_name": "ЧекККМ"}, "254": {"onec_name": "Возврат"}}
```

## Ссылки
- [[Binary данные конвертируются по длине байтов]]
- [[Union объединяет документы в одну витрину]]
