# 10 апреля 2026

## План
- [x] Авто-преобразования по MSSQL типам (binary_auto, fix_year)
- [x] MSSQL INFORMATION_SCHEMA → target_type + transform
- [x] VT чекбоксы независимые (не каскадные)
- [x] fields_cache JSONB — кэш полей 1С + типы MSSQL
- [ ] Тестировать полный flow
- [ ] Подготовить к проду

## Результат
- binary(16)→uuid, binary(1)→bool, binary(4)→int, datetime→timestamp+fix_year
- Column Builder автоматом ставит тип и transform при выборе поля
- 3 вкладки: Источники, Колонки, Статус
- UI на русском
