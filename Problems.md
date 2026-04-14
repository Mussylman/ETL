# Проблемы и баги

## Критичные
- [ ] Повторный Discover → duplicate key error (source_code уже есть)
- [ ] Column Builder и Source Detail — два места для маппинга, путаница

## UX
- [ ] Discover неочевиден: надо сначала parent, потом child, потом discover
- [ ] VT таблицы при discover — все выбраны, нужно наоборот
- [ ] Target type select — varchar(255) не ставится как значение

## Решено
- [x] fix_year не ставился для datetime — добавлен binary_auto
- [x] 1С API вызов с _ префиксом — убран
- [x] MSSQL таблица без _ префикса — добавлен
- [x] VT чекбоксы каскадно снимались — теперь независимые
