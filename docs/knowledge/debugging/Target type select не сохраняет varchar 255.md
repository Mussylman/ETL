---
tags: [баг, решено, UI]
date: 2026-04-10
---
# Target type select не сохраняет varchar(255)

## Ошибка
При редактировании маппинга target_type "varchar(255)" не ставился в select — значение сбрасывалось.

## Причина
HTML `<select>` не имеет option с значением "varchar(255)" — только "varchar".

## Решение
Добавлен hidden input `<input type="hidden" name="target_type">` для отправки формы. Select стал display-only (без атрибута name). JS функция `initTargetType()` парсит "varchar(255)" обратно в select + length.

## Ссылки
- [[ETL Config App управляет конфигурацией на порту 5555]]
