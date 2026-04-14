# Задачи ETL Config

## Сейчас в работе
- [ ] Тестировать полный flow: Register → Discover → Column Builder → Sync
- [ ] Проверить idempotent Discover (повторный не дублирует)
- [ ] Проверить авто-join VT таблиц
- [ ] Проверить binary_auto (uuid/int/bool по длине)

## UI доработки
- [ ] Перевод кнопок и текстов на русский
- [ ] Inline-переименование target колонок
- [ ] Source Detail: 1С имя рядом с target колонкой

## Бэкенд
- [ ] Авто-sync union output_columns при сохранении
- [ ] Авто-sync target table при сохранении колонки
- [ ] Идемпотентный batch_create (skip existing)

## Перед продом
- [ ] Пароли в env/config
- [ ] Обработка ошибок (таймауты MSSQL, 1С API)
- [ ] Логирование
