---
tags: [1С, интеграция, API]
date: 2026-04-10
---
# 1С загружается через HTTP meta API

## Endpoint
`http://192.168.18.224:8090/NikitaBase/hs/meta`

## Методы
- `GET /search?q=Продажи` — поиск объектов по имени
- `GET /db_structure/Document476` — структура таблицы (поля с русскими именами)
  - **Без** префикса `_` (не `_Document476`)
  - Можно передать несколько через запятую

## Что возвращает
```json
{
  "table_name": "Документ.ЧекККМ",
  "table_name_sql": "Document476",
  "fields": [
    {"field_name": "Номенклатура", "field_name_sql": "Fld13628"},
    {"field_name": "Количество", "field_name_sql": "Fld13629"}
  ]
}
```

## Особенности
- API **не возвращает** типы данных полей — берём из MSSQL INFORMATION_SCHEMA
- Имена полей в API без `_` и без `RRef` суффикса
- Реальная MSSQL колонка: `_Fld13628RRef` (для ссылочных полей)

## VT таблицы (табличные части)
- `Document476.VT13626` — API формат
- `_Document476_VT13626` — MSSQL формат
- Join: `_Document476_VT13626._Document476_IDRRef = _Document476._IDRRef`

## Клиенты в коде
- `etl_config_app/onec_client.py` — standalone (прямой HTTP)
- `plugins/onec_api.py` — Airflow plugin (OneCMetaClient)

## Ссылки
- [[Binary данные конвертируются по длине байтов]]
- [[ETL Config App управляет конфигурацией на порту 5555]]
