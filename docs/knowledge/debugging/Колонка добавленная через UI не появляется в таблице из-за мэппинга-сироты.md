---
tags: [баг, решено, конфигуратор, справочники]
date: 2026-08-20
---
# Колонка, добавленная через UI, не появляется в таблице из-за мэппинга-сироты

## Симптом
Пользователь добавил в `dim_nomenklatura` три поля (Родитель, ЭтоГруппа, Бренд).
UI отработал без ошибок, `POST /sources/285/mappings/batch` вернул `200 OK`.
`SELECT * FROM public.dim_nomenklatura` — новых колонок нет.

Обманчивая деталь: мэппинги **в конфиге есть**. Их не видно, если искать
привычным запросом — он джойнит по `register_id`, а тот пустой:

```sql
-- ничего не покажет
SELECT * FROM etl_meta.column_mappings cm
JOIN etl_meta.registers r ON r.id = cm.register_id WHERE r.code='dim_nomenklatura';
-- покажет
SELECT * FROM etl_meta.column_mappings WHERE source_id = 285;
```

## Причина
Эндпоинт `mapping_batch` создавал мэппинг, не проставляя `register_id` и
`target_id`, и не дописывая колонку в `register_targets.include_columns`.
Sync собирает DDL именно по `include_columns` (`_collect_mappings_for_target`),
поэтому не видел новых колонок и честно применял пустой план — отсюда `200 OK`
без единого `ALTER TABLE`. Движок читает мэппинги по `register_id` и тоже бы их
не увидел.

Мэппинг-сирота = запись есть, но не принадлежит ни регистру, ни таргету.

## Решение
`mapping_batch` теперь: проставляет `register_id`/`target_id`, вызывает
`dao.add_include_column()`, и только потом Sync. Пустой список колонок →
`400`, а не «успешно ничего не сделал».

## Три бага, вскрывшихся следом
1. **Несуществующая колонка.** «Бренд» пришёл из 1С meta API с пустым
   `mssql_column`; фронт достраивал имя как `'_' + field_name_sql` → `_Fld25945`,
   которого в `UPP_JAN._Reference123` нет. Первый же `SELECT` к 1С упал бы.
   Теперь такие поля помечены «нет в базе» и не выбираются, а бэкенд их
   отклоняет. См. [[Реквизит есть в конфигурации 1С но колонки в базе нет]].
2. **Неизвестный transform.** UI по умолчанию ставит `binary_auto`, а
   `load_dim_from_config._transformers()` знал только три явных — падение с
   «неизвестный transform_type». Добавлены `binary_auto` и `invert_bool`.
3. **uuid = text.** Значения из `VALUES %s` psycopg2 отдаёт как text, каст
   был только у ключа. Пока все поля справочников были text, это не всплывало;
   первая uuid-колонка дала `operator does not exist: uuid = text`. Теперь
   `_col_types()` кастует по фактическим типам таблицы.

## Как проверять впредь
После добавления колонки через UI:
```sql
SELECT id, register_id, target_id, source_column, target_column
FROM etl_meta.column_mappings WHERE source_id = <src>;   -- register_id не NULL
SELECT include_columns FROM etl_meta.register_targets WHERE id = <tgt>;
```
Пустой `register_id` — колонка не доедет до таблицы.

Связано: [[Латентные баги конфигуратора класса target_role]],
[[Target type select не сохраняет varchar 255]]
