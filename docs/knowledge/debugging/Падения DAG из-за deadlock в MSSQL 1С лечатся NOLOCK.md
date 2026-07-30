---
tags: [debugging, sales, incremental, mssql, deadlock]
date: 2026-07-30
---

# Падения DAG из-за deadlock в MSSQL 1С лечатся NOLOCK

## Симптом
`sales_incremental_5min` падал ~19 раз за неделю (31 за две), без видимой закономерности по времени суток. Retry (retries=1, delay=2 мин) почти всегда спасал: следующая попытка проходила успешно. Один раз за 2 недели упали две попытки подряд — окно не потерялось только благодаря добору хвоста (см. [[Инкремент терял документы из-за лага проведения 1С больше overlap]]).

## Диагностика: причина не сохранялась
`load_history.error_message` был бесполезен: движок писал `str(e)[:2000]`, а исключение начинается с полного текста SQL (~2 КБ) — **настоящая причина обрезалась**. Пришлось искать в структурированных логах Airflow:

```bash
# в logs/dag_id=sales_incremental_5min/run_id=.../attempt=1.log
# причина лежит в JSON-поле error_detail, не в event
python3 -c "... json.loads(line)['error_detail'] ..."
```

Причина (100% падений, 19 из 19 проверенных логов):
```
(1205, b'Transaction (Process ID 62) was deadlocked on lock | communication
buffer resources with another process and has been chosen as the deadlock victim.
Rerun the transaction. DB-Lib error message 20018')
```

Наши SELECT-ы с JOIN-ами по горячим таблицам (`_AccumRg17844` + шапки + VT) конфликтовали с записями самой 1С в боевой базе.

## Решение
1. **`WITH (NOLOCK)` на всех таблицах** — `QueryBuilder.TABLE_HINT` + единый хелпер `_table_ref(schema, table, alias)`, применён во всех трёх генераторах (простой SELECT, UNION, accumrg_with_documents). Грязное чтение приемлемо: данные аналитические, расхождения ловит еженедельная сверка `sales_recon.py`.
2. **Читаемый `error_message`** — `_format_error()` в etl_engine: вырезает тело SQL из сообщения вида `Execution failed on sql '<SQL>': <причина>` и ставит причину первой. Реальный deadlock-кейс: было 2000 символов мусора, стало 369 символов с причиной в начале.

Проверка: генерация — 9/9 таблиц с хинтом в обоих target-ах; живой full_period (2140 строк, валидация зелёная); первый инкрементальный тик 10157 success.

## Урок
Обрезание длинных исключений «с головы» прячет причину — драйверы кладут её в конец сообщения. Для любых `str(e)[:N]` в проекте стоит проверять, что именно попадает в лимит.
