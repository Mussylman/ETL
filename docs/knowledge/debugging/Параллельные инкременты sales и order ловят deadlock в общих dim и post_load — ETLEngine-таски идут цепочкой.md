---
tags: [debugging, airflow, incremental, deadlock, post_load, prod]
date: 2026-09-14
---

# Параллельные инкременты sales и order ловят deadlock в общих dim и post_load — ETLEngine-таски идут цепочкой

## Симптом
В `etl_meta.load_history` PROD за сутки 3 из ~580 прогонов регистров `failed` с
`DeadlockDetected: Process A waits for ShareLock on transaction …; blocked by process B` (13.09 20:20 order,
14.09 13:20 sales, 15:00 order). Каждый раз retry таски (в DAG `retries=1`, `retry_delay=2m`) проходил через
2 минуты, DagRun оставался success, данных не терялось (overlap следующего тика перекрывает окно).

## Причина
Таски `accumrg_with_documents__sales` и `document_with_vt__order` в одном DagRun `incremental_prod` шли
параллельно, а их `post_load_sql` пишут в одни объекты:
- stub-`INSERT … ON CONFLICT (guid) DO NOTHING` + `UPDATE *_id` в общие `dim_kontragent`, `dim_dogovor`,
  `dim_otvetstvennyy`, `dim_sklad`;
- late-resolve `UPDATE public.sales SET zakaz_id …` из post_load `orders` против `UPDATE public.sales`
  собственного post_load sales.
Разный порядок захвата строк двумя транзакциями → классический deadlock.

## Решение
`dags/incremental_dag.py`: у runner'ов появился флаг `serialize`; ETLEngine-таски (accumrg_with_documents,
document_with_vt) выстраиваются цепочкой в порядке discovery (`pipeline_type, code` → sales раньше order)
с `trigger_rule="all_done"`, чтобы падение одного регистра не блокировало следующий. Справочники
(`reference_dim`) остаются параллельными. В TEST один engine-регистр — поведение не изменилось.
Цена: тик PROD длиннее на длительность sales (~10–15 с) при окне 5 минут.

## Как проверять
`SELECT … FROM etl_meta.load_history WHERE status <> 'success' AND started_at >= now() - interval '24 hours'`
— ожидание: 0 строк с `DeadlockDetected`. В UI Airflow у `document_with_vt__order` upstream = sales.

Связано: [[Регистр order строится от шапки документа с UNION табличных частей, а не от AccumRg]],
[[Связь sales → orders через zakaz_id]].
