---
tags: [debugging, sales, loader, pandas, data-quality, prod]
date: 2026-09-03
---

# NULL-меры из COALESCE попадают в витрину как numeric NaN и ломают SUM

## Симптом
В PROD `sales_positions` 408 497 из 435 835 строк имеют `evrika_bonusy = 'NaN'` и `evrika_spisannye = 'NaN'`
(все ЧекККМ 476 и ОтчётКомитенту 352, часть 415/254), `summands` — 407 745, `summa`/`tsena` — 803.
`SELECT SUM(evrika_bonusy) FROM sales_positions` возвращает **NaN**; с фильтром `<> 'NaN'` — 131 085 886.
Любой BI-агрегат по этим колонкам без фильтра ломается.

## Причина
Мера отсутствует у части источников (у ТЧ ЧекККМ нет колонок Эврика_Бонусы) → в SELECT accumrg-пайплайна
`COALESCE(...)` по VT даёт NULL → pandas превращает NULL в числовой колонке во `float('nan')` →
psycopg2 адаптирует `nan` как `'NaN'::numeric` — PostgreSQL это легальное значение numeric, INSERT проходит.
`Loaders._prepare_df` нормализует даты, uuid и JSON, но не NaN.

## Как исправить
1. Loader: `df[col] = df[col].where(pd.notna(df[col]), None)` для всех колонок перед INSERT (NaN → NULL).
2. Разовая чистка PROD: `UPDATE public.sales_positions SET evrika_bonusy = NULL WHERE evrika_bonusy = 'NaN'` (и остальные колонки).
3. Проверка в валидатор: `COUNT(*) WHERE <numeric> = 'NaN'` = 0 по всем numeric-колонкам таргета.

Актуально для order: у услуг нет `kachestvo`, `sklad`, бонусов — UNION отдаёт NULL, без фикса loader'а
получим ту же NaN-заразу. Фикс loader'а — предпосылка загрузки order.

Связано: [[Ссылки в фактах — BIGINT id для BI и raw_refs JSONB для исходных GUID]].
