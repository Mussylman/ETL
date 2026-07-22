#!/usr/bin/env python3
"""Агрегатная сверка витрины продаж с 1С: MSSQL _AccumRg17844 vs PostgreSQL sales/sales_positions.

Использование:
    python3 docs/audits/sql/sales_recon.py                      # последние 30 дней включая сегодня
    python3 docs/audits/sql/sales_recon.py --start 2026-06-15 --end 2026-07-21   # end исключительно
    python3 docs/audits/sql/sales_recon.py --full               # + полная таблица по всем дням

Только SELECT. Сегодняшний день всегда частичный: небольшая дельта на нём — норма
(инкремент отстаёт на 5-25 минут). Смотри на закрытые дни.
"""
import sys, argparse
from datetime import date, timedelta
from zoneinfo import ZoneInfo
from datetime import datetime

sys.path.insert(0, '/home/dev/airflow/dags')
from core.transform.binary import binary_to_int
import psycopg2, pymssql
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument('--start', default=None, help='начало периода YYYY-MM-DD (включительно)')
ap.add_argument('--end', default=None, help='конец периода YYYY-MM-DD (исключительно)')
ap.add_argument('--full', action='store_true', help='показать все дни, не только с дельтой')
args = ap.parse_args()

today = datetime.now(ZoneInfo('Asia/Almaty')).date()
start = date.fromisoformat(args.start) if args.start else today - timedelta(days=30)
end = date.fromisoformat(args.end) if args.end else today + timedelta(days=1)
ms_start = start.replace(year=start.year + 2000)
ms_end = end.replace(year=end.year + 2000)

print(f"Сверка [{start}, {end}) — end исключительно; сегодня {today} (частичный день)\n")

ms = pymssql.connect(server='10.10.1.61', user='musulmon.k', password='Zz123456', database='UPP_JAN')
mdf = pd.read_sql(f"""
    SELECT CAST(_Period AS date) AS d, _RecorderTRef AS tref,
           COUNT(DISTINCT _RecorderRRef) AS docs, COUNT(*) AS rows_cnt,
           SUM(_Fld17855) AS stoimost, SUM(_Fld17857) AS nds
    FROM dbo._AccumRg17844
    WHERE _Period >= '{ms_start}' AND _Period < '{ms_end}'
    GROUP BY CAST(_Period AS date), _RecorderTRef""", ms)
ms.close()
mdf['d'] = mdf['d'].apply(lambda x: (pd.Timestamp(x) - pd.DateOffset(years=2000)).date())

pg = psycopg2.connect(host='10.10.1.142', user='airflow_admin', password='1234Aa', dbname='test')
gdf = pd.read_sql(f"""
    SELECT s.period::date AS d, p.recorder_type AS tref,
           COUNT(DISTINCT p.recorder) AS docs, COUNT(*) AS rows_cnt,
           SUM(NULLIF(p.stoimost,'NaN'::numeric)) AS stoimost,
           SUM(NULLIF(p.nds,'NaN'::numeric)) AS nds
    FROM public.sales_positions p
    JOIN public.sales s ON s.recorder = p.recorder AND s.recorder_type = p.recorder_type
    WHERE s.period >= '{start}' AND s.period < '{end}'
    GROUP BY 1, 2""", pg)
pg.close()

M = ['docs', 'rows_cnt', 'stoimost', 'nds']
for c in M:
    mdf[c] = pd.to_numeric(mdf[c], errors='coerce').fillna(0)
    gdf[c] = pd.to_numeric(gdf[c], errors='coerce').fillna(0)

def fmt(v):
    return f"{v:,.2f}".replace(',', ' ')

# итого
mt, gt = mdf[M].sum(), gdf[M].sum()
print("| Показатель | 1С | Витрина | Δ | Δ% |")
print("|---|---:|---:|---:|---:|")
for name, k in [('Документов', 'docs'), ('Строк', 'rows_cnt'), ('SUM(stoimost)', 'stoimost'), ('SUM(nds)', 'nds')]:
    d = gt[k] - mt[k]
    pct = d / mt[k] * 100 if mt[k] else 0
    print(f"| {name} | {fmt(mt[k])} | {fmt(gt[k])} | {fmt(d)} | {pct:+.2f}% |")

# по дням
msd = mdf.groupby('d')[M].sum()
pgd = gdf.groupby('d')[M].sum()
days = msd.join(pgd, how='outer', lsuffix='_ms', rsuffix='_pg').fillna(0)
bad = []
print("\n| День | Δдок | Δстрок | Δ stoimost | Δ nds | |")
print("|---|---:|---:|---:|---:|---|")
for d, r in days.iterrows():
    dd, rd = r['docs_pg'] - r['docs_ms'], r['rows_cnt_pg'] - r['rows_cnt_ms']
    sd, nd = r['stoimost_pg'] - r['stoimost_ms'], r['nds_pg'] - r['nds_ms']
    is_bad = (dd or rd or abs(sd) > 0.01 or abs(nd) > 0.01)
    mark = '⚠️ частичный день' if d == today else ('❌' if is_bad else '✅')
    if is_bad and d != today:
        bad.append(str(d))
    if args.full or is_bad or d == today:
        print(f"| {d} | {dd:+.0f} | {rd:+.0f} | {fmt(sd)} | {fmt(nd)} | {mark} |")
if not args.full:
    ok_days = len(days) - len(bad) - (1 if today in days.index else 0)
    print(f"\n✅ дней без дельты: {ok_days} (не показаны; --full чтобы видеть все)")
if bad:
    print(f"❌ дни с дельтой: {', '.join(bad)} — виновника ищи анти-джойном (см. журнал в sales_aggregate_recon_2026-07-21.md)")
else:
    print("Закрытые дни сходятся с 1С полностью.")
