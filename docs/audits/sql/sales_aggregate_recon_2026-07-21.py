#!/usr/bin/env python3
"""Простая агрегатная сверка MSSQL 1С (_AccumRg17844) vs PostgreSQL (sales/sales_positions).
Период: [2026-06-15, 2026-07-21) — start включительно, end исключительно. Только SELECT."""
import sys
sys.path.insert(0, '/home/dev/airflow/dags')
from core.transform.binary import binary_to_int
import psycopg2, pymssql
import pandas as pd

# --- MSSQL: агрегаты по дню и типу (год в 1С +2000) ---
ms = pymssql.connect(server='10.10.1.61', user='musulmon.k', password='Zz123456', database='UPP_JAN')
mdf = pd.read_sql("""
    SELECT CAST(_Period AS date) AS d, _RecorderTRef AS tref,
           COUNT(DISTINCT _RecorderRRef) AS docs, COUNT(*) AS rows_cnt,
           SUM(_Fld17854) AS kolichestvo, SUM(_Fld17855) AS stoimost,
           SUM(_Fld17856) AS stoimost_bez_skidok, SUM(_Fld17857) AS nds
    FROM dbo._AccumRg17844
    WHERE _Period >= '4026-06-15' AND _Period < '4026-07-21'
    GROUP BY CAST(_Period AS date), _RecorderTRef
""", ms)
ms.close()
mdf['tref'] = mdf['tref'].apply(binary_to_int)
mdf['d'] = mdf['d'].apply(lambda x: (pd.Timestamp(x) - pd.DateOffset(years=2000)).date())

# --- PostgreSQL: те же агрегаты (позиции + шапки; period из шапки) ---
pg = psycopg2.connect(host='10.10.1.142', user='airflow_admin', password='1234Aa', dbname='test')
gdf = pd.read_sql("""
    SELECT s.period::date AS d, p.recorder_type AS tref,
           COUNT(DISTINCT p.recorder) AS docs, COUNT(*) AS rows_cnt,
           SUM(NULLIF(p.kolichestvo,'NaN'::numeric)) AS kolichestvo,
           SUM(NULLIF(p.stoimost,'NaN'::numeric)) AS stoimost,
           SUM(NULLIF(p.stoimost_bez_skidok,'NaN'::numeric)) AS stoimost_bez_skidok,
           SUM(NULLIF(p.nds,'NaN'::numeric)) AS nds
    FROM public.sales_positions p
    JOIN public.sales s ON s.recorder = p.recorder AND s.recorder_type = p.recorder_type
    WHERE s.period >= '2026-06-15' AND s.period < '2026-07-21'
    GROUP BY 1, 2
""", pg)
pg.close()

METRICS = ['docs', 'rows_cnt', 'kolichestvo', 'stoimost', 'stoimost_bez_skidok', 'nds']
for c in METRICS:
    mdf[c] = pd.to_numeric(mdf[c], errors='coerce').fillna(0)
    gdf[c] = pd.to_numeric(gdf[c], errors='coerce').fillna(0)

def agg(df, by):
    return df.groupby(by)[METRICS].sum()

def fmt(v, money=False):
    if money: return f"{v:,.2f}".replace(',', ' ')
    return f"{v:,.0f}".replace(',', ' ')

def table(ms_row, pg_row, title):
    print(f"\n### {title}")
    print("| Показатель | MSSQL 1С | PostgreSQL | Разница | Разница % |")
    print("|---|---:|---:|---:|---:|")
    items = [
        ('Документов', 'docs', False), ('Строк', 'rows_cnt', False),
        ('SUM(kolichestvo)', 'kolichestvo', True), ('SUM(stoimost)', 'stoimost', True),
        ('SUM(nds)', 'nds', True), (None, None, None),  # stoimost+nds
        ('SUM(stoimost_bez_skidok)', 'stoimost_bez_skidok', True), (None, None, 'disc'),
    ]
    def row(name, m, g):
        d = g - m
        pct = (d / m * 100) if m else (0.0 if d == 0 else float('inf'))
        print(f"| {name} | {fmt(m, True)} | {fmt(g, True)} | {fmt(d, True)} | {pct:+.2f}% |")
    for name, key, money in items:
        if key:
            row(name, float(ms_row[key]), float(pg_row[key]))
        elif money is None:
            row('SUM(stoimost)+SUM(nds)', float(ms_row['stoimost'] + ms_row['nds']),
                float(pg_row['stoimost'] + pg_row['nds']))
        else:
            row('Скидка (sbs - stoimost)', float(ms_row['stoimost_bez_skidok'] - ms_row['stoimost']),
                float(pg_row['stoimost_bez_skidok'] - pg_row['stoimost']))

# ===== 1. Весь период =====
ms_tot, pg_tot = mdf[METRICS].sum(), gdf[METRICS].sum()
table(ms_tot, pg_tot, 'ВЕСЬ ПЕРИОД [2026-06-15, 2026-07-21)')

# ===== 2. По recorder_type =====
ms_t, pg_t = agg(mdf, 'tref'), agg(gdf, 'tref')
for t in sorted(set(ms_t.index) | set(pg_t.index)):
    m = ms_t.loc[t] if t in ms_t.index else pd.Series(0.0, index=METRICS)
    g = pg_t.loc[t] if t in pg_t.index else pd.Series(0.0, index=METRICS)
    name = {476: 'ЧекККМ', 415: 'РеализацияТоваровУслуг', 254: 'ВозвратТоваровОтПокупателя (возвраты)', 352: 'ОтчётКомитентуОПродажах'}.get(t, 'НЕИЗВЕСТНЫЙ ТИП')
    table(m, g, f'recorder_type={t} ({name})')

# ===== 3. По дням (компакт: ключевые дельты) =====
ms_d, pg_d = agg(mdf, 'd'), agg(gdf, 'd')
days = ms_d.join(pg_d, how='outer', lsuffix='_ms', rsuffix='_pg').fillna(0)
print("\n### ПО ДНЯМ")
print("| День | Док 1С | Док PG | Δдок | Строк Δ | stoimost 1С | stoimost PG | Δ stoimost | Δ% | Δ nds |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for d, r in days.iterrows():
    dd = r['docs_pg'] - r['docs_ms']; rd = r['rows_cnt_pg'] - r['rows_cnt_ms']
    sd = r['stoimost_pg'] - r['stoimost_ms']; nd = r['nds_pg'] - r['nds_ms']
    pct = sd / r['stoimost_ms'] * 100 if r['stoimost_ms'] else 0
    print(f"| {d} | {fmt(r['docs_ms'])} | {fmt(r['docs_pg'])} | {dd:+.0f} | {rd:+.0f} | "
          f"{fmt(r['stoimost_ms'], True)} | {fmt(r['stoimost_pg'], True)} | {fmt(sd, True)} | {pct:+.2f}% | {fmt(nd, True)} |")

# ===== 4. Разбивка: дни full_period vs дни инкремента =====
import datetime as dt
cut = dt.date(2026, 6, 25)
for label, mask_ms, mask_pg in [
    ('Дни, загруженные full_period (2026-06-15..24)', mdf['d'] < cut, gdf['d'] < cut),
    ('Дни на инкременте (2026-06-25..07-20)', mdf['d'] >= cut, gdf['d'] >= cut),
]:
    table(mdf[mask_ms][METRICS].sum(), gdf[mask_pg][METRICS].sum(), label)
