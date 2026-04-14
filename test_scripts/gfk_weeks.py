"""
Скрипт для просмотра доступных недель в GFK API за последние 100 дней.
Запуск: python test_scripts/gfk_weeks.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

import requests
from airflow.models import Variable

BASE_URL = "https://startrack.mi.gfk.com"
api_key = Variable.get("gfk_token")

url = f"{BASE_URL}/api/v1/Reports?publishedSinceDaysOffset=100&page=1&pageSize=200"
headers = {"x-api-key": api_key, "Accept": "application/json"}

response = requests.get(url, headers=headers)
response.raise_for_status()
reports = response.json()

print(f"Всего отчётов: {len(reports)}\n")

# Группируем по toolName
sales = []
products = []
other = []

for r in reports:
    tool = r.get("toolName", "")
    period = r.get("periodName", "")
    report_id = r.get("reportId")
    uploaded = r.get("lastUploadDate", "")
    entry = {"period": period, "reportId": report_id, "uploaded": uploaded, "tool": tool}

    if tool == "Flat File (CSV) (.csv)":
        sales.append(entry)
    elif tool == "Flat File (SSV) (.csv)":
        products.append(entry)
    else:
        other.append(entry)

# Sales (недельные)
print("=" * 70)
print("SALES — Flat File (CSV) (.csv)")
print("=" * 70)
print(f"{'#':<4} {'Period':<15} {'ReportId':<12} {'Uploaded'}")
print("-" * 70)
for i, s in enumerate(sales, 1):
    print(f"{i:<4} {s['period']:<15} {s['reportId']:<12} {s['uploaded']}")
print(f"\nИтого sales-отчётов: {len(sales)}")

# Products
print()
print("=" * 70)
print("PRODUCTS — Flat File (SSV) (.csv)")
print("=" * 70)
print(f"{'#':<4} {'Period':<15} {'ReportId':<12} {'Uploaded'}")
print("-" * 70)
for i, p in enumerate(products, 1):
    print(f"{i:<4} {p['period']:<15} {p['reportId']:<12} {p['uploaded']}")
print(f"\nИтого product-отчётов: {len(products)}")

# Other (если есть)
if other:
    print()
    print("=" * 70)
    print("OTHER")
    print("=" * 70)
    for i, o in enumerate(other, 1):
        print(f"{i:<4} {o['period']:<15} {o['reportId']:<12} {o['tool']}")
