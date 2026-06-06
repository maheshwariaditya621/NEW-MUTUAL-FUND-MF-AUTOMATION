import os
import asyncio
from dotenv import load_dotenv
import psycopg2
import sys

sys.path.append('d:/CODING/NEW MUTUAL FUND MF AUTOMATION')
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
cur = conn.cursor()

from src.api.utils.data_coverage import get_period_coverage

# Simulating `_get_scheme_portfolio_by_id` for Abakkus

scheme_id = 19263  # ABAKKUS MUTUAL FUND (from earlier check)

coverage = get_period_coverage(cur)

latest_yr = None
latest_mo = None

if coverage.get("is_partial") and coverage.get("prev") and coverage.get("latest"):
    latest_period_id = coverage["latest"]["period_id"]
    cur.execute(
        "SELECT 1 FROM scheme_snapshots WHERE scheme_id = %s AND period_id = %s LIMIT 1",
        (scheme_id, latest_period_id)
    )
    scheme_has_latest = cur.fetchone() is not None

    if not scheme_has_latest:
        print("Scheme does NOT have latest. Falling back to prev.")
        latest_yr = coverage["prev"]["year"]
        latest_mo = coverage["prev"]["month"]
    else:
        print("Scheme HAS latest.")
        latest_yr = coverage["latest"]["year"]
        latest_mo = coverage["latest"]["month"]
else:
    print("Not partial or missing prev/latest")
    latest_yr = coverage["latest"]["year"]
    latest_mo = coverage["latest"]["month"]

print(f"Target Anchor Month: {latest_yr}-{latest_mo}")

# Generate 4 months
target_months = []
curr_yr, curr_mo = latest_yr, latest_mo
for _ in range(4):
    target_months.append((curr_yr, curr_mo))
    curr_mo -= 1
    if curr_mo == 0:
        curr_mo = 12
        curr_yr -= 1
target_months.reverse()

print("Target Months:", target_months)

period_ids_map = {}
for yr, mo in target_months:
    cur.execute("SELECT period_id FROM periods WHERE year = %s AND month = %s", (yr, mo))
    p_row = cur.fetchone()
    if p_row:
        period_ids_map[(yr, mo)] = p_row[0]

period_ids_list = list(period_ids_map.values())
print("Period IDs List:", period_ids_list)

cur.execute(
    """
    SELECT 
        period_id,
        total_value_inr / 10000000.0 as equity_aum_cr,
        total_net_assets_inr / 10000000.0 as total_aum_cr,
        total_holdings
    FROM scheme_snapshots
    WHERE scheme_id = %s AND period_id = ANY(%s)
    """,
    (scheme_id, period_ids_list)
)
snapshots_map = {row[0]: row for row in cur.fetchall()}
print("Snapshots Map keys:", snapshots_map.keys())

cur.close()
conn.close()
