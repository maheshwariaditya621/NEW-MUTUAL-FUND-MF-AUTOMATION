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

scheme_id = 19263  # ABAKKUS MUTUAL FUND
period_ids_list = [217, 915, 965, 1014]

cur.execute(
    """
    SELECT 
        COALESCE(im.entity_id, c.entity_id, -c.company_id) as logical_id,
        COALESCE(ce.canonical_name, im.canonical_name) as resolved_name,
        c.company_name as raw_name,
        COALESCE(ce.sector, im.sector, c.sector) as sector,
        c.isin,
        p.year,
        p.month,
        ss.total_value_inr / 10000000.0 as equity_aum_cr_val,
        ss.total_net_assets_inr / 10000000.0 as total_aum_cr_val,
        eh.percent_of_nav,
        COALESCE(eh.adj_quantity, eh.quantity) as quantity,
        c.market_cap,
        c.mcap_type,
        c.shares_outstanding
    FROM equity_holdings eh
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN companies c ON eh.company_id = c.company_id
    LEFT JOIN isin_master im ON c.isin = im.isin
    LEFT JOIN corporate_entities ce ON COALESCE(im.entity_id, c.entity_id) = ce.entity_id
    JOIN periods p ON ss.period_id = p.period_id
    WHERE ss.scheme_id = %s AND p.period_id = ANY(%s)
    ORDER BY logical_id, p.year DESC, p.month DESC
    LIMIT 5
    """,
    (scheme_id, period_ids_list)
)
rows = cur.fetchall()
print("Holdings Rows:", rows)

cur.close()
conn.close()
