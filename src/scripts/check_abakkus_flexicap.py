import os
from dotenv import load_dotenv
import psycopg2

load_dotenv('d:/CODING/NEW MUTUAL FUND MF AUTOMATION/.env')

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
cur = conn.cursor()
cur.execute("""
    SELECT s.scheme_name, c.company_name, eh.percent_of_nav 
    FROM equity_holdings eh 
    JOIN scheme_snapshots sn ON eh.snapshot_id = sn.snapshot_id 
    JOIN schemes s ON sn.scheme_id = s.scheme_id 
    JOIN companies c ON eh.company_id = c.company_id 
    WHERE s.scheme_id = 4727 AND sn.period_id = 1014 
    ORDER BY eh.percent_of_nav DESC LIMIT 5
""")
for row in cur.fetchall():
    print(row)
cur.close()
conn.close()
