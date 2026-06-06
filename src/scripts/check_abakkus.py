import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
cur = conn.cursor()

# Get the snapshot info for abakkus
cur.execute("""
    SELECT s.snapshot_id, s.scheme_id, sch.scheme_name, sch.amc_id 
    FROM scheme_snapshots s 
    JOIN schemes sch ON s.scheme_id = sch.scheme_id 
    JOIN amcs a ON sch.amc_id = a.amc_id 
    WHERE a.amc_name ILIKE '%abakkus%' AND s.period_id = 1014
""")
rows = cur.fetchall()
print("Snapshots:", rows)

cur.execute("""
    SELECT count(*) 
    FROM equity_holdings h
    JOIN scheme_snapshots s ON h.snapshot_id = s.snapshot_id
    JOIN schemes sch ON s.scheme_id = sch.scheme_id 
    JOIN amcs a ON sch.amc_id = a.amc_id 
    WHERE a.amc_name ILIKE '%abakkus%' AND s.period_id = 1014
""")
print("Holdings count:", cur.fetchone())

cur.close()
conn.close()
