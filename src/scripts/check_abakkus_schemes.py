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
cur.execute("SELECT scheme_id, scheme_name FROM schemes WHERE amc_id = 277")
print(cur.fetchall())
cur.close()
conn.close()
