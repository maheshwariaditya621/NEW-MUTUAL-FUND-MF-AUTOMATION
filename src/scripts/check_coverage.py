import os
from dotenv import load_dotenv
import psycopg2
import sys

# add src to path
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

coverage = get_period_coverage(cur)
print("COVERAGE:", coverage)

cur.close()
conn.close()
