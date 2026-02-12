from src.db import get_connection

conn = get_connection()
cur = conn.cursor()

# Get equity_holdings schema
cur.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'equity_holdings' 
    ORDER BY ordinal_position
""")

print('equity_holdings columns:')
for row in cur.fetchall():
    print(f'  {row[0]}')

cur.close()
conn.close()
