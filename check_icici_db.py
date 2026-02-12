from src.db import get_connection

conn = get_connection()
cur = conn.cursor()

# Check all AMCs
cur.execute("SELECT amc_name, COUNT(*) FROM amcs GROUP BY amc_name ORDER BY amc_name")
print('AMCs in database:')
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]} AMC records')

# Check schemes per AMC
cur.execute("""
    SELECT a.amc_name, COUNT(s.scheme_id) as scheme_count
    FROM amcs a
    LEFT JOIN schemes s ON a.amc_id = s.amc_id
    GROUP BY a.amc_name
    ORDER BY a.amc_name
""")
print('\nSchemes per AMC:')
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]} schemes')

# Check holdings per AMC
cur.execute("""
    SELECT a.amc_name, COUNT(eh.holding_id) as holdings_count
    FROM amcs a
    LEFT JOIN schemes s ON a.amc_id = s.amc_id
    LEFT JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
    LEFT JOIN equity_holdings eh ON ss.snapshot_id = eh.snapshot_id
    GROUP BY a.amc_name
    ORDER BY a.amc_name
""")
print('\nHoldings per AMC:')
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]} holdings')

# Check ICICI specifically
cur.execute("""
    SELECT s.scheme_name, ss.period_id, COUNT(eh.holding_id) as holdings
    FROM schemes s
    JOIN amcs a ON s.amc_id = a.amc_id
    LEFT JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
    LEFT JOIN equity_holdings eh ON ss.snapshot_id = eh.snapshot_id
    WHERE a.amc_name LIKE '%ICICI%'
    GROUP BY s.scheme_name, ss.period_id
    ORDER BY s.scheme_name
""")
print('\nICICI schemes detail:')
for row in cur.fetchall():
    print(f'  {row[0]}: period_id={row[1]}, holdings={row[2]}')

cur.close()
conn.close()
