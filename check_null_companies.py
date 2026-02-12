from src.db import get_connection

conn = get_connection()
cur = conn.cursor()

# Check companies table for null/N/A entries
cur.execute("""
    SELECT company_id, company_name, isin, sector
    FROM companies
    WHERE company_name IS NULL OR company_name = 'N/A' OR company_name = ''
    LIMIT 20
""")

print("Companies with NULL/N/A names:")
results = cur.fetchall()
if results:
    for row in results:
        print(f"  ID: {row[0]}, Name: '{row[1]}', ISIN: {row[2]}, Sector: {row[3]}")
else:
    print("  None found")

# Check equity_holdings with null company_id
cur.execute("""
    SELECT eh.holding_id, eh.company_id, eh.market_value_inr, s.scheme_name
    FROM equity_holdings eh
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
    AND eh.company_id IS NULL
    LIMIT 10
""")

print("\nICICI Holdings with NULL company_id:")
results = cur.fetchall()
if results:
    for row in results:
        print(f"  Holding ID: {row[0]}, Company ID: {row[1]}, Value: {row[2]}, Scheme: {row[3]}")
else:
    print("  None found")

# Check actual company names for ICICI holdings
cur.execute("""
    SELECT c.company_name, c.isin, COUNT(*) as count
    FROM equity_holdings eh
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    LEFT JOIN companies c ON eh.company_id = c.company_id
    WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
    GROUP BY c.company_name, c.isin
    HAVING c.company_name IS NULL OR c.company_name = 'N/A'
    LIMIT 20
""")

print("\nICICI Holdings grouped by NULL/N/A company:")
results = cur.fetchall()
if results:
    for row in results:
        print(f"  Company: '{row[0]}', ISIN: {row[1]}, Count: {row[2]}")
else:
    print("  None found")

cur.close()
conn.close()
