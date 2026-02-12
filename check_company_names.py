from src.db import get_connection

conn = get_connection()
cur = conn.cursor()

# Check ICICI holdings with null company_name
cur.execute("""
    SELECT 
        COUNT(*) as total_holdings,
        COUNT(CASE WHEN company_name IS NULL THEN 1 END) as null_company_names,
        COUNT(CASE WHEN company_name IS NOT NULL THEN 1 END) as valid_company_names
    FROM equity_holdings eh
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
""")

result = cur.fetchone()
print(f"ICICI Holdings Analysis:")
print(f"  Total holdings: {result[0]}")
print(f"  NULL company_name: {result[1]} ({result[1]/result[0]*100:.1f}%)")
print(f"  Valid company_name: {result[2]} ({result[2]/result[0]*100:.1f}%)")

# Sample null company_name records
cur.execute("""
    SELECT s.scheme_name, eh.isin, eh.company_name, eh.market_value_inr
    FROM equity_holdings eh
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
    AND eh.company_name IS NULL
    LIMIT 10
""")

print("\nSample NULL company_name records:")
for row in cur.fetchall():
    print(f"  Scheme: {row[0]}")
    print(f"    ISIN: {row[1]}, Company: {row[2]}, Value: {row[3]}")

# Compare with HDFC
cur.execute("""
    SELECT 
        COUNT(*) as total_holdings,
        COUNT(CASE WHEN company_name IS NULL THEN 1 END) as null_company_names
    FROM equity_holdings eh
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    WHERE a.amc_name = 'HDFC Mutual Fund'
""")

result = cur.fetchone()
print(f"\nHDFC Holdings (for comparison):")
print(f"  Total holdings: {result[0]}")
print(f"  NULL company_name: {result[1]} ({result[1]/result[0]*100:.1f}%)")

cur.close()
conn.close()
