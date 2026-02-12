from src.db import get_connection

conn = get_connection()
cur = conn.cursor()

# Check if these ISINs exist in isin_master
test_isins = [
    'INE034S01021',
    'INE258B01022',
    'INE955V01021',
    'INE163A01018',
    'INE910A01012'
]

print("Checking ISIN master for sample ISINs:\n")
for isin in test_isins:
    cur.execute("SELECT isin, canonical_name, sector FROM isin_master WHERE isin = %s", (isin,))
    result = cur.fetchone()
    if result:
        print(f"  ✅ {isin}: '{result[1]}' ({result[2]})")
    else:
        print(f"  ❌ {isin}: NOT FOUND in isin_master")

# Check total ISINs in isin_master
cur.execute("SELECT COUNT(*) FROM isin_master")
total = cur.fetchone()[0]
print(f"\nTotal ISINs in isin_master: {total}")

# Check how many ICICI ISINs are missing
cur.execute("""
    SELECT COUNT(DISTINCT c.isin)
    FROM companies c
    JOIN equity_holdings eh ON c.company_id = eh.company_id
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
    AND c.company_name = 'N/A'
""")
missing_count = cur.fetchone()[0]
print(f"ICICI ISINs with 'N/A' company name: {missing_count}")

cur.close()
conn.close()
