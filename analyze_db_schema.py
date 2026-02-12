"""
Analyze database schema and relationships for company data storage.
"""
from src.db import get_connection

conn = get_connection()
cur = conn.cursor()

print("="*80)
print("DATABASE SCHEMA ANALYSIS")
print("="*80)

# 1. isin_master table
print("\n1. isin_master table:")
cur.execute("""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'isin_master'
    ORDER BY ordinal_position
""")
print("   Columns:")
for row in cur.fetchall():
    print(f"     - {row[0]} ({row[1]}) {'NULL' if row[2] == 'YES' else 'NOT NULL'}")

cur.execute("SELECT COUNT(*) FROM isin_master")
print(f"   Total rows: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(*) FROM isin_master WHERE canonical_name = 'N/A'")
print(f"   Rows with canonical_name='N/A': {cur.fetchone()[0]}")

# 2. companies table
print("\n2. companies table:")
cur.execute("""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'companies'
    ORDER BY ordinal_position
""")
print("   Columns:")
for row in cur.fetchall():
    print(f"     - {row[0]} ({row[1]}) {'NULL' if row[2] == 'YES' else 'NOT NULL'}")

cur.execute("SELECT COUNT(*) FROM companies")
print(f"   Total rows: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(*) FROM companies WHERE company_name = 'N/A'")
print(f"   Rows with company_name='N/A': {cur.fetchone()[0]}")

# 3. equity_holdings table
print("\n3. equity_holdings table:")
cur.execute("""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'equity_holdings'
    ORDER BY ordinal_position
""")
print("   Columns:")
for row in cur.fetchall():
    print(f"     - {row[0]} ({row[1]}) {'NULL' if row[2] == 'YES' else 'NOT NULL'}")

# 4. Foreign key relationships
print("\n4. Foreign Key Relationships:")
cur.execute("""
    SELECT
        tc.table_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_name IN ('equity_holdings', 'companies', 'isin_master')
    ORDER BY tc.table_name
""")
print("   Foreign Keys:")
for row in cur.fetchall():
    print(f"     {row[0]}.{row[1]} → {row[2]}.{row[3]}")

# 5. Sample data flow
print("\n5. Sample Data Flow (1 ICICI holding with 'N/A'):")
cur.execute("""
    SELECT 
        eh.holding_id,
        c.company_id,
        c.company_name,
        c.isin,
        im.canonical_name as isin_master_name,
        c.sector,
        eh.market_value_inr,
        s.scheme_name
    FROM equity_holdings eh
    JOIN companies c ON eh.company_id = c.company_id
    LEFT JOIN isin_master im ON c.isin = im.isin
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
    AND c.company_name = 'N/A'
    LIMIT 1
""")
result = cur.fetchone()
if result:
    print(f"   Holding ID: {result[0]}")
    print(f"   Company ID: {result[1]}")
    print(f"   Company Name (companies table): '{result[2]}'")
    print(f"   ISIN: {result[3]}")
    print(f"   Canonical Name (isin_master): '{result[4]}'")
    print(f"   Sector: {result[5]}")
    print(f"   Market Value: {result[6]}")
    print(f"   Scheme: {result[7]}")

print("\n" + "="*80)
print("KEY FINDINGS:")
print("="*80)
print("1. equity_holdings stores company_id (FK to companies table)")
print("2. companies table has: company_id, company_name, isin, sector, industry")
print("3. isin_master has: isin, canonical_name, sector, industry")
print("4. Data flow: extractor → loader → isin_master + companies → equity_holdings")
print("5. Issue: loader prioritizes isin_master.canonical_name over extractor's company_name")

cur.close()
conn.close()
