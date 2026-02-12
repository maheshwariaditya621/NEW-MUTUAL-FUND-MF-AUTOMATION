"""
Test the fix by reloading ICICI data and checking if company names are updated.
"""
from src.db import get_connection
from src.extractors.orchestrator import ExtractionOrchestrator
from src.config.constants import AMC_ICICI
from pathlib import Path
import sys

print("="*80)
print("TESTING COMPANY NAME FIX")
print("="*80)

# 1. Check current state
conn = get_connection()
cur = conn.cursor()

print("\n1. BEFORE reload - Sample ISINs with 'N/A':")
test_isins = ['INE034S01021', 'INE258B01022', 'INE955V01021']
for isin in test_isins:
    cur.execute("""
        SELECT canonical_name FROM isin_master WHERE isin = %s
    """, (isin,))
    result = cur.fetchone()
    if result:
        print(f"   {isin}: '{result[0]}'")

cur.close()
conn.close()

# 2. Delete ICICI data to allow reload
print("\n2. Deleting existing ICICI data...")
conn = get_connection()
cur = conn.cursor()

# Delete in correct order (FK constraints)
cur.execute("""
    DELETE FROM equity_holdings
    WHERE snapshot_id IN (
        SELECT ss.snapshot_id
        FROM scheme_snapshots ss
        JOIN schemes s ON ss.scheme_id = s.scheme_id
        JOIN amcs a ON s.amc_id = a.amc_id
        WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
    )
""")
deleted_holdings = cur.rowcount
print(f"   Deleted {deleted_holdings} holdings")

cur.execute("""
    DELETE FROM scheme_snapshots
    WHERE scheme_id IN (
        SELECT s.scheme_id
        FROM schemes s
        JOIN amcs a ON s.amc_id = a.amc_id
        WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
    )
""")
deleted_snapshots = cur.rowcount
print(f"   Deleted {deleted_snapshots} snapshots")

cur.execute("""
    DELETE FROM schemes
    WHERE amc_id IN (
        SELECT amc_id FROM amcs WHERE amc_name = 'ICICI Prudential Mutual Fund'
    )
""")
deleted_schemes = cur.rowcount
print(f"   Deleted {deleted_schemes} schemes")

conn.commit()
cur.close()
conn.close()

# 3. Reload ICICI data
print("\n3. Reloading ICICI data with fixed loader...")
orchestrator = ExtractionOrchestrator()

try:
    result = orchestrator.process_amc_month(
        amc_slug="icici",
        year=2025,
        month=12,
        redo=True  # Force reload
    )
    
    print(f"   ✅ Extraction completed")
    print(f"   Holdings inserted: {result.get('holdings_inserted', 0)}")
    print(f"   Schemes processed: {result.get('schemes_processed', 0)}")
    
except Exception as e:
    print(f"   ❌ Extraction failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 4. Check updated state
print("\n4. AFTER reload - Sample ISINs (should have real names now):")
conn = get_connection()
cur = conn.cursor()

for isin in test_isins:
    cur.execute("""
        SELECT im.canonical_name, c.company_name
        FROM isin_master im
        LEFT JOIN companies c ON im.isin = c.isin
        WHERE im.isin = %s
    """, (isin,))
    result = cur.fetchone()
    if result:
        print(f"   {isin}:")
        print(f"     isin_master: '{result[0]}'")
        print(f"     companies:   '{result[1]}'")

# 5. Count remaining N/A entries
cur.execute("SELECT COUNT(*) FROM isin_master WHERE canonical_name = 'N/A'")
na_count = cur.fetchone()[0]
print(f"\n5. Remaining 'N/A' entries in isin_master: {na_count}")

cur.close()
conn.close()

print("\n" + "="*80)
print("TEST COMPLETE")
print("="*80)
