"""
Delete all ICICI data from database and reload with fresh extraction.
"""
from src.db import get_connection
from src.config import logger

conn = get_connection()
cur = conn.cursor()

try:
    # Start transaction
    conn.autocommit = False
    
    # Get ICICI AMC ID
    cur.execute("SELECT amc_id FROM amcs WHERE amc_name LIKE '%ICICI%'")
    result = cur.fetchone()
    if not result:
        print("No ICICI AMC found in database")
        exit(0)
    
    amc_id = result[0]
    print(f"Found ICICI AMC with ID: {amc_id}")
    
    # Get scheme IDs
    cur.execute("SELECT scheme_id FROM schemes WHERE amc_id = %s", (amc_id,))
    scheme_ids = [row[0] for row in cur.fetchall()]
    print(f"Found {len(scheme_ids)} ICICI schemes")
    
    if not scheme_ids:
        print("No ICICI schemes to delete")
        exit(0)
    
    # Get snapshot IDs
    cur.execute(f"SELECT snapshot_id FROM scheme_snapshots WHERE scheme_id IN ({','.join(['%s']*len(scheme_ids))})", scheme_ids)
    snapshot_ids = [row[0] for row in cur.fetchall()]
    print(f"Found {len(snapshot_ids)} snapshots")
    
    # Delete holdings first (foreign key constraint)
    if snapshot_ids:
        cur.execute(f"DELETE FROM equity_holdings WHERE snapshot_id IN ({','.join(['%s']*len(snapshot_ids))})", snapshot_ids)
        deleted_holdings = cur.rowcount
        print(f"Deleted {deleted_holdings} holdings")
    
    # Delete snapshots
    if scheme_ids:
        cur.execute(f"DELETE FROM scheme_snapshots WHERE scheme_id IN ({','.join(['%s']*len(scheme_ids))})", scheme_ids)
        deleted_snapshots = cur.rowcount
        print(f"Deleted {deleted_snapshots} snapshots")
    
    # Delete schemes
    cur.execute("DELETE FROM schemes WHERE amc_id = %s", (amc_id,))
    deleted_schemes = cur.rowcount
    print(f"Deleted {deleted_schemes} schemes")
    
    # Commit transaction
    conn.commit()
    print("\n✅ Successfully deleted all ICICI data from database")
    print(f"   - {deleted_holdings} holdings")
    print(f"   - {deleted_snapshots} snapshots")
    print(f"   - {deleted_schemes} schemes")
    
except Exception as e:
    conn.rollback()
    print(f"\n❌ Error deleting ICICI data: {e}")
    raise
finally:
    cur.close()
    conn.close()
