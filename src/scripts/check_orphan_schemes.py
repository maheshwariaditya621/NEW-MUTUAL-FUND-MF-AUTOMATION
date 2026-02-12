from src.db.connection import get_cursor, close_connection
from src.config.constants import AMC_SBI

def check_orphans():
    cursor = get_cursor()
    
    print(f"\nCHECKING ORPHANS FOR {AMC_SBI}")
    print("="*60)
    
    # Check schemes with NO holdings (orphans)
    cursor.execute("""
        SELECT s.scheme_name, s.scheme_id
        FROM schemes s
        JOIN amcs a ON s.amc_id = a.amc_id
        LEFT JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
        WHERE a.amc_name = %s
        GROUP BY s.scheme_id, s.scheme_name
        HAVING COUNT(ss.snapshot_id) = 0
    """, (AMC_SBI,))
    
    orphans = cursor.fetchall()
    print(f"Found {len(orphans)} Orphan Schemes (0 Holdings).")
    if orphans:
        print("Sample Orphans:")
        for r in orphans[:10]:
            print(f"  ID {r[1]}: {r[0]}")
            
    # Check schemes WITH holdings (Active)
    cursor.execute("""
        SELECT s.scheme_name, count(ss.snapshot_id)
        FROM schemes s
        JOIN amcs a ON s.amc_id = a.amc_id
        JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
        WHERE a.amc_name = %s
        GROUP BY s.scheme_name
        ORDER BY s.scheme_name
    """, (AMC_SBI,))
    
    active = cursor.fetchall()
    print(f"\nFound {len(active)} Active Schemes (With Holdings).")
    print("Sample Active:")
    for r in active[:10]:
        print(f"  {r[0]} ({r[1]} snapshots)")

    close_connection()

if __name__ == "__main__":
    check_orphans()
