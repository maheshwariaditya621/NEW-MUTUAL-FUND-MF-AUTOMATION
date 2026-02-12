from src.db.connection import get_connection

def delete_orphans():
    conn = get_connection()
    cursor = conn.cursor()
    
    print("Finding orphan schemes (0 holdings)...")
    
    # Find IDs
    cursor.execute("""
        SELECT s.scheme_id, s.scheme_name, a.amc_name
        FROM schemes s
        JOIN amcs a ON s.amc_id = a.amc_id
        LEFT JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
        GROUP BY s.scheme_id, s.scheme_name, a.amc_name
        HAVING COUNT(ss.snapshot_id) = 0
    """)
    orphans = cursor.fetchall()
    
    if not orphans:
        print("No orphan schemes found.")
        return

    print(f"Found {len(orphans)} orphans.")
    ids_to_delete = [r[0] for r in orphans]
    
    # Delete
    print(f"Deleting {len(ids_to_delete)} schemes...")
    cursor.execute("DELETE FROM schemes WHERE scheme_id = ANY(%s)", (ids_to_delete,))
    deleted = cursor.rowcount
    
    conn.commit()
    print(f"Successfully deleted {deleted} orphan schemes.")
    
    keys = {}
    for r in orphans:
        amc = r[2]
        keys[amc] = keys.get(amc, 0) + 1
        
    print("Breakdown by AMC:")
    for k, v in keys.items():
        print(f"  {k}: {v} deleted")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    delete_orphans()
