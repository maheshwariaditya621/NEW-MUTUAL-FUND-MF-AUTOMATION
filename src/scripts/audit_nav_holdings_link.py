from src.db.connection import get_connection
from src.config import logger

def audit_nav_links():
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Schemes with holdings but NO NAV history
    cur.execute("""
        SELECT DISTINCT s.scheme_id, s.scheme_name, s.amfi_code
        FROM schemes s
        JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
        LEFT JOIN nav_history nh ON s.scheme_id = nh.scheme_id
        WHERE nh.scheme_id IS NULL
    """)
    missing_nav = cur.fetchall()
    
    print("\n--- AUDIT: SCHEMES WITH HOLDINGS BUT NO NAV ---")
    if missing_nav:
        for s in missing_nav:
            print(f"ID: {s[0]} | Name: {s[1]} | AMFI Code: {s[2]}")
        print(f"Total Missing NAV: {len(missing_nav)}")
    else:
        print("All schemes with holdings have NAV history.")

    # 2. Schemes with NAV history but NO holdings (Ghost schemes or mapping errors)
    cur.execute("""
        SELECT DISTINCT s.scheme_id, s.scheme_name, s.amfi_code
        FROM schemes s
        JOIN nav_history nh ON s.scheme_id = nh.scheme_id
        LEFT JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
        WHERE ss.scheme_id IS NULL
    """)
    extra_nav = cur.fetchall()
    
    print("\n--- AUDIT: SCHEMES WITH NAV BUT NO HOLDINGS ---")
    if extra_nav:
        # Note: This is common if we just started AMFI ingest but haven't extracted all months
        print(f"Total schemes with NAV but no holdings: {len(extra_nav)}")
        print("Top 5 examples:")
        for s in extra_nav[:5]:
            print(f"ID: {s[0]} | Name: {s[1]} | AMFI Code: {s[2]}")
    else:
        print("All schemes with NAV have holdings.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    audit_nav_links()
