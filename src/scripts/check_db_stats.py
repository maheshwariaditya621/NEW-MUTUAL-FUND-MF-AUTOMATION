from src.db.connection import get_connection

def check_stats():
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM schemes WHERE amfi_code IS NOT NULL")
    schemes_with_code = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM nav_history")
    total_nav_rows = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM nav_history WHERE scheme_id IS NOT NULL")
    nav_with_id = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT scheme_code) FROM nav_history WHERE scheme_id IS NOT NULL")
    distinct_mapped_codes = cur.fetchone()[0]

    print(f"Schemes with AMFI Code: {schemes_with_code}")
    print(f"Total NAV History Rows: {total_nav_rows}")
    print(f"NAV Rows with Scheme ID: {nav_with_id}")
    print(f"Distinct AMFI Codes successfully linked: {distinct_mapped_codes}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_stats()
