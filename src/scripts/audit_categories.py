from src.db.connection import get_connection

def audit_categories():
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT count(*) FROM schemes")
    total_schemes = cur.fetchone()[0]
    
    cur.execute("SELECT count(*) FROM scheme_category_master")
    total_mapped = cur.fetchone()[0]
    
    cur.execute("""
        SELECT count(*) 
        FROM schemes s 
        JOIN scheme_category_master c ON s.amfi_code = c.amfi_code
    """)
    matched_schemes = cur.fetchone()[0]
    
    print(f"Total Schemes in 'schemes' table: {total_schemes}")
    print(f"Total entries in 'scheme_category_master': {total_mapped}")
    print(f"Schemes correctly matched to a category: {matched_schemes}")
    
    if matched_schemes < total_schemes:
        print("\nTop unmatched schemes:")
        cur.execute("""
            SELECT s.amfi_code, s.scheme_name 
            FROM schemes s 
            LEFT JOIN scheme_category_master c ON s.amfi_code = c.amfi_code 
            WHERE c.amfi_code IS NULL 
            LIMIT 10
        """)
        for r in cur.fetchall():
            print(f"- {r[0]}: {r[1]}")
            
    cur.close()
    conn.close()

if __name__ == "__main__":
    audit_categories()
