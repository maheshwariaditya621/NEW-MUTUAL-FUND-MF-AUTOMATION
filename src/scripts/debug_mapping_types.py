from src.db.connection import get_connection

def debug_mapping():
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Check for schemes with codes not linked
    cur.execute("""
        SELECT amfi_code, scheme_name, scheme_id 
        FROM schemes 
        WHERE amfi_code IS NOT NULL 
          AND amfi_code NOT IN (SELECT DISTINCT scheme_code FROM nav_history WHERE scheme_id IS NOT NULL)
    """)
    not_linked = cur.fetchall()
    
    print(f"Schemes with codes but NOT LINKED in nav_history ({len(not_linked)}):")
    for row in not_linked:
        code, name, s_id = row
        print(f"Code: '{code}' | Name: {name}")
        
        # Check if code exists in nav_history (even if unlinked)
        cur.execute("SELECT scheme_name, scheme_id FROM nav_history WHERE scheme_code = %s", (code,))
        nav_match = cur.fetchone()
        if nav_match:
            print(f"  -> Found in nav_history: '{nav_match[0]}' | Current scheme_id in nav: {nav_match[1]}")
        else:
            # Check for same code with spaces
            cur.execute("SELECT scheme_code FROM nav_history WHERE scheme_code LIKE %s", (f"%{code}%",))
            partial = cur.fetchall()
            if partial:
                print(f"  -> Found partial matches in nav: {partial}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    debug_mapping()
