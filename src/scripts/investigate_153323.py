from src.db.connection import get_connection

def investigate_mapping():
    conn = get_connection()
    cur = conn.cursor()
    
    amfi_code = '153323'
    
    # 1. Check AMFI Entry
    cur.execute("""
        SELECT scheme_name, plan_type, option_type, nav_value, nav_date 
        FROM nav_history 
        WHERE scheme_code = %s
    """, (amfi_code,))
    amfi_row = cur.fetchone()
    
    # 2. Check Our Scheme Entry
    cur.execute("""
        SELECT scheme_id, scheme_name, plan_type, option_type, amfi_code 
        FROM schemes 
        WHERE amfi_code = %s
    """, (amfi_code,))
    scheme_row = cur.fetchone()
    
    print(f"--- AMFI SIDE (Code: {amfi_code}) ---")
    if amfi_row:
        print(f"Name: {amfi_row[0]}")
        print(f"Plan: {amfi_row[1]} | Option: {amfi_row[2]}")
        print(f"Latest NAV: {amfi_row[3]} ({amfi_row[4]})")
    else:
        print("No AMFI entry found.")
        
    print(f"\n--- OUR DATABASE SIDE ---")
    if scheme_row:
        print(f"Scheme ID: {scheme_row[0]}")
        print(f"Extracted Name: {scheme_row[1]}")
        print(f"Plan: {scheme_row[2]} | Option: {scheme_row[3]}")
    else:
        print("No mapping found in our 'schemes' table.")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    investigate_mapping()
