from src.db.connection import get_connection

def find_unmatched():
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT amfi_code, scheme_name 
        FROM schemes 
        WHERE amfi_code IS NOT NULL 
          AND amfi_code NOT IN (SELECT DISTINCT scheme_code FROM nav_history)
    """)
    unmatched = cur.fetchall()
    
    print(f"Unmatched Schemes ({len(unmatched)}):")
    for amfi_code, name in unmatched:
        print(f"[{amfi_code}] {name}")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    find_unmatched()
