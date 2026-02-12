from src.db.connection import get_connection

def debug_hdfc():
    conn = get_connection()
    cur = conn.cursor()
    
    amfi_code = '149366'
    
    cur.execute("SELECT scheme_id, scheme_name FROM schemes WHERE amfi_code = %s", (amfi_code,))
    scheme = cur.fetchone()
    if not scheme:
        print(f"AMC Code {amfi_code} not found in schemes table.")
        return
        
    scheme_id = scheme[0]
    print(f"Scheme found: ID={scheme_id}, Name={scheme[1]}")
    
    cur.execute("SELECT count(*) FROM nav_history WHERE scheme_id = %s", (scheme_id,))
    nav_count = cur.fetchone()[0]
    print(f"NAV entries for this scheme: {nav_count}")
    
    cur.execute("SELECT return_3m FROM scheme_returns WHERE scheme_id = %s", (scheme_id,))
    ret_3m = cur.fetchone()
    print(f"Precomputed 3M Return: {ret_3m}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    debug_hdfc()
