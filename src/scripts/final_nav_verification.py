from src.db.connection import get_connection
from src.config import logger
from datetime import date, timedelta

def verify_any_valid_return():
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Find a scheme with a non-null return_3m
    cur.execute("""
        SELECT sr.scheme_id, s.scheme_name, s.amfi_code, sr.return_3m, sr.latest_nav_date
        FROM scheme_returns sr 
        JOIN schemes s ON sr.scheme_id = s.scheme_id 
        WHERE sr.return_3m IS NOT NULL 
        LIMIT 1
    """)
    res = cur.fetchone()
    if not res:
        print("No precomputed 3M returns found.")
        return
        
    s_id, name, amfi, precomputed_3m, latest_date = res
    print(f"--- VERIFICATION: {name} ({amfi}) ---")
    
    # 2. Get Latest NAV
    cur.execute("SELECT nav_value, nav_date FROM nav_history WHERE scheme_id = %s AND nav_date = %s", (s_id, latest_date))
    curr = cur.fetchone()
    
    # 3. Get NAV 90 Days Ago
    three_mo_ago = latest_date - timedelta(days=90)
    cur.execute("""
        SELECT nav_value, nav_date 
        FROM nav_history 
        WHERE scheme_id = %s AND nav_date <= %s 
        ORDER BY nav_date DESC 
        LIMIT 1
    """, (s_id, three_mo_ago))
    past = cur.fetchone()
    
    if not past:
        print("Could not find NAV from 90 days ago.")
        return
        
    print(f"Latest: {curr[0]} on {curr[1]}")
    print(f"3 Months Ago: {past[0]} on {past[1]}")
    
    manual_ret = (float(curr[0]) / float(past[0])) - 1
    manual_ret_pct = manual_ret * 100
    
    print(f"Manual 3M Return: {manual_ret_pct:.4f}%")
    print(f"Precomputed 3M: {precomputed_3m:.4f}%")
    
    diff = abs(manual_ret_pct - float(precomputed_3m))
    print(f"Difference: {diff:.6f}%")
    
    if diff < 0.2:
        print("Status: VERIFIED")
    else:
        print("Status: MISMATCH")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    verify_any_valid_return()
