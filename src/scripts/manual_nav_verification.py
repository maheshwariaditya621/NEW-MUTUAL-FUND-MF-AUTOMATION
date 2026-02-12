from src.db.connection import get_connection
from src.config import logger
from datetime import date, timedelta

def verify_hdfc_1yr():
    conn = get_connection()
    cur = conn.cursor()
    
    # HDFC Multi Cap Fund - Growth Option (Code: 149366)
    amfi_code = '149366'
    
    # Get Latest NAV (Today-ish)
    cur.execute("""
        SELECT nav_value, nav_date, scheme_id 
        FROM nav_history 
        WHERE scheme_code = %s 
        ORDER BY nav_date DESC 
        LIMIT 1
    """, (amfi_code,))
    curr = cur.fetchone()
    
    # Get NAV ~3 Months Ago
    three_mo_ago = curr[1] - timedelta(days=90)
    cur.execute("""
        SELECT nav_value, nav_date 
        FROM nav_history 
        WHERE scheme_code = %s AND nav_date <= %s 
        ORDER BY nav_date DESC 
        LIMIT 1
    """, (amfi_code, three_mo_ago))
    past = cur.fetchone()
    
    # Get Precomputed Return
    cur.execute("SELECT return_3m FROM scheme_returns WHERE scheme_id = %s", (curr[2],))
    precomputed = cur.fetchone()
    
    print(f"--- VERIFICATION: HDFC Multi Cap (149366) ---")
    print(f"Latest: {curr[0]} on {curr[1]}")
    print(f"3 Months Ago: {past[0]} on {past[1]}")
    
    manual_ret = (float(curr[0]) / float(past[0])) - 1
    manual_ret_pct = manual_ret * 100
    
    print(f"Manual 3M Return: {manual_ret_pct:.4f}%")
    if precomputed:
        print(f"Precomputed 1Y: {precomputed[0]:.4f}%")
        diff = abs(manual_ret_pct - float(precomputed[0]))
        print(f"Difference: {diff:.6f}%")
        if diff < 0.2:
            print("Status: VERIFIED (Tolerance < 0.2%)")
        else:
            print("Status: MISMATCH (Check dividends or date alignment)")
    else:
        print("Status: Precomputed return not found.")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    verify_hdfc_1yr()
