from src.db.connection import get_cursor, close_connection

def check_sbi():
    cursor = get_cursor()
    
    print("\n--- SBI Schemes containing 'Blue' ---")
    cursor.execute("SELECT scheme_name FROM schemes WHERE scheme_name LIKE '%Blue%'")
    for r in cursor.fetchall():
        print(r[0])
        
    print("\n--- SBI Large and Midcap Fund Details (First 5) ---")
    cursor.execute("""
        SELECT h.quantity, h.market_value_inr, h.percent_of_nav
        FROM equity_holdings h
        JOIN scheme_snapshots ss ON h.snapshot_id = ss.snapshot_id
        JOIN schemes s ON ss.scheme_id = s.scheme_id
        WHERE s.scheme_name = 'SBI Large and Midcap Fund'
        LIMIT 5
    """)
    for r in cursor.fetchall():
        print(r)

    close_connection()

if __name__ == "__main__":
    check_sbi()
