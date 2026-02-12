from src.db.connection import get_connection

def verify_returns():
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT s.scheme_name, sr.return_1m, sr.return_3m 
        FROM scheme_returns sr 
        JOIN schemes s ON sr.scheme_id = s.scheme_id 
        WHERE sr.return_3m IS NOT NULL 
        LIMIT 10
    """)
    results = cur.fetchall()
    
    print("Precomputed Returns Verification (HDFC Backfill):")
    if results:
        for name, r1m, r3m in results:
            print(f"Scheme: {name}")
            print(f"  Return 1M: {r1m:.2f}%")
            print(f"  Return 3M: {r3m:.2f}%")
    else:
        print("No precomputed 3-month returns found yet. Check if backfill is complete.")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    verify_returns()
