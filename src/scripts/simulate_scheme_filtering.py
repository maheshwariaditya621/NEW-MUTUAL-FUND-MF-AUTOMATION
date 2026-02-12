from src.db.connection import get_cursor, close_connection

def simulate():
    cursor = get_cursor()
    cursor.execute("""
        SELECT DISTINCT s.scheme_name
        FROM schemes s
        JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
        JOIN periods p ON ss.period_id = p.period_id
        JOIN amcs a ON s.amc_id = a.amc_id
        WHERE a.amc_name = 'SBI Mutual Fund'
          AND p.year = 2025 AND p.month = 12
    """)
    all_schemes = [r[0] for r in cursor.fetchall()]
    
    # Exclusion Keywords for Pure Debt
    exclude_keywords = [
        "Liquid", "Overnight", "Bond", "Debt", "Gilt", "Fixed Maturity Plan", "FMP", 
        "Savings Fund", # e.g. SBI Savings Fund (not Equity Savings)
        "Credit Risk", "Medium Duration", "Short Term", "Ultra Short", "Low Duration",
        "Floating Rate"
    ]
    
    kept = []
    dropped = []
    
    for s in all_schemes:
        is_dropped = False
        # Special Case: "Equity Savings" contains "Savings" but is Equity-like
        if "Equity Savings" in s:
            kept.append(s)
            continue
            
        for kw in exclude_keywords:
            if kw.upper() in s.upper():
                dropped.append(s)
                is_dropped = True
                break
        
        if not is_dropped:
            kept.append(s)
            
    print(f"Total Schemes: {len(all_schemes)}")
    print(f"Dropped: {len(dropped)}")
    print(f"Kept: {len(kept)}")
    
    print("\n--- DROPPED SCHEMES ---")
    for s in sorted(dropped):
        print(f"[X] {s}")

    print("\n--- KEPT SCHEMES ---")
    for s in sorted(kept):
        print(f"[OK] {s}")
        
    close_connection()

if __name__ == "__main__":
    simulate()
