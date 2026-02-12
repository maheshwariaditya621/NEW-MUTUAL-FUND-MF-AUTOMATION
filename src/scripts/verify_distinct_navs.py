from src.db.connection import get_connection

def verify_distinct_navs():
    conn = get_connection()
    cur = conn.cursor()
    
    # Check "SBI Bluechip" for Regular vs Direct or Growth vs IDCW
    cur.execute("""
        SELECT s.scheme_name, s.plan_type, s.option_type, s.amfi_code, n.nav_value 
        FROM schemes s 
        JOIN nav_history n ON s.amfi_code = n.scheme_code 
        WHERE s.scheme_name ILIKE '%Bluechip%' 
          AND n.nav_date = '2026-02-11'
        ORDER BY s.plan_type, s.option_type
    """)
    results = cur.fetchall()
    
    print("Distinct NAV Verification (Latest Date: 2026-02-11):")
    if results:
        for r in results:
            print(f"[{r[3]}] {r[1]} {r[2]} ({r[0]}) | NAV: {r[4]}")
    else:
        print("No matching records found for Bluechip.")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    verify_distinct_navs()
