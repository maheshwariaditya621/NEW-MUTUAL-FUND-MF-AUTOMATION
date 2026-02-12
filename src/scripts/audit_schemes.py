from src.db.connection import get_cursor, close_connection
from src.config.constants import AMC_HDFC, AMC_SBI

def audit_schemes():
    cursor = get_cursor()
    
    print(f"\nAUDIT REPORT: {AMC_HDFC} vs {AMC_SBI}")
    print("="*60)

    # 1. Total Counts
    cursor.execute("""
        SELECT a.amc_name, count(s.scheme_id)
        FROM schemes s
        JOIN amcs a ON s.amc_id = a.amc_id
        GROUP BY a.amc_name
    """)
    totals = cursor.fetchall()
    print("TOTAL SCHEME COUNTS:")
    for r in totals:
        print(f"  {r[0]}: {r[1]}")
        
    # 2. Duplicate Name Check (Same Name, Different IDs)
    print("\n POTENTIAL DUPLICATES (Same Name check):")
    cursor.execute("""
        SELECT s.scheme_name, count(*) 
        FROM schemes s
        GROUP BY s.scheme_name
        HAVING count(*) > 1
    """)
    dupes = cursor.fetchall()
    if not dupes:
        print("  [Pass] No schemes managed by different AMCs share the EXACT same name string.")
    else:
        for r in dupes:
            print(f"  [WARN] '{r[0]}' appears {r[1]} times.")

    # 3. Deep Dive: SBI vs HDFC structure
    print("\n DATA STRUCTURE ANALYSIS:")
    for amc_name in [AMC_HDFC, AMC_SBI]:
        print(f"\n--- {amc_name} ---")
        cursor.execute("""
            SELECT s.plan_type, s.option_type, count(*)
            FROM schemes s
            JOIN amcs a ON s.amc_id = a.amc_id
            WHERE a.amc_name = %s
            GROUP BY s.plan_type, s.option_type
            ORDER BY count(*) DESC
        """, (amc_name,))
        breakdown = cursor.fetchall()
        for r in breakdown:
            print(f"  Plan: {r[0]:<10} | Option: {r[1]:<15} | Count: {r[2]}")

    # 4. List SBI Schemes with "Direct" or "Growth" to see proliferation
    print("\n SAMPLE SBI SCHEMES (First 10):")
    cursor.execute("""
        SELECT s.scheme_name, s.plan_type, s.option_type
        FROM schemes s
        JOIN amcs a ON s.amc_id = a.amc_id
        WHERE a.amc_name = %s
        LIMIT 10
    """, (AMC_SBI,))
    sbi_schemes = cursor.fetchall()
    for r in sbi_schemes:
        print(f"  {r[0]} ({r[1]} - {r[2]})")

    close_connection()

if __name__ == "__main__":
    audit_schemes()
