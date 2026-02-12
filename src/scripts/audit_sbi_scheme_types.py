from src.db.connection import get_cursor, close_connection

def audit():
    cursor = get_cursor()
    cursor.execute("""
        SELECT 
            s.scheme_name,
            COUNT(DISTINCT CASE WHEN SUBSTRING(c.isin, 9, 2) = '10' THEN c.isin END) as equity_count,
            COUNT(DISTINCT CASE WHEN SUBSTRING(c.isin, 9, 2) != '10' THEN c.isin END) as other_ine_count,
            COUNT(DISTINCT c.isin) as total_isins
        FROM schemes s
        JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
        JOIN equity_holdings eh ON ss.snapshot_id = eh.snapshot_id
        JOIN companies c ON eh.company_id = c.company_id
        JOIN periods p ON ss.period_id = p.period_id
        JOIN amcs a ON s.amc_id = a.amc_id
        WHERE a.amc_name = 'SBI Mutual Fund'
          AND p.year = 2025 AND p.month = 12
        GROUP BY s.scheme_name
        ORDER BY equity_count ASC, other_ine_count DESC
    """)
    
    print("Scheme Composition (Equity vs Other INE):")
    print(f"{'Scheme Name':<55} | {'Equity(10)':<10} | {'Other(INE)':<10} | {'Total'}")
    print("-" * 90)
    
    schemes = cursor.fetchall()
    
    pure_equity = 0
    mixed = 0
    pure_debt = 0
    
    for row in schemes:
        name = row[0][:50]
        eq = row[1]
        other = row[2]
        total = row[3]
        print(f"{name:<55} | {eq:<10} | {other:<10} | {total}")
        
        if eq > 0:
            pure_equity += 1
        elif other > 0:
            pure_debt += 1
            
    print("-" * 90)
    print(f"Total Schemes: {len(schemes)}")
    print(f"Schemes with AT LEAST 1 Equity Share (Code 10): {pure_equity}")
    print(f"Schemes with ONLY Other INE (Likely Debt): {pure_debt}")

    close_connection()

if __name__ == "__main__":
    audit()
