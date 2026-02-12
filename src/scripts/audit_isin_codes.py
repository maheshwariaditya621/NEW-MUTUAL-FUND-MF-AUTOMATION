from src.db.connection import get_cursor, close_connection

def audit():
    cursor = get_cursor()
    print("Security Code Distribution for SBI:")
    print("Code | ISINs | Holdings | Sample")
    print("-" * 50)
    
    # Corrected Query
    cursor.execute("""
        SELECT 
            SUBSTRING(c.isin, 9, 2) as sec_code,
            COUNT(DISTINCT c.isin) as distinct_isins,
            COUNT(eh.holding_id) as total_holdings,
            MIN(c.company_name) as sample_name
        FROM equity_holdings eh
        JOIN companies c ON eh.company_id = c.company_id
        JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
        JOIN schemes s ON ss.scheme_id = s.scheme_id
        JOIN amcs a ON s.amc_id = a.amc_id
        JOIN periods p ON ss.period_id = p.period_id
        WHERE a.amc_name = 'SBI Mutual Fund'
          AND p.year = 2025 AND p.month = 12
        GROUP BY sec_code
        ORDER BY total_holdings DESC
    """)
    
    for row in cursor.fetchall():
        print(f"{row[0]}   | {row[1]:<5} | {row[2]:<8} | {row[3]}")
        
    close_connection()

if __name__ == "__main__":
    audit()
