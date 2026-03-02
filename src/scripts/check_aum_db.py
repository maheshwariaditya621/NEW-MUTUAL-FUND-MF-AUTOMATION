from src.db.connection import get_connection

def check_db():
    conn = get_connection()
    cur = conn.cursor()
    
    print("--- Non-Equity Schemes ---")
    cur.execute("""
        SELECT s.scheme_name, ss.total_holdings, ss.total_net_assets_inr 
        FROM scheme_snapshots ss 
        JOIN schemes s ON ss.scheme_id = s.scheme_id 
        WHERE s.amc_id = (SELECT amc_id FROM amcs WHERE amc_name = 'SBI MUTUAL FUND') 
        AND ss.total_holdings = 0 
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"{row[0][:40]:<40} | Holdings: {row[1]:>3} | AUM: {row[2]}")

    print("\n--- Equity Schemes ---")
    cur.execute("""
        SELECT s.scheme_name, ss.total_holdings, ss.total_net_assets_inr 
        FROM scheme_snapshots ss 
        JOIN schemes s ON ss.scheme_id = s.scheme_id 
        WHERE s.amc_id = (SELECT amc_id FROM amcs WHERE amc_name = 'SBI MUTUAL FUND') 
        AND ss.total_holdings > 0 
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"{row[0][:40]:<40} | Holdings: {row[1]:>3} | AUM: {row[2]}")

if __name__ == "__main__":
    check_db()
