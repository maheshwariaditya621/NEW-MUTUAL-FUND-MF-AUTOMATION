from src.db import get_connection
import pandas as pd

def check_icici_schemes():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            query = """
            SELECT s.scheme_name, p.year, p.month, sn.total_holdings, sn.total_value_inr
            FROM schemes s
            JOIN amcs a ON s.amc_id = a.amc_id
            JOIN scheme_snapshots sn ON sn.scheme_id = s.scheme_id
            JOIN periods p ON sn.period_id = p.period_id
            WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
            ORDER BY s.scheme_name;
            """
            cur.execute(query)
            rows = cur.fetchall()
            
            if not rows:
                print("No ICICI holdings found in DB.")
                return
                
            df = pd.DataFrame(rows, columns=['Scheme', 'Year', 'Month', 'Holdings', 'Value'])
            print(df.to_string(index=False))
    finally:
        conn.close()

if __name__ == "__main__":
    check_icici_schemes()
