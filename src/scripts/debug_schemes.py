from src.db import get_connection
import pandas as pd

def debug_schemes():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            query = "SELECT amc_id, amc_name FROM amcs WHERE amc_name LIKE '%ICICI%';"
            cur.execute(query)
            print("AMCS:", cur.fetchall())
            
            query = "SELECT scheme_id, amc_id, scheme_name, plan_type, option_type FROM schemes WHERE amc_id = (SELECT amc_id FROM amcs WHERE amc_name = 'ICICI Prudential Mutual Fund');"
            cur.execute(query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=['ID', 'AMC_ID', 'Name', 'Plan', 'Option'])
            print(df.to_string(index=False))
    finally:
        conn.close()

if __name__ == "__main__":
    debug_schemes()
