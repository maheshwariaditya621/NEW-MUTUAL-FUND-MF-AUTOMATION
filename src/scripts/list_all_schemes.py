from src.db.connection import get_connection
import pandas as pd

def list_schemes():
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT scheme_id, scheme_name, amfi_code FROM schemes ORDER BY scheme_name", conn)
        print("scheme_id,scheme_name,amfi_code")
        for index, row in df.iterrows():
            print(f"{row['scheme_id']},{row['scheme_name']},{row['amfi_code']}")
    finally:
        conn.close()

if __name__ == "__main__":
    list_schemes()
