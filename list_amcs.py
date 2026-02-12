from src.db import get_connection

def list_amcs():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT amc_name FROM amcs ORDER BY amc_name;")
            rows = cur.fetchall()
            for row in rows:
                print(row[0])
    finally:
        conn.close()

if __name__ == "__main__":
    list_amcs()
