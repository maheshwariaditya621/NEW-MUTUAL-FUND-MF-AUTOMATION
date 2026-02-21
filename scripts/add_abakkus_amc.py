from src.config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
import psycopg2

def insert_abakkus():
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO amcs (amc_name) VALUES (%s) ON CONFLICT (amc_name) DO NOTHING RETURNING amc_id",
            ('ABAKKUS MUTUAL FUND',)
        )
        res = cur.fetchone()
        conn.commit()
        if res:
            print(f"Inserted Abakkus with ID: {res[0]}")
        else:
            print("Abakkus already exists in DB.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    insert_abakkus()
