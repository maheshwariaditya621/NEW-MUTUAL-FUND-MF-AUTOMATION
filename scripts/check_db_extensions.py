import psycopg2
from src.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
)

def check_extensions():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute("SELECT extname FROM pg_extension;")
        extensions = [row[0] for row in cur.fetchall()]
        print(f"Installed extensions: {extensions}")
        
        if 'pg_trgm' not in extensions:
            print("pg_trgm NOT found. Attempting to create...")
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
                conn.commit()
                print("pg_trgm created successfully.")
            except Exception as e:
                print(f"Failed to create pg_trgm: {e}")
        else:
            print("pg_trgm is already installed.")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error connecting to DB: {e}")

if __name__ == "__main__":
    check_extensions()
