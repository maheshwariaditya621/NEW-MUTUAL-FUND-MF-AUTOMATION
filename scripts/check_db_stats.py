import psycopg2
from src.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
)

def check_indexes():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        
        print("--- Checking Indexes ---")
        cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE tablename IN ('companies', 'schemes');")
        for row in cur.fetchall():
            print(f"Table: {row[0]}, Definition: {row[1]}")
            
        print("\n--- Creating/Checking GIN Indexes for Fuzzy Search ---")
        try:
            # Companies index
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_companies_name_trgm 
                ON companies USING gin (company_name gin_trgm_ops);
            """)
            # Schemes index
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_schemes_name_trgm 
                ON schemes USING gin (scheme_name gin_trgm_ops);
            """)
            conn.commit()
            print("GIN indexes checked/created.")
        except Exception as e:
            print(f"Failed to create indexes: {e}")
            conn.rollback()
            
        print("\n--- Checking Row Counts ---")
        for table in ['companies', 'schemes', 'equity_holdings']:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            print(f"Table {table}: {cur.fetchone()[0]} rows")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_indexes()
