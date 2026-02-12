from src.db.connection import get_cursor, close_connection

def check():
    cursor = get_cursor()
    try:
        cursor.execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'chk_isin_format'")
        row = cursor.fetchone()
        if row:
            print(f"Constraint Definition: {row[0]}")
        else:
            print("Constraint check_isin_format not found.")
            
        cursor.execute("""
            SELECT conname, pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid = 'equity_holdings'::regclass
        """)
        print("\nConstraints on 'equity_holdings':")
        for r in cursor.fetchall():
            print(f"- {r[0]}: {r[1]}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        close_connection()

if __name__ == "__main__":
    check()
