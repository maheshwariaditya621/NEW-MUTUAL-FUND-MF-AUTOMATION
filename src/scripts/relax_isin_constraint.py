from src.db.connection import get_cursor, close_connection, get_connection

def migrate():
    print("Relaxing chk_isin_format constraint on 'companies' table...")
    conn = get_connection()
    conn.autocommit = True # DDL requires autocommit or commit after
    cursor = conn.cursor()
    
    try:
        # 1. Drop old constraint
        print("Dropping old constraint...")
        cursor.execute("ALTER TABLE companies DROP CONSTRAINT IF EXISTS chk_isin_format")
        
        # 2. Add new relaxed constraint
        # Allow any 12-char pattern starting with INE
        print("Adding new relaxed constraint...")
        cursor.execute("""
            ALTER TABLE companies 
            ADD CONSTRAINT chk_isin_format 
            CHECK (isin ~ '^IN[A-Z0-9]{10}$')
        """)
        
        print("SUCCESS: Constraint updated.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
