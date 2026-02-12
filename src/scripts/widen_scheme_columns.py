from src.db.connection import get_connection

def migrate():
    print("Widening plan_type and option_type in schemes table...")
    conn = get_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:
        # 1. Widen plan_type
        print("Altering plan_type to VARCHAR(50)...")
        cursor.execute("ALTER TABLE schemes ALTER COLUMN plan_type TYPE VARCHAR(50)")
        
        # 2. Widen option_type
        print("Altering option_type to VARCHAR(50)...")
        cursor.execute("ALTER TABLE schemes ALTER COLUMN option_type TYPE VARCHAR(50)")
        
        print("SUCCESS: Columns widened.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
