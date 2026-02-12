from src.db.connection import get_connection

def migrate():
    print("Relaxing equity_holdings_percent_of_nav_check...")
    conn = get_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:
        # Drop old constraint
        print("Dropping constraint...")
        cursor.execute("ALTER TABLE equity_holdings DROP CONSTRAINT IF EXISTS equity_holdings_percent_of_nav_check")
        
        # Add new relaxed constraint
        print("Adding relaxed constraint (-1000 to 1000)...")
        cursor.execute("""
            ALTER TABLE equity_holdings 
            ADD CONSTRAINT equity_holdings_percent_of_nav_check 
            CHECK (percent_of_nav >= -1000 AND percent_of_nav <= 1000)
        """)
        
        print("SUCCESS: Constraint updated.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
