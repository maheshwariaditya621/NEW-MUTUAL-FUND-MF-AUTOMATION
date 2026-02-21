import psycopg2
from src.config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from datetime import date

def insert_mcx_split():
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cur = conn.cursor()
    
    try:
        # Check columns
        cur.execute("SELECT * FROM corporate_actions LIMIT 0")
        columns = [desc[0] for desc in cur.description]
        print(f"Columns: {columns}")
        
        # MCX Meta
        entity_id = 543
        action_type = 'SPLIT/BONUS'
        ratio_factor = 5.0  # 1:5 split
        effective_date = date(2026, 1, 3) # Record date was Jan 2
        status = 'CONFIRMED'
        description = '1:5 Stock Split'
        
        # Check if already exists
        cur.execute("SELECT id FROM corporate_actions WHERE entity_id = %s AND action_type = %s AND effective_date = %s", 
                    (entity_id, action_type, effective_date))
        if cur.fetchone():
            print("Corporate action for MCX split already exists.")
        else:
            cur.execute("""
                INSERT INTO corporate_actions (entity_id, action_type, ratio_factor, effective_date, status, description, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (entity_id, action_type, ratio_factor, effective_date, status, description, 'MANUAL_ENTRY'))
            print("Inserted MCX 1:5 split into corporate_actions.")
            
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    insert_mcx_split()
