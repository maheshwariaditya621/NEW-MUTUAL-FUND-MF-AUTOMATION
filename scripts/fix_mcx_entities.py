import psycopg2
from src.config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def fix_entities():
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cur = conn.cursor()
    
    try:
        # 1. Update logical references in all linked tables
        for table in ["companies", "isin_master", "corporate_actions"]:
            cur.execute(f"UPDATE {table} SET entity_id = 543 WHERE entity_id = 9675")
            print(f"Updated {cur.rowcount} rows in {table} to entity 543.")
            
        cur.execute("UPDATE resolution_audit SET resolved_entity_id = 543 WHERE resolved_entity_id = 9675")
        print(f"Updated {cur.rowcount} rows in resolution_audit to entity 543.")
        
        # 2. Update canonical name for entity 543
        cur.execute("UPDATE corporate_entities SET canonical_name = 'Multi Commodity Exchange of India Ltd' WHERE entity_id = 543")
        print(f"Updated entity 543 canonical name.")
        
        # 3. Delete the redundant entity 9675
        cur.execute("DELETE FROM corporate_entities WHERE entity_id = 9675")
        print(f"Deleted redundant entity 9675.")
        
        conn.commit()
        print("Successfully committed changes.")
    except Exception as e:
        conn.rollback()
        print(f"Error occurred: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    fix_entities()
