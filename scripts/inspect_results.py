import psycopg2
from src.config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def inspect_data():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    
    print("--- CORPORATE ACTIONS ---")
    cur.execute("SELECT id, entity_id, action_type, ratio_factor, effective_date, status, source FROM corporate_actions")
    for row in cur.fetchall():
        print(row)
        
    print("\n--- CORPORATE ENTITIES (Sample) ---")
    cur.execute("SELECT entity_id, canonical_name, group_symbol FROM corporate_entities WHERE canonical_name ILIKE '%RELIANCE%' OR canonical_name ILIKE '%KOTAK%' LIMIT 5")
    for row in cur.fetchall():
        print(row)
        
    print("\n--- RESOLUTION AUDIT (Last 5) ---")
    cur.execute("SELECT audit_id, isin, raw_name, resolved_entity_id, resolution_tier FROM resolution_audit ORDER BY created_at DESC LIMIT 5")
    for row in cur.fetchall():
        print(row)
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect_data()
