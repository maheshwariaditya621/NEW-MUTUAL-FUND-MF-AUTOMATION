import psycopg2
from src.config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def cleanup():
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cur = conn.cursor()
    
    # Reset entity_id for companies that are definitely NOT Larsen & Toubro but were linked to entity 823 (LT)
    cur.execute("""
        UPDATE companies 
        SET entity_id = NULL 
        WHERE entity_id = 823 
        AND company_name NOT ILIKE '%LARSEN%' 
        AND company_name NOT ILIKE '%L&T%'
    """)
    print(f"Reset {cur.rowcount} wrong links to LT.")
    
    # Also reset MCX 035 just in case
    cur.execute("UPDATE companies SET entity_id = NULL WHERE isin = 'INE745G01035'")
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    cleanup()
