from src.db.connection import get_cursor, close_connection

def count_schemes():
    cursor = get_cursor()
    
    print("\n--- Total Schemes ---")
    cursor.execute("SELECT count(*) FROM schemes")
    print(cursor.fetchone()[0])
    
    print("\n--- Schemes by AMC ---")
    cursor.execute("""
        SELECT a.amc_name, count(s.scheme_id) 
        FROM schemes s 
        JOIN amcs a ON s.amc_id = a.amc_id 
        GROUP BY a.amc_name
    """)
    for r in cursor.fetchall():
        print(r)

    print("\n--- Listing 10 HDFC Schemes ---")
    cursor.execute("""
        SELECT s.scheme_name 
        FROM schemes s 
        JOIN amcs a ON s.amc_id = a.amc_id 
        WHERE a.amc_name LIKE '%HDFC%' 
        LIMIT 10
    """)
    for r in cursor.fetchall():
        print(r[0])
        
    close_connection()

if __name__ == "__main__":
    count_schemes()
