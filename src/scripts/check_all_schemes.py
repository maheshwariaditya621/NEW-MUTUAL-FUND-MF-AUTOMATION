from src.db.connection import get_cursor, close_connection

def check_all():
    cursor = get_cursor()
    
    print("\n--- AMCs ---")
    cursor.execute("SELECT amc_name FROM amcs")
    for r in cursor.fetchall():
        print(r[0])
        
    print("\n--- All Schemes (First 50) ---")
    cursor.execute("SELECT scheme_name FROM schemes ORDER BY scheme_name LIMIT 50")
    for r in cursor.fetchall():
        print(r[0])

    print("\n--- Schemes containing 'Top 100' ---")
    cursor.execute("SELECT scheme_name FROM schemes WHERE scheme_name ILIKE '%Top 100%'")
    for r in cursor.fetchall():
        print(r[0])

    close_connection()

if __name__ == "__main__":
    check_all()
