from src.db.connection import get_cursor, close_connection

def check_content():
    cursor = get_cursor()
    
    print("\n--- Available Periods ---")
    cursor.execute("SELECT DISTINCT year, month FROM periods ORDER BY year, month")
    for r in cursor.fetchall():
        print(r)
        
    print("\n--- Sample HDFC Schemes ---")
    cursor.execute("SELECT scheme_name FROM schemes WHERE scheme_name LIKE '%HDFC%' LIMIT 10")
    for r in cursor.fetchall():
        print(r[0])

    print("\n--- Sample Companies (HDFC Bank) ---")
    cursor.execute("SELECT company_name FROM companies WHERE company_name ILIKE '%HDFC Bank%' LIMIT 5")
    for r in cursor.fetchall():
        print(r[0])
        
    close_connection()

if __name__ == "__main__":
    check_content()
