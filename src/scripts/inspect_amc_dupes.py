from src.db.connection import get_cursor, close_connection

def inspect_dupes():
    cursor = get_cursor()
    
    print("\n--- AMCs ---")
    cursor.execute("SELECT amc_id, amc_name FROM amcs")
    amcs = cursor.fetchall()
    for r in amcs:
        print(f"ID: {r[0]} | Name: '{r[1]}'")
        
    print("\n--- Scheme Counts by AMC ID ---")
    cursor.execute("""
        SELECT amc_id, count(*) 
        FROM schemes 
        GROUP BY amc_id
    """)
    counts = cursor.fetchall()
    for r in counts:
        print(f"AMC ID: {r[0]} | Scheme Count: {r[1]}")

    close_connection()

if __name__ == "__main__":
    inspect_dupes()
