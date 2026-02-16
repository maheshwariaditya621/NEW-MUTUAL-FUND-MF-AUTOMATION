
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

try:
    from src.db.connection import get_cursor, close_connection
    
    cursor = get_cursor()
    
    # Query to get all tables in the public schema
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name;
    """)
    
    tables = [row[0] for row in cursor.fetchall()]
    
    if not tables:
        print("No tables found in public schema.")
    else:
        print(f"Found {len(tables)} tables:")
        print("-" * 50)
        print(f"{'Table Name':<30} | {'Row Count':<15}")
        print("-" * 50)
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table:<30} | {count:<15}")
            except Exception as e:
                print(f"{table:<30} | Error: {e}")
        print("-" * 50)
    
    close_connection()

except Exception as e:
    print(f"Error: {e}")
