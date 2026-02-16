
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

try:
    from src.db.connection import get_cursor, close_connection
    
    cursor = get_cursor()
    
    # Get column names for schema_migrations
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'schema_migrations'
        ORDER BY ordinal_position;
    """)
    
    columns = cursor.fetchall()
    print("Columns in schema_migrations:")
    for col, dtype in columns:
        print(f"- {col} ({dtype})")
    
    print("\nData in schema_migrations:")
    cursor.execute("SELECT * FROM schema_migrations;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    
    close_connection()

except Exception as e:
    print(f"Error: {e}")
