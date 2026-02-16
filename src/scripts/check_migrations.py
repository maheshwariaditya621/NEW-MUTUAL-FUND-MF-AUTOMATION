
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

try:
    from src.db.connection import get_cursor, close_connection
    
    cursor = get_cursor()
    
    # Query to get all applied migrations
    cursor.execute("SELECT migration_name, applied_at FROM schema_migrations ORDER BY applied_at;")
    
    rows = cursor.fetchall()
    
    if not rows:
        print("No migrations found in schema_migrations table.")
    else:
        print(f"Applied Migrations ({len(rows)}):")
        print("-" * 50)
        for name, applied_at in rows:
            print(f"{name:<40} | {applied_at}")
        print("-" * 50)
    
    close_connection()

except Exception as e:
    print(f"Error: {e}")
