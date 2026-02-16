
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.db.connection import get_cursor, close_connection

def check_nav_linking():
    cursor = get_cursor()
    
    cursor.execute("SELECT COUNT(*) FROM nav_history WHERE scheme_id IS NOT NULL")
    linked = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM nav_history")
    total = cursor.fetchone()[0]
    
    print(f"Linked: {linked}")
    print(f"Total: {total}")
    
    close_connection()

if __name__ == "__main__":
    check_nav_linking()
