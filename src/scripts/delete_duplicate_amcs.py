from src.db.connection import get_cursor, close_connection, get_connection

def delete_dupes():
    conn = get_connection()
    cursor = conn.cursor()
    
    # IDs to delete (Confirmed empty)
    ids_to_delete = [1, 47]
    
    try:
        print(f"Deleting AMC IDs: {ids_to_delete}")
        cursor.execute("DELETE FROM amcs WHERE amc_id = ANY(%s)", (ids_to_delete,))
        print(f"Deleted {cursor.rowcount} rows.")
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    delete_dupes()
