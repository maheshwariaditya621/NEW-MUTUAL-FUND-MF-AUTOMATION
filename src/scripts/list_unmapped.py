from src.db.connection import get_connection

def list_unmapped():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT scheme_name FROM schemes WHERE amfi_code IS NULL")
    for r in cur.fetchall():
        print(f"'{r[0]}'")
    cur.close()
    conn.close()

if __name__ == "__main__":
    list_unmapped()
