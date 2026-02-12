from src.db.connection import get_connection

def list_cats():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT broad_category, scheme_category FROM scheme_category_master ORDER BY 1, 2")
    for r in cur.fetchall():
        print(f"{r[0]}: {r[1]}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    list_cats()
