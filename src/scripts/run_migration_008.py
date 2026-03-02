from src.db.connection import get_connection

def run_migration():
    conn = get_connection()
    cur = conn.cursor()
    with open(r'database\migrations\008_add_total_aum.sql', 'r') as f:
        sql = f.read()
    cur.execute(sql)
    conn.commit()
    print("Migration successful")

if __name__ == "__main__":
    run_migration()
