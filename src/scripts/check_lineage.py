from src.db.connection import get_connection

def check_latest_run():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT run_id, git_commit_hash, status FROM extraction_runs ORDER BY run_id DESC LIMIT 1")
    row = cur.fetchone()
    print(f"Latest Run: ID={row[0]}, Hash={row[1]}, Status={row[2]}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_latest_run()
