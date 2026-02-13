
from src.db.connection import get_connection

def check_runs():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT run_id, amc_id, period_id, file_name, status FROM extraction_runs")
    rows = cursor.fetchall()
    
    print(f"Total Runs: {len(rows)}")
    for row in rows:
        print(row)
        
    conn.close()

if __name__ == "__main__":
    check_runs()
