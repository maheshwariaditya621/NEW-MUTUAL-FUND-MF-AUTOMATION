import psycopg2
from src.config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def check_completeness():
    conn = psycopg2.connect(
        host=DB_HOST, 
        port=DB_PORT, 
        dbname=DB_NAME, 
        user=DB_USER, 
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    
    # Define target periods
    target_periods = [
        (2025, 11),
        (2025, 12),
        (2026, 1)
    ]
    
    # Get all AMCs
    cur.execute("SELECT amc_id, amc_name FROM amcs ORDER BY amc_name")
    amcs = cur.fetchall()
    
    # Get all extraction runs for these periods
    cur.execute("""
        SELECT a.amc_name, p.year, p.month, er.status
        FROM extraction_runs er
        JOIN amcs a ON er.amc_id = a.amc_id
        JOIN periods p ON er.period_id = p.period_id
        WHERE (p.year = 2025 AND p.month IN (11, 12))
           OR (p.year = 2026 AND p.month = 1)
    """)
    runs = {(row[0], row[1], row[2]): row[3] for row in cur.fetchall()}
    
    missing = []
    for amc_id, amc_name in amcs:
        for year, month in target_periods:
            if (amc_name, year, month) not in runs:
                missing.append((amc_name, year, month))
    
    if not missing:
        print("All AMCs have data for the specified periods.")
    else:
        print("| AMC Name | Year | Month | Status |")
        print("| :--- | :--- | :--- | :--- |")
        for name, yr, mo in missing:
            print(f"| {name} | {yr} | {mo:02d} | Missing |")

    cur.close()
    conn.close()

if __name__ == "__main__":
    check_completeness()
