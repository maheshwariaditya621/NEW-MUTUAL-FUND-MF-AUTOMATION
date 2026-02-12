from src.db.connection import get_cursor, close_connection
from src.config.constants import AMC_HDFC, AMC_SBI

def list_schemes():
    cursor = get_cursor()
    
    with open("valid_schemes_list.txt", "w", encoding="utf-8") as f:
        f.write("VALID SCHEMES REPORT\n")
        f.write("====================\n\n")
        
        for amc in [AMC_HDFC, AMC_SBI]:
            f.write(f"--- {amc} ---\n")
            cursor.execute("""
                SELECT s.scheme_id, s.scheme_name, s.plan_type, s.option_type, count(ss.snapshot_id) as snapshots
                FROM schemes s
                JOIN amcs a ON s.amc_id = a.amc_id
                LEFT JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
                WHERE a.amc_name = %s
                GROUP BY s.scheme_id, s.scheme_name, s.plan_type, s.option_type
                ORDER BY s.scheme_name
            """, (amc,))
            
            schemes = cursor.fetchall()
            f.write(f"Total Count: {len(schemes)}\n\n")
            
            for r in schemes:
                f.write(f"ID: {r[0]} | Name: {r[1]} | Plan: {r[2]} | Option: {r[3]} | Snapshots: {r[4]}\n")
            f.write("\n")
            
    print("Scheme list generated in valid_schemes_list.txt")
    close_connection()

if __name__ == "__main__":
    list_schemes()
