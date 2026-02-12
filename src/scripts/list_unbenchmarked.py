from src.db.connection import get_connection
import pandas as pd

def list_unbenchmarked():
    conn = get_connection()
    query = """
    SELECT 
        s.scheme_name, 
        b.benchmark_name 
    FROM scheme_benchmark_history sbh 
    JOIN schemes s ON sbh.scheme_id = s.scheme_id 
    JOIN benchmark_master b ON sbh.benchmark_id = b.benchmark_id 
    LEFT JOIN benchmark_history bh ON b.benchmark_id = bh.benchmark_id
    GROUP BY s.scheme_name, b.benchmark_name
    HAVING COUNT(bh.nav_date) = 0
    ORDER BY b.benchmark_name
    """
    df = pd.read_sql(query, conn)
    print(df.to_string())
    conn.close()

if __name__ == "__main__":
    list_unbenchmarked()
