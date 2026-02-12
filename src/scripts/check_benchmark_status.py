from src.db.connection import get_connection
import pandas as pd

def check_benchmark_status():
    conn = get_connection()
    query = """
    SELECT 
        b.benchmark_name, 
        b.index_symbol, 
        COUNT(DISTINCT bh.nav_date) as history_count, 
        COUNT(DISTINCT sbh.scheme_id) as linked_schemes 
    FROM benchmark_master b 
    LEFT JOIN benchmark_history bh ON b.benchmark_id = bh.benchmark_id 
    LEFT JOIN scheme_benchmark_history sbh ON b.benchmark_id = sbh.benchmark_id 
    GROUP BY b.benchmark_id, b.benchmark_name, b.index_symbol
    ORDER BY linked_schemes DESC
    """
    df = pd.read_sql(query, conn)
    print(df.to_string())
    conn.close()

if __name__ == "__main__":
    check_benchmark_status()
