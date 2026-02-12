from src.db.connection import get_connection
import pandas as pd

def verify_phase_b():
    conn = get_connection()
    
    print("=== Phase B Verification ===\n")
    
    # 1. Category Coverage
    print("1. Category Coverage:")
    df_cat = pd.read_sql("""
        SELECT 
            count(*) as total_schemes,
            count(c.amfi_code) as mapped_schemes,
            round(count(c.amfi_code)::numeric / count(*) * 100, 2) as coverage_pct
        FROM schemes s
        LEFT JOIN scheme_category_master c ON s.amfi_code = c.amfi_code
    """, conn)
    print(df_cat.to_string(index=False))
    
    # 2. Benchmark Assignment
    print("\n2. Benchmark Assignment Coverage:")
    df_bench = pd.read_sql("""
        SELECT 
            count(DISTINCT s.scheme_id) as total_mapped_schemes,
            count(DISTINCT sb.scheme_id) as schemes_with_benchmark,
            round(count(DISTINCT sb.scheme_id)::numeric / count(DISTINCT s.scheme_id) * 100, 2) as bench_coverage_pct
        FROM schemes s
        JOIN scheme_category_master c ON s.amfi_code = c.amfi_code
        LEFT JOIN scheme_benchmark_history sb ON s.scheme_id = sb.scheme_id
    """, conn)
    print(df_bench.to_string(index=False))
    
    # 3. Benchmark Master
    print("\n3. Active Benchmarks:")
    df_bm = pd.read_sql("SELECT benchmark_name, index_symbol, provider, is_tri FROM benchmark_master ORDER BY benchmark_name", conn)
    print(df_bm.to_string(index=False))
    
    # 4. Unmapped Schemes (Sample)
    print("\n4. Remaining Unmapped Schemes (Top 5):")
    df_unmapped = pd.read_sql("""
        SELECT s.scheme_name 
        FROM schemes s 
        LEFT JOIN scheme_category_master c ON s.amfi_code = c.amfi_code 
        WHERE c.amfi_code IS NULL 
        LIMIT 5
    """, conn)
    print(df_unmapped.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    verify_phase_b()
