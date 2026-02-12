from src.db.connection import get_connection
from src.config import logger
from datetime import date

def init_benchmark_mapping():
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Get Benchmark IDs
    cur.execute("SELECT benchmark_id, index_symbol FROM benchmark_master")
    bench_map = {row[1]: row[0] for row in cur.fetchall()}
    
    # 2. Get categorized schemes
    cur.execute("""
        SELECT s.scheme_id, c.scheme_category 
        FROM schemes s 
        JOIN scheme_category_master c ON s.amfi_code = c.amfi_code
    """)
    schemes = cur.fetchall()
    
    mappings = []
    start_date = date(2020, 1, 1) # Default start for records
    
    for s_id, cat in schemes:
        cat_lower = cat.lower()
        target_bench = None
        
        if 'large cap' in cat_lower:
            target_bench = 'NIFTY 50'
        elif 'mid cap' in cat_lower:
            target_bench = 'NIFTY MIDCAP 150'
        elif 'small cap' in cat_lower:
            target_bench = 'NIFTY SMALLCAP 250'
        elif any(x in cat_lower for x in ['flexi cap', 'multi cap', 'elss', 'value', 'contra']):
            target_bench = 'NIFTY 500'
        elif 'next 50' in cat_lower:
            target_bench = 'NIFTY NEXT 50'
        elif 'arbitrage' in cat_lower:
            target_bench = 'NIFTY 50 ARBITRAGE'
        elif 'balanced advantage' in cat_lower or 'dynamic asset allocation' in cat_lower:
            target_bench = 'NIFTY 50 HYBRID 50:50'
        elif 'equity savings' in cat_lower:
            target_bench = 'NIFTY EQUITY SAVINGS'
        elif 'liquid' in cat_lower:
            target_bench = 'NIFTY LIQUID'
        elif 'banking and psu' in cat_lower:
            target_bench = 'NIFTY BANKING PSU DEBT'
        
        if target_bench and target_bench in bench_map:
            mappings.append((s_id, bench_map[target_bench], start_date))
            
    if mappings:
        logger.info(f"Applying {len(mappings)} default benchmark mappings...")
        query = """
            INSERT INTO scheme_benchmark_history (scheme_id, benchmark_id, start_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (scheme_id, start_date) DO UPDATE SET
                benchmark_id = EXCLUDED.benchmark_id;
        """
        cur.executemany(query, mappings)
        conn.commit()
        logger.info("Default benchmark mapping complete.")
    else:
        logger.warning("No mappings to apply.")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    init_benchmark_mapping()
