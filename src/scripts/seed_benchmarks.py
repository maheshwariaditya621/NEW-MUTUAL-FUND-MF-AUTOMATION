from src.db.connection import get_connection
from src.config import logger

def seed_benchmarks():
    conn = get_connection()
    cur = conn.cursor()
    
    benchmarks = [
        ('Nifty 50 TRI', 'NIFTY 50', 'NSE', True),
        ('Nifty Next 50 TRI', 'NIFTY NEXT 50', 'NSE', True),
        ('Nifty Midcap 150 TRI', 'NIFTY MIDCAP 150', 'NSE', True),
        ('Nifty Smallcap 250 TRI', 'NIFTY SMALLCAP 250', 'NSE', True),
        ('Nifty 500 TRI', 'NIFTY 500', 'NSE', True),
        ('S&P BSE SENSEX TRI', 'S&P BSE SENSEX', 'BSE', True),
        ('Nifty 50 Hybrid Composite debt 50:50 Index', 'NIFTY 50 HYBRID 50:50', 'NSE', True),
        ('Nifty 50 Arbitrage Index', 'NIFTY 50 ARBITRAGE', 'NSE', True),
        ('NIFTY Equity Savings Index', 'NIFTY EQUITY SAVINGS', 'NSE', True),
        ('NIFTY Liquid Index', 'NIFTY LIQUID', 'NSE', True),
        ('NIFTY Banking & PSU Debt Index', 'NIFTY BANKING PSU DEBT', 'NSE', True),
    ]
    
    query = """
        INSERT INTO benchmark_master (benchmark_name, index_symbol, provider, is_tri)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (index_symbol) DO UPDATE SET
            benchmark_name = EXCLUDED.benchmark_name,
            provider = EXCLUDED.provider,
            is_tri = EXCLUDED.is_tri;
    """
    
    try:
        cur.executemany(query, benchmarks)
        conn.commit()
        logger.info(f"Seeded {len(benchmarks)} benchmarks into benchmark_master.")
    except Exception as e:
        logger.error(f"Error seeding benchmarks: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    seed_benchmarks()
