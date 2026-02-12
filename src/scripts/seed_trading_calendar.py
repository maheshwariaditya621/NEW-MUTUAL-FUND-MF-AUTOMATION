from datetime import date, timedelta
from src.db.connection import get_connection
from src.config import logger

def seed_calendar():
    conn = get_connection()
    cur = conn.cursor()
    
    # Range: 2024-01-01 to 2025-12-31
    start_date = date(2024, 1, 1)
    end_date = date(2025, 12, 31)
    
    # NSE Holidays 2024 (from search)
    holidays_2024 = {
        date(2024, 1, 26), date(2024, 3, 8), date(2024, 3, 25), date(2024, 3, 29),
        date(2024, 4, 11), date(2024, 4, 17), date(2024, 5, 1), date(2024, 5, 20),
        date(2024, 6, 17), date(2024, 7, 17), date(2024, 8, 15), date(2024, 10, 2),
        date(2024, 11, 1), date(2024, 11, 15), date(2024, 12, 25)
    }
    
    # NSE Holidays 2025 (from search)
    holidays_2025 = {
        date(2025, 2, 19), date(2025, 2, 26), date(2025, 3, 14), date(2025, 3, 31),
        date(2025, 4, 1), date(2025, 4, 10), date(2025, 4, 14), date(2025, 4, 18),
        date(2025, 5, 1), date(2025, 5, 12), date(2025, 8, 15), date(2025, 8, 27),
        date(2025, 9, 5), date(2025, 10, 2), date(2025, 10, 21), date(2025, 10, 22),
        date(2025, 11, 5), date(2025, 12, 25)
    }
    
    all_holidays = holidays_2024.union(holidays_2025)
    
    logger.info(f"Seeding trading calendar from {start_date} to {end_date}...")
    
    curr = start_date
    batch = []
    while curr <= end_date:
        is_trading = True
        # Saturday = 5, Sunday = 6
        if curr.weekday() >= 5:
            is_trading = False
        elif curr in all_holidays:
            is_trading = False
            
        batch.append((curr, is_trading))
        curr += timedelta(days=1)
    
    from psycopg2.extras import execute_values
    execute_values(cur, """
        INSERT INTO trading_calendar (trading_date, is_trading_day)
        VALUES %s
        ON CONFLICT (trading_date) DO UPDATE SET is_trading_day = EXCLUDED.is_trading_day
    """, batch)
    
    conn.commit()
    logger.info(f"Successfully seeded {len(batch)} days into trading_calendar.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    seed_calendar()
