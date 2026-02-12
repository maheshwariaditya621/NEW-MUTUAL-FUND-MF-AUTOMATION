from src.db.connection import get_connection
from src.config import logger
from datetime import date, timedelta

def detect_dividends():
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Find IDCW schemes and their Growth counterparts
    # We join on normalized name (stripping 'Plan', 'Direct', 'Growth', 'IDCW', etc.)
    # For now, a simple ilike comparison
    cur.execute("""
        SELECT s_idcw.scheme_id, s_idcw.scheme_name, s_growth.scheme_id, s_growth.scheme_name
        FROM schemes s_idcw
        JOIN schemes s_growth ON REPLACE(REPLACE(s_idcw.scheme_name, 'IDCW', ''), 'Payout', '') 
                             ILIKE REPLACE(REPLACE(s_growth.scheme_name, 'Growth', ''), 'Accumulation', '')
        WHERE s_idcw.option_type = 'IDCW' 
          AND s_growth.option_type = 'Growth'
          AND s_idcw.plan_type = s_growth.plan_type
          AND s_idcw.scheme_id != s_growth.scheme_id
    """)
    pairs = cur.fetchall()
    
    logger.info(f"Analyzing {len(pairs)} Growth/IDCW pairs for dividend detection...")
    
    for i_id, i_name, g_id, g_name in pairs:
        # Get last 30 days of NAV comparison
        cur.execute("""
            SELECT i.nav_date, i.nav_value, g.nav_value,
                   LAG(i.nav_value) OVER (ORDER BY i.nav_date) as i_prev,
                   LAG(g.nav_value) OVER (ORDER BY g.nav_date) as g_prev
            FROM nav_history i
            JOIN nav_history g ON i.nav_date = g.nav_date
            WHERE i.scheme_id = %s AND g.scheme_id = %s
            ORDER BY i.nav_date DESC
            LIMIT 30
        """, (i_id, g_id))
        
        rows = cur.fetchall()
        for row in rows:
            nav_date, i_val, g_val, i_prev, g_prev = row
            if not i_prev or not g_prev or i_prev == 0 or g_prev == 0:
                continue
            
            i_ret = (float(i_val) / float(i_prev)) - 1
            g_ret = (float(g_val) / float(g_prev)) - 1
            
            # If IDCW variant dropped significantly more than Growth variant (> 0.5% diff)
            if i_ret < g_ret - 0.005:
                 est_div = (g_ret - i_ret) * float(i_prev)
                 logger.info(f"Potential Dividend for {i_name} on {nav_date}: {est_div:.4f}")
                 
                 # Upsert into scheme_dividends
                 cur.execute("""
                    INSERT INTO scheme_dividends (scheme_id, record_date, dividend_value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (scheme_id, record_date) DO UPDATE 
                    SET dividend_value = EXCLUDED.dividend_value
                 """, (i_id, nav_date, est_div))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    detect_dividends()
