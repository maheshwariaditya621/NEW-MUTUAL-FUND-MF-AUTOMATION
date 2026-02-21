
from src.db.connection import get_connection, close_connection
from src.config import logger

def seed_known_actions():
    conn = get_connection()
    cur = conn.cursor()
    
    actions = [
        # (Old ISIN, New ISIN, Effective Date, Action Type, Description)
        ('INE237A01028', 'INE237A01036', '2026-01-14', 'SPLIT', 'Kotak Mahindra Bank Stock Split (10:1)')
    ]
    
    try:
        for old, new, date, act_type, desc in actions:
            logger.info(f"Seeding corporate action: {old} -> {new}")
            cur.execute("""
                INSERT INTO corporate_actions (old_isin, new_isin, effective_date, action_type, description)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (old, new, date, act_type, desc))
        
        conn.commit()
        logger.info("Known actions seeded successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Seeding failed: {e}")
    finally:
        cur.close()
        close_connection()

if __name__ == "__main__":
    seed_known_actions()
