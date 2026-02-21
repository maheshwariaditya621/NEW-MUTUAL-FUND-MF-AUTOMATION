import sys
import os

# Ensure we can import from src
sys.path.append(os.getcwd())

import psycopg2
from src.config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def fix_aum_scale(dry_run=True):
    """
    Rescales AUM and holding values by 100,000x for AMCs where Lakhs 
    were incorrectly identified as Rupees.
    """
    # Affected AMC IDs
    # PGIM: 123, Sundaram: 147
    TARGET_AMC_IDS = [123, 147]
    
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    
    try:
        print(f"{'DRY RUN' if dry_run else 'EXECUTION'} MODE")
        
        # 1. Identify affected snapshots
        # Threshold: Snapshots for these AMCs that haven't been rescaled yet
        # (rescaled ones would be in Billions, unrescaled are in Thousands/Lakhs)
        cur.execute("""
            SELECT ss.snapshot_id, s.scheme_name, ss.total_value_inr, a.amc_name
            FROM scheme_snapshots ss
            JOIN schemes s ON ss.scheme_id = s.scheme_id
            JOIN amcs a ON s.amc_id = a.amc_id
            WHERE a.amc_id = ANY(%s)
            AND ss.total_value_inr < 1000000000
        """, (TARGET_AMC_IDS,))
        
        affected_snapshots = cur.fetchall()
        print(f"Found {len(affected_snapshots)} affected snapshots.")
        
        for snap_id, name, val, amc in affected_snapshots:
            new_val = val * 100000
            print(f"  - [{amc}] {name}: {val:,.2f} -> {new_val:,.2f}")
            
            if not dry_run:
                # Update Snapshot
                cur.execute(
                    "UPDATE scheme_snapshots SET total_value_inr = %s WHERE snapshot_id = %s",
                    (new_val, snap_id)
                )
                
                # Update individual holdings
                cur.execute(
                    "UPDATE equity_holdings SET market_value_inr = market_value_inr * 100000 WHERE snapshot_id = %s",
                    (snap_id,)
                )
        
        if not dry_run:
            conn.commit()
            print("\nSuccessfully committed changes.")
        else:
            print("\nDry run complete. No changes made.")
            
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    is_real = len(sys.argv) > 1 and sys.argv[1].lower() == "--execute"
    fix_aum_scale(dry_run=not is_real)
