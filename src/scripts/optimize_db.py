"""
Database optimization script to speed up Mutual Fund Automation queries.
Adds indices to key tables: companies, schemes, equity_holdings, and scheme_snapshots.
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.db.connection import get_connection, logger

def optimize_db():
    conn = get_connection()
    cur = conn.cursor()
    
    indices = [
        # Companies
        ("idx_companies_isin", "companies(isin)"),
        ("idx_companies_entity_id", "companies(entity_id)"),
        ("idx_companies_name_trgm", "companies USING gin (company_name gin_trgm_ops)"),
        
        # Schemes
        ("idx_schemes_amc_id", "schemes(amc_id)"),
        
        # Snapshots
        ("idx_snapshots_scheme_id", "scheme_snapshots(scheme_id)"),
        ("idx_snapshots_period_id", "scheme_snapshots(period_id)"),
        
        # Equity Holdings
        ("idx_equity_holdings_snapshot_id", "equity_holdings(snapshot_id)"),
        ("idx_equity_holdings_company_id", "equity_holdings(company_id)"),
    ]
    
    print("Starting database optimization...")
    
    # Enable pg_trgm for fuzzy search if not exists
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        conn.commit()
    except Exception as e:
        print(f"Note: Could not create pg_trgm extension: {e}")
        conn.rollback()

    for name, definition in indices:
        try:
            print(f"Creating index {name}...")
            cur.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {definition};")
            conn.commit()
            print(f"Successfully created {name}.")
        except Exception as e:
            print(f"Error creating index {name}: {e}")
            conn.rollback()
            
    print("Database optimization complete.")
    cur.close()

if __name__ == "__main__":
    optimize_db()
