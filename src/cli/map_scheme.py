import argparse
import sys
from src.db.connection import get_connection

def find_scheme_by_name(cur, amc_id, name):
    cur.execute("SELECT scheme_id FROM schemes WHERE amc_id = %s AND scheme_name = %s LIMIT 1", (amc_id, name.upper()))
    res = cur.fetchone()
    return res[0] if res else None

def map_scheme(amc_name: str, source: str, target: str):
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Find AMC
        cur.execute("SELECT amc_id FROM amcs WHERE amc_name ILIKE %s LIMIT 1", (f"%{amc_name}%",))
        amc_res = cur.fetchone()
        if not amc_res:
            print(f"Error: AMC '{amc_name}' not found.")
            sys.exit(1)
        amc_id = amc_res[0]
        
        source = source.strip().upper()
        target = target.strip().upper()
        
        # Verify target exists
        target_id = find_scheme_by_name(cur, amc_id, target)
        if not target_id:
            print(f"Error: Target canonical scheme '{target}' does not exist in DB for AMC ID {amc_id}. It must exist to map to it.")
            sys.exit(1)
            
        # Insert mapping
        cur.execute(
            """
            INSERT INTO scheme_name_mappings (amc_id, source_name, canonical_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (amc_id, source_name) DO UPDATE SET canonical_name = EXCLUDED.canonical_name, updated_at = NOW()
            """,
            (amc_id, source, target)
        )
        conn.commit()
        print(f"Successfully mapped:\n  Source: {source}\n  -> Target: {target}\nFor future extractions.")
        
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cur.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Map a source scheme name to a canonical target name.")
    parser.add_argument("--amc", required=True, help="Partial or full AMC name (e.g. 'SBI')")
    parser.add_argument("--source", required=True, help="The bad/old scheme name to map from")
    parser.add_argument("--target", required=True, help="The canonical/new scheme name to map to")
    args = parser.parse_args()
    
    map_scheme(args.amc, args.source, args.target)
