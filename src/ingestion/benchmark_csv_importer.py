import csv
import os
from datetime import datetime
from src.db.connection import get_connection
from src.config import logger

def import_benchmark_csv(file_path: str, index_symbol: str):
    """
    Imports benchmark data from a CSV file.
    Expected format: Date, Close (or Total Return Index)
    The file should be downloaded from NiftyIndices or similar.
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Get Benchmark ID and TRI status
        cur.execute("SELECT benchmark_id, is_tri FROM benchmark_master WHERE index_symbol = %s", (index_symbol,))
        res = cur.fetchone()
        if not res:
            logger.error(f"Benchmark symbol {index_symbol} not found in master.")
            return
        b_id, is_tri = res
        
        if not is_tri:
            logger.error(f"Benchmark {index_symbol} is not marked as Total Return Index (TRI). Import rejected.")
            logger.error("Please update benchmark_master.is_tri if this is intentional, but beware of Price vs TRI mismatch.")
            return

        logger.info(f"Importing {index_symbol} (TRI={is_tri}) from {file_path}...")
        
        with open(file_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            # Detect columns
            headers = [h.strip().lower() for h in reader.fieldnames or []]
            date_col = next((h for h in headers if 'date' in h), None)
            val_col = next((h for h in headers if 'total return' in h or 'close' in h), None)
            
            if not date_col or not val_col:
                logger.error(f"Could not identify Date/Value columns in: {headers}")
                return
            
            # Sanity check for TRI in CSV headers if possible
            if 'total return' not in val_col and 'tri' not in val_col and 'total' not in val_col:
                logger.warning(f"Column '{val_col}' does not explicitly mention 'Total Return'. Verify this is TRI data.")
                
            entries = []
            source_filename = os.path.basename(file_path)
            
            for row in reader:
                # Map back to original keys
                d_key = next(k for k in row.keys() if k.strip().lower() == date_col)
                v_key = next(k for k in row.keys() if k.strip().lower() == val_col)
                
                raw_date = row[d_key]
                raw_val = row[v_key]
                
                try:
                    # Date formats vary: "01-Jan-2024" or "2024-01-01" or "31 Dec 2015"
                    dt = None
                    raw_date = raw_date.strip('"').strip() # Remove quotes if any
                    
                    if '-' in raw_date:
                        parts = raw_date.split('-')
                        if len(parts[1]) == 3: # dd-Mon-yyyy
                            dt = datetime.strptime(raw_date, "%d-%b-%Y").date()
                        else:
                            dt = datetime.strptime(raw_date, "%Y-%m-%d").date()
                    elif ' ' in raw_date:
                        # "31 Dec 2015"
                        try:
                            dt = datetime.strptime(raw_date, "%d %b %Y").date()
                        except:
                            pass
                    
                    if dt:
                        val = float(raw_val.replace(',', '').replace('"', ''))
                        entries.append((b_id, dt, val, source_filename))
                except Exception as e:
                    # logger.debug(f"Row parse error: {e}")
                    pass # Skip bad rows
            
            if entries:
                query = """
                    INSERT INTO benchmark_history (benchmark_id, nav_date, index_value, source_file, imported_at)
                    VALUES %s
                    ON CONFLICT (benchmark_id, nav_date) DO UPDATE SET
                        index_value = EXCLUDED.index_value,
                        source_file = EXCLUDED.source_file,
                        imported_at = EXCLUDED.imported_at;
                """
                # Prepare data with timestamp
                # actually execute_values handles templating. 
                # We need to pass (b_id, dt, val, source_filename, datetime.utcnow())
                # but execute_values is efficient.
                
                final_entries = [(x[0], x[1], x[2], x[3], datetime.utcnow()) for x in entries]

                from psycopg2.extras import execute_values
                execute_values(cur, query, final_entries)
                conn.commit()
                logger.info(f"Imported {len(entries)} records for {index_symbol}.")
            else:
                logger.warning("No valid records found to import.")
                
    except Exception as e:
        logger.error(f"Import failed: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # Example usage
    import sys
    if len(sys.argv) > 2:
        import_benchmark_csv(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python src/ingestion/benchmark_csv_importer.py <file_path> <index_symbol>")
