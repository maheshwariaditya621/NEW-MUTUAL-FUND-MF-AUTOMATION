import os
import sys
import glob
from src.ingestion.benchmark_csv_importer import import_benchmark_csv
from src.config import logger

def bulk_import(folder_path, index_symbol):
    """
    Imports all CSV files from a folder for a given index symbol.
    """
    if not os.path.exists(folder_path):
        logger.error(f"Folder not found: {folder_path}")
        return

    # Find all csvs
    files = glob.glob(os.path.join(folder_path, "*.csv"))
    files.sort() # Ensure chronological order roughly by name (names have dates)
    
    if not files:
        logger.warning(f"No CSV files found in {folder_path}")
        return
        
    logger.info(f"Found {len(files)} files for {index_symbol}. Starting bulk import...")
    
    success_count = 0
    for f_path in files:
        try:
            import_benchmark_csv(f_path, index_symbol)
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to import {f_path}: {e}")
            
    logger.info(f"Bulk import finished. Successfully imported {success_count}/{len(files)} files.")

if __name__ == "__main__":
    if len(sys.argv) > 2:
        bulk_import(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python src/scripts/bulk_import_benchmarks.py <folder_path> <index_symbol>")
