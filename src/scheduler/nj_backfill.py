# src/scheduler/nj_backfill.py

import argparse
from src.downloaders.nj_downloader import NJDownloader
from src.config import logger

def run_backfill(start_year: int, start_month: int, end_year: int, end_month: int):
    downloader = NJDownloader()
    
    logger.info(f"Starting NJ backfill from {start_year}-{start_month} to {end_year}-{end_month}")
    
    # Persistent Session: Open once for the entire backfill
    # downloader.open_session()
    
    try:
        current_year, current_month = start_year, start_month
        
        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            try:
                downloader.download(current_year, current_month)
            except Exception as e:
                logger.error(f"Backfill failed for {current_year}-{current_month}: {e}")
            
            # Increment month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
                
    finally:
        # downloader.close_session()
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NJ Mutual Fund Backfill")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--start-month", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--end-month", type=int, required=True)
    
    args = parser.parse_args()
    run_backfill(args.start_year, args.start_month, args.end_year, args.end_month)
