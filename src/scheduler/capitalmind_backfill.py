# src/scheduler/capitalmind_backfill.py

import argparse
import time
from datetime import datetime
from src.downloaders.capitalmind_downloader import CapitalMindDownloader
from src.config import logger

def run_backfill(start_year, start_month, end_year, end_month):
    downloader = CapitalMindDownloader()
    
    # Open persistent session once for the entire backfill
    downloader.open_session()
    
    try:
        current_year = start_year
        current_month = start_month
        
        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            logger.info(f"\n>>> Processing CapitalMind: {current_year}-{current_month:02d}")
            
            try:
                downloader.download(current_year, current_month)
            except Exception as e:
                logger.error(f"Critical error in backfill for {current_year}-{current_month:02d}: {e}")
            
            # Increment month
            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1
                
    finally:
        # Close persistent session at the end
        downloader.close_session()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CapitalMind Multi-month Backfill")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--start-month", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--end-month", type=int, required=True)
    
    args = parser.parse_args()
    
    logger.info(f"Starting CapitalMind backfill from {args.start_year}-{args.start_month:02d} to {args.end_year}-{args.end_month:02d}")
    run_backfill(args.start_year, args.start_month, args.end_year, args.end_month)
