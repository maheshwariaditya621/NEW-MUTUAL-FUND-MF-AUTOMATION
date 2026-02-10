# src/scheduler/old_bridge_backfill.py

import argparse
from datetime import datetime
from src.downloaders.old_bridge_downloader import OldBridgeDownloader
from src.config import logger

def run_backfill(start_year, start_month, end_year, end_month):
    downloader = OldBridgeDownloader()
    
    # Use persistent session for efficient backfill
    # downloader.open_session()
    
    try:
        for year in range(start_year, end_year + 1):
            m_start = start_month if year == start_year else 1
            m_end = end_month if year == end_year else 12
            
            for month in range(m_start, m_end + 1):
                try:
                    downloader.download(year, month)
                except Exception as e:
                    logger.error(f"Failed to backfill {year}-{month:02d}: {e}")
    finally:
        # downloader.close_session()
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Old Bridge Mutual Fund Backfill")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--start-month", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--end-month", type=int, required=True)
    
    args = parser.parse_args()
    run_backfill(args.start_year, args.start_month, args.end_year, args.end_month)
