"""
PPFAS Auto Backfill Module.

Supports two modes:
1. Manual range mode: User-defined date range
2. Auto mode: Latest eligible month only

Uses _SUCCESS.json marker as source of truth for completion.
"""

import time
import json
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional
from src.downloaders.ppfas_downloader import PPFASDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

# Import dry-run config
try:
    from src.config.downloader_config import DRY_RUN
except ImportError:
    DRY_RUN = False


def generate_month_range(start_year: int, start_month: int, end_year: int, end_month: int) -> List[Tuple[int, int]]:
    """Generate list of (year, month) tuples from start to end."""
    months = []
    year = start_year
    month = start_month
    
    while (year < end_year) or (year == end_year and month <= end_month):
        months.append((year, month))
        
        # Move to next month
        month += 1
        if month > 12:
            month = 1
            year += 1
    
    return months


def get_latest_eligible_month() -> Tuple[int, int]:
    """Get the latest eligible month (previous completed month)."""
    now = datetime.now()
    if now.month == 1:
        return (now.year - 1, 12)
    else:
        return (now.year, now.month - 1)


def is_month_complete(year: int, month: int) -> bool:
    """Check if month is complete (has _SUCCESS.json marker)."""
    folder_path = Path(f"data/raw/ppfas/{year}_{month:02d}")
    success_marker = folder_path / "_SUCCESS.json"
    return success_marker.exists()


def run_ppfas_backfill(
    start_year: Optional[int] = None,
    start_month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None
) -> dict:
    """
    Run PPFAS backfill.
    """
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("PPFAS BACKFILL STARTED")
    if DRY_RUN:
        logger.info("MODE: DRY RUN (no network calls)")
    logger.info("=" * 70)
    
    # Determine mode
    if all(v is not None for v in [start_year, start_month, end_year, end_month]):
        mode = "MANUAL"
        logger.info(f"Mode: {mode}")
        logger.info(f"Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        months = generate_month_range(start_year, start_month, end_year, end_month)
    else:
        mode = "AUTO"
        logger.info(f"Mode: {mode}")
        latest_year, latest_month = get_latest_eligible_month()
        logger.info(f"Latest eligible month: {latest_year}-{latest_month:02d}")
        months = [(latest_year, latest_month)]
    
    logger.info(f"Total months to check: {len(months)}")
    
    downloader = PPFASDownloader()
    notifier = get_notifier()
    
    skipped = 0
    downloaded_months = []
    failed_months = []
    not_published_count = 0
    
    for year, month in months:
        month_start_time = time.time()
        
        if is_month_complete(year, month):
            logger.info(f"[SKIP] {year}-{month:02d} - Complete (_SUCCESS.json exists)")
            skipped += 1
        else:
            logger.info(f"[MISSING] {year}-{month:02d} - Attempting download...")
            
            if DRY_RUN:
                logger.info(f"[DRY RUN] Would download {year}-{month:02d}")
                downloaded_months.append((year, month))
                continue
            
            try:
                result = downloader.download(year=year, month=month)
                status = result["status"]
                
                if status == "success":
                    downloaded_months.append((year, month))
                    month_duration = time.time() - month_start_time
                    logger.success(f"[SUCCESS] {year}-{month:02d} - Downloaded {result['files_downloaded']} file(s) in {month_duration:.2f}s")
                elif status == "skipped":
                    skipped += 1
                    logger.info(f"[SKIP] {year}-{month:02d} - Already complete")
                elif status == "not_published":
                    not_published_count += 1
                    logger.info(f"[NOT PUBLISHED] {year}-{month:02d} - Data not yet available")
                else:
                    reason = result.get("reason", "Unknown error")
                    failed_months.append((year, month, reason))
                    logger.warning(f"[FAILED] {year}-{month:02d} - {reason}")
            except Exception as e:
                error_msg = str(e)
                failed_months.append((year, month, error_msg))
                logger.error(f"[ERROR] {year}-{month:02d} - {error_msg}")
    
    total_duration = time.time() - start_time
    logger.info("=" * 70)
    logger.info("BACKFILL SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Mode: {mode}")
    logger.info(f"Total checked: {len(months)}")
    logger.info(f"Skipped: {skipped}")
    logger.info(f"Downloaded: {len(downloaded_months)}")
    logger.info(f"Not published: {not_published_count}")
    logger.info(f"Failed: {len(failed_months)}")
    logger.info(f"Total duration: {total_duration:.2f}s")
    logger.info("=" * 70)
    
    return {
        "mode": mode,
        "total_checked": len(months),
        "skipped": skipped,
        "downloaded": len(downloaded_months),
        "not_published": not_published_count,
        "failed": len(failed_months),
        "downloaded_months": downloaded_months,
        "failed_months": failed_months,
        "duration": total_duration
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="PPFAS Mutual Fund Backfill")
    parser.add_argument("--start-year", type=int, help="Start year (YYYY)")
    parser.add_argument("--start-month", type=int, help="Start month (1-12)")
    parser.add_argument("--end-year", type=int, help="End year (YYYY)")
    parser.add_argument("--end-month", type=int, help="End month (1-12)")
    
    args = parser.parse_args()
    
    # Check for partial ranges
    provided_args = [args.start_year, args.start_month, args.end_year, args.end_month]
    non_none_count = sum(1 for arg in provided_args if arg is not None)
    
    if 0 < non_none_count < 4:
        logger.error("Error: All four arguments (--start-year, --start-month, --end-year, --end-month) must be provided together")
        exit(1)
    
    if non_none_count == 4:
        result = run_ppfas_backfill(args.start_year, args.start_month, args.end_year, args.end_month)
    else:
        result = run_ppfas_backfill()
    
    if result["failed"] > 0:
        exit(1)
    exit(0)
