"""
Quantum MF Auto Backfill Module.

Supports two modes:
1. Manual range mode: User-defined date range
2. Auto mode: Latest eligible month only

Uses _SUCCESS.json marker as source of truth for completion.
"""

import time
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional
from src.downloaders.quantum_downloader import QuantumDownloader
from src.config import logger

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
    """Get the latest eligible month (previous calendar month)."""
    now = datetime.now()
    if now.month == 1:
        return (now.year - 1, 12)
    else:
        return (now.year, now.month - 1)


def is_month_complete(year: int, month: int) -> bool:
    """Check if month is complete (has _SUCCESS.json marker)."""
    folder_path = Path(f"data/raw/quantum/{year}_{month:02d}")
    success_marker = folder_path / "_SUCCESS.json"
    return success_marker.exists()


def run_quantum_backfill(
    start_year: Optional[int] = None,
    start_month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None
) -> dict:
    """
    Run Quantum backfill.
    
    Two modes:
    MODE 1 - Manual Range (if dates provided): Missing months in range
    MODE 2 - Auto (if no dates provided): Only latest eligible month
    """
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("QUANTUM MF BACKFILL STARTED")
    if DRY_RUN:
        logger.info("MODE: DRY RUN (no network calls)")
    logger.info("=" * 70)
    
    downloader = QuantumDownloader()
    
    skipped = 0
    downloaded_months = []
    failed_months = []
    not_published_count = 0
    
    # Determine mode
    if all([start_year, start_month, end_year, end_month]):
        mode = "MANUAL_RANGE"
        logger.info(f"Mode: {mode}")
        logger.info(f"Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        months = generate_month_range(start_year, start_month, end_year, end_month)
    else:
        mode = "AUTO"
        logger.info(f"Mode: {mode}")
        year, month = get_latest_eligible_month()
        logger.info(f"Latest eligible month: {year}-{month:02d}")
        months = [(year, month)]

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
                    logger.success(f"[SUCCESS] {year}-{month:02d} - Downloaded 1 file(s)")
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
    total_checked = len(months)
    
    logger.info("=" * 70)
    logger.info("BACKFILL SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Mode: {mode}")
    logger.info(f"Total checked: {total_checked}")
    logger.info(f"Skipped (already complete): {skipped}")
    logger.info(f"Downloaded: {len(downloaded_months)}")
    logger.info(f"Not published: {not_published_count}")
    logger.info(f"Failed: {len(failed_months)}")
    logger.info(f"Total duration: {total_duration:.2f}s")
    logger.info("=" * 70)
    
    return {
        "mode": mode,
        "total_checked": total_checked,
        "skipped": skipped,
        "downloaded": len(downloaded_months),
        "not_published": not_published_count,
        "failed": len(failed_months),
        "failed_months": failed_months,
        "downloaded_months": downloaded_months
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Quantum MF Backfill")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--start-month", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--end-month", type=int)
    
    args = parser.parse_args()
    
    date_args = [args.start_year, args.start_month, args.end_year, args.end_month]
    
    if any(date_args) and not all(date_args):
        logger.error("Error: Either provide all 4 date arguments (--start-year, --start-month, --end-year, --end-month) or none for auto mode.")
        exit(1)
        
    result = run_quantum_backfill(
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month
    )
    
    if result["mode"] == "AUTO":
        if result["downloaded"] > 0 or result["skipped"] > 0 or result["not_published"] > 0:
            exit(0)
        else:
            exit(1)
    else:
        if result["failed"] > 0:
            exit(1)
        else:
            exit(0)
