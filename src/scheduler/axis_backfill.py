"""
Axis Auto Backfill Module.

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
from src.downloaders.axis_downloader import AxisDownloader
from src.config import logger
from src.alerts.telegram_notifier import get_notifier

# Import dry-run config
try:
    from src.config.downloader_config import DRY_RUN
except ImportError:
    DRY_RUN = False


def generate_month_range(start_year: int, start_month: int, end_year: int, end_month: int) -> List[Tuple[int, int]]:
    """
    Generate list of (year, month) tuples from start to end.
    
    Args:
        start_year: Starting year
        start_month: Starting month (1-12)
        end_year: Ending year
        end_month: Ending month (1-12)
        
    Returns:
        List of (year, month) tuples
    """
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
    """
    Get the latest eligible month (previous completed month).
    
    Logic:
    - Always returns previous month from current date
    - Works for any month (Jan-Dec)
    
    Returns:
        Tuple of (year, month)
    """
    now = datetime.now()
    
    # Previous month
    if now.month == 1:
        return (now.year - 1, 12)
    else:
        return (now.year, now.month - 1)


def is_month_complete(year: int, month: int) -> bool:
    """
    Check if month is complete (has _SUCCESS.json marker).
    
    Args:
        year: Year
        month: Month (1-12)
        
    Returns:
        True if _SUCCESS.json exists, False otherwise
    """
    folder_path = Path(f"data/raw/axis/{year}_{month:02d}")
    success_marker = folder_path / "_SUCCESS.json"
    
    return success_marker.exists()


def has_not_published_marker(year: int, month: int) -> bool:
    """
    Check if NOT_PUBLISHED marker exists for this month.
    
    Args:
        year: Year
        month: Month (1-12)
        
    Returns:
        True if _NOT_PUBLISHED.json exists, False otherwise
    """
    folder_path = Path(f"data/raw/axis/{year}_{month:02d}")
    not_published_marker = folder_path / "_NOT_PUBLISHED.json"
    
    return not_published_marker.exists()


def create_not_published_marker(year: int, month: int):
    """
    Create NOT_PUBLISHED marker atomically.
    
    Args:
        year: Year
        month: Month (1-12)
    """
    folder_path = Path(f"data/raw/axis/{year}_{month:02d}")
    folder_path.mkdir(parents=True, exist_ok=True)
    
    marker_path = folder_path / "_NOT_PUBLISHED.json"
    tmp_marker_path = folder_path / "_NOT_PUBLISHED.json.tmp"
    
    marker_data = {
        "amc": "axis",
        "year": year,
        "month": month,
        "timestamp": datetime.now().isoformat()
    }
    
    # Atomic write: write to tmp, then rename
    with open(tmp_marker_path, "w") as f:
        json.dump(marker_data, f, indent=2)
    
    tmp_marker_path.rename(marker_path)
    
    logger.info(f"Created NOT_PUBLISHED marker: {marker_path.name}")


def run_axis_backfill(
    start_year: Optional[int] = None,
    start_month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None
) -> dict:
    """
    Run Axis backfill.
    
    Two modes:
    
    MODE 1 - Manual Range (if dates provided):
        Downloads missing months in user-defined range
        
    MODE 2 - Auto (if no dates provided):
        Downloads only latest eligible month if missing
    
    Args:
        start_year: Optional start year
        start_month: Optional start month (1-12)
        end_year: Optional end year
        end_month: Optional end month (1-12)
        
    Returns:
        Dictionary with summary:
            - mode: str ("range" or "auto")
            - total_checked: int
            - skipped: int
            - downloaded: int
            - failed: int
            - downloaded_months: List[Tuple[int, int]]
            - failed_months: List[Tuple[int, int, str]]
    """
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("AXIS BACKFILL STARTED")
    if DRY_RUN:
        logger.info("MODE: DRY RUN (no network calls)")
    logger.info("=" * 70)
    
    # Determine mode
    if start_year is not None and start_month is not None and end_year is not None and end_month is not None:
        # MODE 1: Manual range
        mode = "MANUAL"
        logger.info(f"Mode: {mode}")
        logger.info(f"Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        
        months = generate_month_range(start_year, start_month, end_year, end_month)
        logger.info(f"Total months to check: {len(months)}")
    else:
        # MODE 2: Auto (latest month only)
        mode = "AUTO"
        logger.info(f"Mode: {mode}")
        
        latest_year, latest_month = get_latest_eligible_month()
        logger.info(f"Latest eligible month: {latest_year}-{latest_month:02d}")
        
        months = [(latest_year, latest_month)]
    
    # Initialize downloader
    downloader = AxisDownloader()
    
    # Initialize notifier
    notifier = get_notifier()
    
    # Track results
    skipped = 0
    downloaded_months = []
    failed_months = []
    corruption_recoveries = 0
    
    # Process each month
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
                
                if result["status"] == "success":
                    downloaded_months.append((year, month))
                    month_duration = time.time() - month_start_time
                    logger.success(f"[SUCCESS] {year}-{month:02d} - Downloaded {result['files_downloaded']} file(s) in {month_duration:.2f}s")
                    
                    # Send success alert
                    notifier.notify_success(
                        amc="Axis",
                        year=year,
                        month=month,
                        files_downloaded=result['files_downloaded'],
                        duration=month_duration
                    )
                elif result["status"] == "skipped":
                    # Already complete (handled by downloader's _SUCCESS.json check)
                    skipped += 1
                    logger.info(f"[SKIP] {year}-{month:02d} - Already complete")
                elif result["status"] == "not_published":
                    # Data not yet available - treat as skipped, not failed
                    skipped += 1
                    logger.info(f"[SKIP] {year}-{month:02d} - Not yet published")
                    
                    # Send Telegram alert ONLY on first detection
                    if not has_not_published_marker(year, month):
                        logger.info(f"First NOT_PUBLISHED detection for {year}-{month:02d} - sending Telegram alert")
                        notifier.notify_not_published(
                            amc="Axis",
                            year=year,
                            month=month
                        )
                        create_not_published_marker(year, month)
                    else:
                        logger.info(f"NOT_PUBLISHED marker already exists for {year}-{month:02d} - skipping alert")
                elif result["status"] == "failed":
                    # Actual failure
                    reason = result.get("reason", "Unknown error")
                    failed_months.append((year, month, reason))
                    logger.warning(f"[FAILED] {year}-{month:02d} - {reason}")
                    
                    # Send failure alert
                    notifier.notify_error(
                        amc="Axis",
                        year=year,
                        month=month,
                        error_type="Download Failed",
                        reason=reason
                    )
                else:
                    # Unknown status - treat as failure
                    reason = f"Unknown status: {result['status']}"
                    failed_months.append((year, month, reason))
                    logger.warning(f"[FAILED] {year}-{month:02d} - {reason}")
            except Exception as e:
                error_msg = str(e)
                failed_months.append((year, month, error_msg))
                logger.error(f"[ERROR] {year}-{month:02d} - {error_msg}")
    
    # Summary
    total_duration = time.time() - start_time
    
    logger.info("=" * 70)
    logger.info("BACKFILL SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Mode: {mode}")
    logger.info(f"Total checked: {len(months)}")
    logger.info(f"Skipped (already complete): {skipped}")
    logger.info(f"Downloaded: {len(downloaded_months)}")
    logger.info(f"Failed: {len(failed_months)}")
    logger.info(f"Total duration: {total_duration:.2f}s")
    
    if downloaded_months:
        logger.info("Downloaded months:")
        for year, month in downloaded_months:
            logger.info(f"  ✅ {year}-{month:02d}")
    
    if failed_months:
        logger.warning("Failed months:")
        for year, month, reason in failed_months:
            logger.warning(f"  ❌ {year}-{month:02d} - {reason}")
    
    logger.info("=" * 70)
    
    return {
        "mode": mode,
        "total_checked": len(months),
        "skipped": skipped,
        "downloaded": len(downloaded_months),
        "failed": len(failed_months),
        "downloaded_months": downloaded_months,
        "failed_months": failed_months,
        "duration": total_duration
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Axis Mutual Fund Backfill")
    parser.add_argument("--start-year", type=int, help="Start year (YYYY)")
    parser.add_argument("--start-month", type=int, help="Start month (1-12)")
    parser.add_argument("--end-year", type=int, help="End year (YYYY)")
    parser.add_argument("--end-month", type=int, help="End month (1-12)")
    
    args = parser.parse_args()
    
    # Check for partial ranges (not allowed)
    provided_args = [args.start_year, args.start_month, args.end_year, args.end_month]
    non_none_count = sum(1 for arg in provided_args if arg is not None)
    
    if non_none_count > 0 and non_none_count < 4:
        logger.error("Error: All four arguments (--start-year, --start-month, --end-year, --end-month) must be provided together")
        logger.error("For AUTO mode, omit all arguments")
        exit(1)
    
    # Validate month ranges if provided
    if args.start_month is not None:
        if args.start_month < 1 or args.start_month > 12:
            logger.error(f"Invalid start month: {args.start_month}. Must be between 1 and 12.")
            exit(1)
    
    if args.end_month is not None:
        if args.end_month < 1 or args.end_month > 12:
            logger.error(f"Invalid end month: {args.end_month}. Must be between 1 and 12.")
            exit(1)
    
    # Run backfill
    if non_none_count == 4:
        # Manual range mode
        result = run_axis_backfill(
            start_year=args.start_year,
            start_month=args.start_month,
            end_year=args.end_year,
            end_month=args.end_month
        )
    else:
        # Auto mode
        result = run_axis_backfill()
    
    # Summary
    if result["downloaded"] > 0:
        logger.success(f"✅ Backfill completed - {result['downloaded']} month(s) downloaded")
    elif result["skipped"] == result["total_checked"]:
        logger.info("ℹ️  All months already downloaded")
    else:
        logger.warning(f"⚠️  Backfill completed with {result['failed']} failure(s)")
