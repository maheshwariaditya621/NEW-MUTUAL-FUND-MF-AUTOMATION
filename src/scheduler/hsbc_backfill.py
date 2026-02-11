"""
HSBC Auto Backfill Module.

Supports two modes:
1. Manual range mode: User-defined date range
2. Auto mode: Latest eligible month only

Uses _SUCCESS.json marker as source of truth for completion.
"""

import time
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional
from src.downloaders.hsbc_downloader import HSBCDownloader
from src.config import logger

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
    # NOTE: This is a heuristic. The authoritative check is in the downloader.
    # We check the standard path data/raw/{amc_name}/YYYY_MM/_SUCCESS.json
    # We need to construct the AMC name directory correctly.
    # Since AMC_NAME is defined in the downloader, we might just rely on the downloader's idempotency.
    # But for reporting 'skipped' without invoking downloader overhead, we can check.
    # However, to be safe and consistent with "Gold Standard" logic, we let the downloader handle it.
    pass


def run_backfill(
    start_year: Optional[int] = None,
    start_month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None
) -> dict:
    """
    Run HSBC backfill.
    
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
        Dictionary with summary metrics.
    """
    start_time = time.time()
    
    logger.info("=" * 70)
    logger.info("HSBC BACKFILL STARTED")
    if DRY_RUN:
        logger.info("MODE: DRY RUN (no network calls)")
    logger.info("=" * 70)
    
    # Initialize downloader
    downloader = HSBCDownloader()
    
    # Track results
    skipped = 0
    downloaded_months = []
    failed_months = []
    not_published_count = 0
    
    # Determine mode and execute
    if all([start_year, start_month, end_year, end_month]):
        # ========================================================================
        # MODE 1: MANUAL RANGE
        # ========================================================================
        mode = "MANUAL_RANGE"
        logger.info(f"Mode: {mode}")
        logger.info(f"Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        
        months = generate_month_range(start_year, start_month, end_year, end_month)
        logger.info(f"Total months to check: {len(months)}")
        
        # Process each month in range
        for year, month in months:
            logger.info(f"[CHECK] {year}-{month:02d}")
            month_start_time = time.time()
            
            try:
                # Downloader now handles idempotency and consolidation trigger
                result = downloader.download(year=year, month=month)
                status = result["status"]
                
                if status == "success":
                    downloaded_months.append((year, month))
                    month_duration = time.time() - month_start_time
                    logger.success(f"[SUCCESS] {year}-{month:02d} - Consolidated in {month_duration:.2f}s")
                elif status == "skipped":
                    skipped += 1
                    logger.success(f"[OK] {year}-{month:02d} - Already complete (Consolidation refreshed)")
                elif status == "not_published":
                    not_published_count += 1
                    logger.info(f"[NOT PUBLISHED] {year}-{month:02d} - Data not available")
                else:
                    reason = result.get("reason", "Unknown error")
                    failed_months.append((year, month, reason))
                    logger.warning(f"[FAILED] {year}-{month:02d} - {reason}")
            except Exception as e:
                error_msg = str(e)
                failed_months.append((year, month, error_msg))
                logger.error(f"[ERROR] {year}-{month:02d} - {error_msg}")
    
    else:
        # ========================================================================
        # MODE 2: AUTO
        # ========================================================================
        mode = "AUTO"
        logger.info(f"Mode: {mode}")
        
        year, month = get_latest_eligible_month()
        logger.info(f"Latest eligible month: {year}-{month:02d}")
        month_start_time = time.time()
        
        try:
            result = downloader.download(year=year, month=month)
            status = result["status"]
            
            if status == "success":
                downloaded_months.append((year, month))
                month_duration = time.time() - month_start_time
                logger.success(f"[SUCCESS] {year}-{month:02d} - Consolidated in {month_duration:.2f}s")
            elif status == "skipped":
                skipped = 1
                logger.success(f"[OK] {year}-{month:02d} - Already complete (Consolidation refreshed)")
            elif status == "not_published":
                not_published_count = 1
                logger.info(f"[NOT PUBLISHED] {year}-{month:02d} - Data not available")
            else:
                reason = result.get("reason", "Unknown error")
                failed_months.append((year, month, reason))
                logger.warning(f"[FAILED] {year}-{month:02d} - {reason}")
        except Exception as e:
            error_msg = str(e)
            failed_months.append((year, month, error_msg))
            logger.error(f"[ERROR] {year}-{month:02d} - {error_msg}")
    
    # Summary
    total_duration = time.time() - start_time
    total_checked = 1 if mode == "AUTO" else len(months)
    
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
        "total_checked": total_checked,
        "skipped": skipped,
        "downloaded": len(downloaded_months),
        "failed": len(failed_months),
        "not_published": not_published_count,
        "downloaded_months": downloaded_months,
        "failed_months": failed_months,
        "duration": total_duration
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="HSBC Backfill")
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
        result = run_backfill(
            start_year=args.start_year,
            start_month=args.start_month,
            end_year=args.end_year,
            end_month=args.end_month
        )
    else:
        # Auto mode
        result = run_backfill()
    
    # Exit status based on mode
    if result["mode"] == "AUTO":
        # AUTO mode: success OR not_published → exit 0
        if result["downloaded"] > 0:
            logger.success(f"✅ Backfill completed - {result['downloaded']} month(s) downloaded")
            exit(0)
        elif result["skipped"] > 0:
            logger.info("ℹ️  Latest month already downloaded")
            exit(0)
        elif result["not_published"] > 0:
            logger.info("ℹ️  Latest month not yet published")
            exit(0)
        else:
            # Failed
            logger.error(f"❌ Backfill failed: {result['failed_months'][0][2] if result['failed_months'] else 'Unknown error'}")
            exit(1)
    else:
        # MANUAL mode: failures > 0 → exit 1, else → exit 0
        if result["failed"] > 0:
            logger.warning(f"⚠️  Backfill completed with {result['failed']} failure(s)")
            exit(1)
        elif result["downloaded"] > 0:
            logger.success(f"✅ Backfill completed - {result['downloaded']} month(s) downloaded")
            exit(0)
        else:
            logger.info("ℹ️  All months already downloaded")
            exit(0)
