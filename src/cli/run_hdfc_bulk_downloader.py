"""
Bulk downloader CLI for HDFC Mutual Fund.

Downloads multiple months of portfolio data in one command.
Supports optional date range - if not provided, uses auto mode.
"""

import argparse
from src.scheduler.hdfc_backfill import run_hdfc_backfill
from src.config import logger


def main():
    """Bulk download HDFC portfolio data."""
    parser = argparse.ArgumentParser(
        description="HDFC Mutual Fund Bulk Downloader",
        epilog="Example: python -m src.cli.run_hdfc_bulk_downloader --start-year 2024 --start-month 4 --end-year 2025 --end-month 3"
    )
    
    parser.add_argument("--start-year", type=int, help="Start year (YYYY)")
    parser.add_argument("--start-month", type=int, help="Start month (1-12)")
    parser.add_argument("--end-year", type=int, help="End year (YYYY)")
    parser.add_argument("--end-month", type=int, help="End month (1-12)")
    
    args = parser.parse_args()
    
    # Check if all or none of the date arguments are provided
    date_args = [args.start_year, args.start_month, args.end_year, args.end_month]
    provided_count = sum(x is not None for x in date_args)
    
    if provided_count > 0 and provided_count < 4:
        logger.error("Error: Either provide all date arguments or none")
        logger.error("Required: --start-year, --start-month, --end-year, --end-month")
        return 1
    
    # Validate months if provided
    if args.start_month is not None:
        if args.start_month < 1 or args.start_month > 12:
            logger.error("Invalid start month. Must be between 1 and 12.")
            return 1
    
    if args.end_month is not None:
        if args.end_month < 1 or args.end_month > 12:
            logger.error("Invalid end month. Must be between 1 and 12.")
            return 1
    
    # Run backfill
    try:
        result = run_hdfc_backfill(
            start_year=args.start_year,
            start_month=args.start_month,
            end_year=args.end_year,
            end_month=args.end_month
        )
        
        # Determine exit code
        if result["downloaded"] > 0:
            logger.success(f"✅ Bulk download completed - {result['downloaded']} month(s) downloaded")
            return 0
        elif result["skipped"] > 0 and result["failed"] == 0:
            logger.info("ℹ️  All months already downloaded")
            return 0
        elif result["failed"] > 0 and result["downloaded"] == 0:
            logger.error("❌ All months failed")
            return 1
        else:
            logger.info("ℹ️  Bulk download completed")
            return 0
    
    except Exception as e:
        logger.error(f"❌ Bulk download failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())
