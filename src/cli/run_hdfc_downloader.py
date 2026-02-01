"""
CLI wrapper for HDFC downloader.

Recommended way to run HDFC downloader.
"""

import argparse
from src.downloaders.hdfc_downloader import HDFCDownloader
from src.config import logger


def main():
    """Main CLI entrypoint for HDFC downloader."""
    parser = argparse.ArgumentParser(
        description="HDFC Mutual Fund Portfolio Downloader",
        epilog="Example: python -m src.cli.run_hdfc_downloader --year 2025 --month 1"
    )
    
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Calendar year (YYYY), e.g., 2025"
    )
    
    parser.add_argument(
        "--month",
        type=int,
        required=True,
        help="Month (1-12), e.g., 1 for January"
    )
    
    args = parser.parse_args()
    
    # Validate month
    if args.month < 1 or args.month > 12:
        logger.error("Invalid month. Must be between 1 and 12.")
        return 1
    
    # Create downloader and execute
    try:
        downloader = HDFCDownloader()
        result = downloader.download(year=args.year, month=args.month)
        
        if result["status"] == "success":
            logger.info(f"✅ Success: Downloaded {result['files_downloaded']} file(s)")
            logger.info(f"Files saved to: {result['files'][0].rsplit('/', 1)[0] if result['files'] else 'N/A'}")
            return 0
        else:
            logger.error(f"❌ Failed: {result.get('reason', 'Unknown error')}")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())
