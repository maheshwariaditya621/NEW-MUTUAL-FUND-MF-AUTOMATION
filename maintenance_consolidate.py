"""
Maintenance script to manually trigger AMC consolidation.
Usage: python maintenance_consolidate.py --amc hdfc --year 2025 --month 11
"""

import argparse
from pathlib import Path
from src.utils import consolidate_amc_downloads
from src.config import logger

def main():
    parser = argparse.ArgumentParser(description="Manually trigger AMC portfolio consolidation.")
    parser.add_argument("--amc", type=str, required=True, help="AMC slug (e.g., hdfc, icici, ppfas)")
    parser.add_argument("--year", type=int, required=True, help="Year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")

    args = parser.parse_args()

    logger.info(f"Manual consolidation triggered for: {args.amc} {args.year}-{args.month:02d}")
    
    # Check if raw folder exists
    raw_folder = Path(f"data/raw/{args.amc}/{args.year}_{args.month:02d}")
    if not raw_folder.exists():
        logger.error(f"❌ Raw folder not found: {raw_folder}")
        return

    result = consolidate_amc_downloads(args.amc, args.year, args.month)
    
    if result:
        logger.success(f"✅ Consolidation successful! File: {result}")
    else:
        logger.error(f"❌ Consolidation failed for {args.amc} {args.year}-{args.month:02d}")

if __name__ == "__main__":
    main()
