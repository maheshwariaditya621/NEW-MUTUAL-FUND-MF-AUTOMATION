import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils import consolidate_amc_downloads
from src.config import logger

def main():
    parser = argparse.ArgumentParser(description="Consolidate AMC scheme-wise downloads into a single Excel file.")
    parser.add_argument("--amc", required=True, help="AMC slug (e.g., mirae_asset)")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g., 2025)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")

    args = parser.parse_args()
    
    logger.info(f"Manual consolidation request: {args.amc} for {args.year}-{args.month:02d}")
    
    result = consolidate_amc_downloads(args.amc, args.year, args.month)
    
    if result and result.exists():
        logger.success(f"Successfully consolidated: {result}")
        print(f"\n✅ SUCCESS: {result}")
    else:
        logger.error(f"Failed to consolidate {args.amc} for {args.year}-{args.month:02d}")
        print("\n❌ FAILED")

if __name__ == "__main__":
    main()
