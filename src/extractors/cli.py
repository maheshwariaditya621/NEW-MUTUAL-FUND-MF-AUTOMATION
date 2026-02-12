import argparse
import sys
from src.config import logger
from src.extractors.orchestrator import ExtractionOrchestrator

def main():
    parser = argparse.ArgumentParser(description="Mutual Fund Portfolio Extractor CLI")
    parser.add_argument("--amc", required=True, help="AMC slug (e.g., hdfc, sbi)")
    parser.add_argument("--year", type=int, required=True, help="Year (YYYY)")
    parser.add_argument("--month", type=int, required=True, help="Month (MM)")
    parser.add_argument("--redo", action="store_true", help="Purge existing data and re-process")
    parser.add_argument("--dry-run", action="store_true", help="Extract and validate but do not load to DB")

    args = parser.parse_args()

    orchestrator = ExtractionOrchestrator()
    
    logger.info(f"Starting Extraction for {args.amc.upper()} {args.year}-{args.month:02d}")
    if args.dry_run:
        logger.info("MODE: DRY RUN (No DB Persistence)")
    if args.redo:
        logger.info("MODE: REDO (Purging existing data)")

    result = orchestrator.process_amc_month(
        amc_slug=args.amc,
        year=args.year,
        month=args.month,
        redo=args.redo,
        dry_run=args.dry_run
    )

    if result["status"] == "success":
        extracted_count = result.get('rows_inserted') or result.get('rows_read', 0)
        logger.info(f"SUCCESS: Extracted {extracted_count} holdings.")
    elif result["status"] == "skipped":
        logger.info(f"SKIPPED: {result.get('reason')}")
    else:
        logger.error(f"FAILED: {result.get('error') or result.get('reason')}")
        sys.exit(1)

if __name__ == "__main__":
    main()
