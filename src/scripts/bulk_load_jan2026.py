from datetime import datetime
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.extractors.orchestrator import ExtractionOrchestrator
from src.extractors.extractor_factory import ADDITIONAL_AMC_NAMES
from src.config import logger

def main() -> None:
    year, month = 2026, 1
    results = []

    orchestrator = ExtractionOrchestrator()
    # Use ADDITIONAL_AMC_NAMES keys as the list of AMC slugs
    # Also add standard ones if not in additional
    amc_slugs = sorted(list(ADDITIONAL_AMC_NAMES.keys()))
    
    # Ensure hdfc, sbi, icici etc are included (though they usually are in factory maps)
    core_amcs = ["hdfc", "sbi", "icici", "kotak", "axis", "nippon", "absl", "uti", "ppfas", "hsbc", "bajaj", "angelone"]
    for slug in core_amcs:
        if slug not in amc_slugs:
            amc_slugs.append(slug)

    started = datetime.now()
    logger.info(f"Bulk Extraction/Load Batch for {year}-{month:02d} started at {started}")

    total_stats = {"success": 0, "skipped": 0, "failed": 0, "not_found": 0}

    for amc_slug in amc_slugs:
        logger.info(f"\n--- Processing {amc_slug.upper()} ---")
        try:
            # Running with redo=False by default to be idempotent
            result = orchestrator.process_amc_month(amc_slug, year, month, redo=False)
            
            status = result.get("status")
            if status == "success":
                total_stats["success"] += 1
            elif status == "skipped":
                total_stats["skipped"] += 1
            elif result.get("reason") == "file_not_found":
                total_stats["not_found"] += 1
            else:
                total_stats["failed"] += 1
            
            logger.info(f"Result for {amc_slug}: {result}")
            results.append({"amc": amc_slug, "result": result})
            
        except Exception as e:
            logger.error(f"CRITICAL ERROR for {amc_slug}: {e}")
            total_stats["failed"] += 1
            results.append({"amc": amc_slug, "result": {"status": "error", "message": str(e)}})

    ended = datetime.now()
    logger.info("\n=== BULK LOAD SUMMARY ===")
    logger.info(f"Started: {started}")
    logger.info(f"Ended:   {ended}")
    logger.info(f"Duration: {ended - started}")
    logger.info(f"Stats: {total_stats}")

if __name__ == "__main__":
    main()
