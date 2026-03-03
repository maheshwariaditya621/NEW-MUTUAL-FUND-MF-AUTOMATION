"""
run_hybrid_pipeline.py

This script is designed to be run LOCALLY.
It performs the following:
1. Downloads AMC data (using local residential IP to avoid blocks).
2. Merges and extracts the data.
3. Uploads the processed holdings directly to the REMOTE AWS Database.

Usage:
    python src/scripts/run_hybrid_pipeline.py --amc nippon --year 2026 --month 1
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Force local environment to point to AWS if configured
# You should set DB_HOST to your AWS EC2 IP/DNS in your local .env
from src.config import logger, DB_HOST
from src.downloaders.downloader_orchestrator import DownloaderOrchestrator
from src.extractors.orchestrator import ExtractionOrchestrator

def run_pipeline(amc_slug: str, year: int, month: int, dry_run: bool = False, redo: bool = False):
    logger.info("=" * 60)
    logger.info(f"🚀 STARTING HYBRID PIPELINE: {amc_slug.upper()} {year}-{month:02d}")
    logger.info(f"Target Database Host: {DB_HOST}")
    logger.info("=" * 60)

    # 1. DOWNLOAD
    logger.info("\n--- PHASE 1: DOWNLOAD ---")
    downloader = DownloaderOrchestrator()
    dl_result = downloader.download_amc_month(amc_slug, year, month)
    
    if dl_result.get("status") not in ["success", "skipped"]:
        logger.error(f"❌ Download failed: {dl_result.get('reason')}")
        return dl_result

    # 2. EXTRACTION & LOAD
    # The ExtractionOrchestrator handles merging (internally in some extractors) 
    # and loading into the DB defined in src.config
    logger.info("\n--- PHASE 2: EXTRACTION & REMOTE LOAD ---")
    extractor = ExtractionOrchestrator()
    ext_result = extractor.process_amc_month(
        amc_slug=amc_slug, 
        year=year, 
        month=month, 
        redo=redo, 
        dry_run=dry_run
    )

    if ext_result.get("status") == "success":
        logger.success(f"✅ Pipeline completed successfully for {amc_slug}")
        if not dry_run:
            logger.info(f"Rows Inserted to AWS: {ext_result.get('rows_inserted')}")
    else:
        logger.error(f"❌ Extraction/Load failed: {ext_result.get('reason') or ext_result.get('error')}")

    return ext_result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hybrid Local-to-Remote Pipeline")
    parser.add_argument("--amc", type=str, required=True, help="AMC slug (e.g., nippon, icici)")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true", help="Do not load into DB")
    parser.add_argument("--redo", action="store_true", help="Purge existing data and re-run")
    
    args = parser.parse_args()
    
    try:
        run_pipeline(args.amc, args.year, args.month, args.dry_run, args.redo)
    except KeyboardInterrupt:
        logger.info("Pipeline manually interrupted.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Pipeline crashed: {e}")
        sys.exit(1)
