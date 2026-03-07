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
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Force local environment to point to AWS if configured
# You should set DB_HOST to your AWS EC2 IP/DNS in your local .env
from src.config import logger, DB_HOST
from src.downloaders.downloader_orchestrator import PipelineOrchestrator

def run_hybrid(amc_slug: str, year: int, month: int, dry_run: bool = False, redo: bool = False):
    logger.info("=" * 60)
    logger.info(f"🚀 STARTING HYBRID PIPELINE: {amc_slug.upper()} {year}-{month:02d}")
    logger.info(f"Target Database Host: {DB_HOST}")
    logger.info("=" * 60)

    # Use the existing PipelineOrchestrator which handles Download -> Merge -> Extract -> Load
    orchestrator = PipelineOrchestrator()
    
    # We run all steps: download the raw data, merge into Excel, then extract and load to DB
    steps = ["download", "merge", "extract"]
    
    result = orchestrator.run_pipeline(
        amc_slug=amc_slug,
        year=year,
        month=month,
        steps=steps,
        dry_run=dry_run,
        redo=redo
    )

    status = result.get("status")
    
    if status == "success":
        logger.success(f"[SUCCESS] Hybrid Pipeline completed successfully for {amc_slug}")
        # Show extraction stats if available
        ext_res = result.get("steps", {}).get("extract", {})
        if ext_res:
             logger.info(f"Rows Inserted to AWS: {ext_res.get('rows_inserted', 0)}")
             
    elif status == "stopped":
        reason = result.get("reason", "Internal stop")
        logger.info(f"[INFO]  Pipeline stopped gracefully: {reason}")
        
    elif status == "failed":
        # Check for specific step failures
        failed_step = next((s for s, r in result.get("steps", {}).items() if r.get("status") in ["failed", "error"]), None)
        if failed_step:
            step_res = result["steps"][failed_step]
            step_reason = step_res.get("reason") or step_res.get("error") or "Unknown error"
            logger.error(f"[ERROR] Hybrid Pipeline failed at '{failed_step}' step: {step_reason}")
        else:
            logger.error(f"[ERROR] Hybrid Pipeline failed: {result.get('reason') or 'Unknown error'}")
            
    else:
        logger.warning(f"⚠️ Pipeline finished with status: {status}")

    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hybrid Local-to-Remote Pipeline")
    parser.add_argument("--amc", type=str, required=True, help="AMC slug (e.g., nippon, icici)")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true", help="Do not load into DB")
    parser.add_argument("--redo", action="store_true", help="Purge existing data and re-run")
    
    args = parser.parse_args()
    
    try:
        run_hybrid(args.amc, args.year, args.month, args.dry_run, args.redo)
    except KeyboardInterrupt:
        logger.info("Pipeline manually interrupted.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Pipeline crashed: {e}")
        sys.exit(1)
