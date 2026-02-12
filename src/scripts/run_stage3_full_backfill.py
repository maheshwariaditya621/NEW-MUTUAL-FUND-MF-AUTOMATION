from src.extractors.orchestrator import ExtractionOrchestrator
from src.utils.db_backup import backup_database
from src.config import logger
import time

def run_stage3():
    """
    Stage 3: Full Historical Backfill
    - Targets: HDFC, SBI (All supported AMCs)
    - Period: Jan 2024 to Dec 2025
    - Strategy: Sequential processing to maintain DB stability
    """
    
    # 1. Manual Backup before starting the heavy lift
    logger.info("PRE-RUN: Taking baseline database backup before Stage 3...")
    backup_path = backup_database()
    logger.info(f"Baseline backup created: {backup_path}")

    orchestrator = ExtractionOrchestrator()
    
    # Define scope
    from src.config.constants import AMC_MAPPING
    amcs = list(AMC_MAPPING.keys()) # ["hdfc", "sbi"] derived from keys
    years = [2024, 2025]
    months = range(1, 13) # 1 to 12

    total_stats = {"processed": 0, "skipped": 0, "errors": 0}

    for amc_key in amcs:
        logger.info(f"\n{'='*50}")
        logger.info(f"STARTING BACKFILL FOR AMC: {AMC_MAPPING[amc_key].upper()}")
        logger.info(f"{'='*50}")
        
        for year in years:
            for month in months:
                # Optional: Skip future months if running in current year
                if year == 2026 and month > 2:
                    continue

                # Force REDO for HDFC 2025 to apply Unit Normalization fix
                force_redo = (amc_key == "sbi") 
                
                logger.info(f"\n--- Processing {AMC_MAPPING[amc_key].upper()} {year}-{month:02d} (Redo: {force_redo}) ---")
                try:
                    result = orchestrator.process_amc_month(amc_key, year, month, redo=force_redo)
                    
                    if result["status"] == "success":
                        total_stats["processed"] += 1
                        # Small sleep to let DB settle/logs flush
                        time.sleep(1)
                    elif result["status"] == "skipped":
                        total_stats["skipped"] += 1
                    else:
                        total_stats["errors"] += 1
                        
                except Exception as e:
                    logger.error(f"CRITICAL FAILED for {amc_key} {year}-{month}: {e}")
                    total_stats["errors"] += 1

    logger.info(f"\n{'='*50}")
    logger.info("STAGE 3 COMPLETED")
    logger.info(f"Stats: {total_stats}")
    logger.info(f"{'='*50}")

if __name__ == "__main__":
    run_stage3()
