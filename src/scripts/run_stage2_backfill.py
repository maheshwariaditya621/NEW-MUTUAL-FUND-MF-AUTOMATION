from src.extractors.orchestrator import ExtractionOrchestrator
from src.utils.db_backup import backup_database
from src.config import logger

def run_stage2():
    # 1. Manual Backup
    logger.info("PRE-RUN: Taking manual database backup...")
    backup_path = backup_database()
    logger.info(f"Backup snapshot created: {backup_path}")

    orchestrator = ExtractionOrchestrator()
    
    amc_slug = "hdfc"
    # Stage 2: Last 2-3 months available
    # We found 11 and 12 in data/output/merged excels/hdfc/2025/
    months = [11, 12]
    
    for month in months:
        logger.info(f"\n--- Processing HDFC 2025-{month:02d} ---")
        result = orchestrator.process_amc_month(amc_slug, 2025, month, redo=True)
        logger.info(f"Result for {month:02d}: {result}")

if __name__ == "__main__":
    run_stage2()
