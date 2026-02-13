
from src.extractors.orchestrator import ExtractionOrchestrator
from src.config import logger
import sys
import logging

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

def run_pipeline():
    orchestrator = ExtractionOrchestrator()
    try:
        logger.info("Starting Kotak Pipeline for Dec 2025...")
        report_path = orchestrator.process_amc_month("kotak", 2025, 12)
        logger.info(f"Pipeline completed successfully. Report generated at: {report_path}")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
