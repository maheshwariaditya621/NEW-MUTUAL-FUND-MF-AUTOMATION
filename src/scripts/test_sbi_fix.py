from src.extractors.orchestrator import ExtractionOrchestrator
from src.config import logger
from src.db.connection import get_cursor, close_connection
from src.config.constants import AMC_SBI

def test_sbi_fix():
    print("Testing SBI Extraction with Loose Filter...")
    
    # 1. Run Orchestrator for SBI Dec 2025 (Redo)
    orchestrator = ExtractionOrchestrator()
    result = orchestrator.process_amc_month("sbi", 2025, 12, redo=True)
    print(f"Extraction Result: {result}")
    
    # 2. Check Scheme Count in DB
    cursor = get_cursor()
    cursor.execute("""
        SELECT count(DISTINCT s.scheme_name)
        FROM schemes s
        JOIN scheme_snapshots ss ON s.scheme_id = ss.scheme_id
        JOIN periods p ON ss.period_id = p.period_id
        JOIN amcs a ON s.amc_id = a.amc_id
        WHERE a.amc_name = %s
          AND p.year = 2025 AND p.month = 12
    """, (AMC_SBI,))
    count = cursor.fetchone()[0]
    print(f"New Scheme Count for SBI Dec 2025: {count}")
    
    close_connection()

if __name__ == "__main__":
    test_sbi_fix()
