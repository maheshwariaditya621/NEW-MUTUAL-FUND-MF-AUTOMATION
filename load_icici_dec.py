"""
Load ICICI December 2025 data into database using orchestrator.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent))

from src.extractors.orchestrator import ExtractionOrchestrator
from src.config import logger

# Suppress debug logs
import logging
logging.getLogger('mf_analytics').setLevel(logging.INFO)

def main():
    orchestrator = ExtractionOrchestrator()
    
    amc = "icici"
    year = 2025
    month = 12
    
    print(f"\n{'='*60}")
    print(f"Loading ICICI December 2025 into Database")
    print(f"{'='*60}\n")
    
    try:
        result = orchestrator.process_amc_month(amc_slug=amc, year=year, month=month, redo=True)
        
        print(f"\n{'='*60}")
        print("LOAD SUMMARY")
        print(f"{'='*60}")
        print(f"Status: {result.get('status', 'unknown')}")
        print(f"Holdings extracted: {result.get('holdings_extracted', 0)}")
        print(f"Schemes loaded: {result.get('schemes_loaded', 0)}")
        print(f"Duration: {result.get('duration', 0):.2f}s")
        
        if result.get('status') == 'success':
            print(f"\n✅ Successfully loaded ICICI data into database")
            print(f"   Reconciliation report: reports/reconciliation_icici_2025_12.csv")
        else:
            print(f"\n❌ Failed to load ICICI data")
            print(f"   Reason: {result.get('reason', 'Unknown')}")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
