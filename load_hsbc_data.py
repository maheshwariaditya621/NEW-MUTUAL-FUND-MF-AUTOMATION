"""
Load HSBC Data via Orchestrator.
"""
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(os.getcwd())

from src.extractors.orchestrator import ExtractionOrchestrator
from src.config import logger

def load_hsbc():
    orchestrator = ExtractionOrchestrator()
    
    # Process HSBC Dec 2025
    # redo=True to ensure clean slate (though it's first run)
    result = orchestrator.process_amc_month(
        amc_slug="hsbc",
        year=2025,
        month=12,
        redo=True
    )
    
    print("\nOrchestrator Result:")
    print(result)

if __name__ == "__main__":
    load_hsbc()
