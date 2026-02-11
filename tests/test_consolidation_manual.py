import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils import consolidate_amc_downloads
from src.config import logger

def test_mirae_consolidation():
    amc = "mirae_asset"
    year = 2025
    month = 12
    
    logger.info(f"Testing consolidation for {amc} {year}-{month:02d}")
    result = consolidate_amc_downloads(amc, year, month)
    
    if result and result.exists():
        # Verify the path matches new requirement
        expected_part = f"data\\output\\merged excels\\{amc}\\{year}"
        if expected_part in str(result):
            logger.success(f"TEST PASSED: Consolidated file exists at {result}")
            print(f"\n✅ SUCCESS: {result}")
        else:
            logger.error(f"TEST FAILED: Path structure mismatch. Got {result}")
            print(f"\n❌ FAILED: Structure mismatch")
    else:
        logger.error("TEST FAILED: Consolidated file not created.")
        print("\n❌ FAILED: No file created")

if __name__ == "__main__":
    test_mirae_consolidation()
