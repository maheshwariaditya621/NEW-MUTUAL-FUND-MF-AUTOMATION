
import logging
import time
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.excel_merger import consolidate_amc_downloads
from src.config import logger

# Setup logging
logging.basicConfig(level=logging.INFO)

def touch_file(path: Path):
    """Update file modification time to now."""
    if path.exists():
        path.touch()

def create_dummy_excel(path: Path):
    """Create a dummy excel file."""
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws["A1"] = "Test Data"
    wb.save(path)

def test_smart_consolidation():
    amc = "test_amc_smart"
    year = 2029
    month = 12
    
    # Setup paths
    raw_folder = Path(f"data/raw/{amc}/{year}_{month:02d}")
    raw_folder.mkdir(parents=True, exist_ok=True)
    
    output_folder = Path(f"data/output/merged excels/{amc}/{year}")
    output_file = output_folder / f"CONSOLIDATED_{amc.upper()}_{year}_{month:02d}.xlsx"
    
    # Cleanup previous runs
    if output_file.exists():
        output_file.unlink()
    
    # Create raw files
    raw_file1 = raw_folder / "file1.xlsx"
    create_dummy_excel(raw_file1)
    
    print("\n--- TEST 1: Initial Merge (Should Create) ---")
    result = consolidate_amc_downloads(amc, year, month)
    assert result is not None
    assert result.exists()
    initial_mtime = result.stat().st_mtime
    print("✅ Initial merge successful.")
    
    print("\n--- TEST 2: No Changes (Should Skip) ---")
    # Ensure raw files are older than output (they should be, but let's be safe)
    # Windows filesystem mtime resolution can be coarse, so pause briefly
    time.sleep(1.1) 
    
    result = consolidate_amc_downloads(amc, year, month)
    assert result is not None
    current_mtime = result.stat().st_mtime
    
    if current_mtime == initial_mtime:
        print("✅ Skipped merge as expected (timestamps unchanged).")
    else:
        print(f"❌ FAILED: File was modified! {initial_mtime} -> {current_mtime}")
        return

    print("\n--- TEST 3: New Raw File (Should Update) ---")
    time.sleep(1.1)
    # Update raw file mtime
    touch_file(raw_file1)
    
    result = consolidate_amc_downloads(amc, year, month)
    assert result is not None
    current_mtime = result.stat().st_mtime
    
    if current_mtime > initial_mtime:
        print("✅ Updated merge as expected (raw file newer).")
    else:
        print(f"❌ FAILED: File was NOT modified! {initial_mtime} vs {current_mtime}")
        return

    print("\n🎉 ALL SMART CONSOLIDATION TESTS PASSED!")

    # Cleanup
    import shutil
    if raw_folder.exists():
        shutil.rmtree(f"data/raw/{amc}")
    if output_folder.exists():
        shutil.rmtree(f"data/output/merged excels/{amc}")

if __name__ == "__main__":
    test_smart_consolidation()
