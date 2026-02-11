
import logging
import sys
import shutil
import zipfile
from pathlib import Path
from openpyxl import Workbook

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.excel_merger import consolidate_amc_downloads

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_dummy_excel(path: Path):
    """Create a dummy excel file."""
    wb = Workbook()
    ws = wb.active
    ws["A1"] = "Test Data From ZIP"
    wb.save(path)

def create_dummy_zip(zip_path: Path, file_name: str):
    """Create a zip file containing a dummy excel file."""
    # Create excel in temp location
    temp_excel = zip_path.parent / file_name
    create_dummy_excel(temp_excel)
    
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        zipf.write(temp_excel, arcname=file_name)
    
    # Remove the temp excel so we only have the zip
    temp_excel.unlink()

def test_zip_consolidation():
    amc = "test_amc_zip"
    year = 2030
    month = 12
    
    # Setup paths
    raw_folder = Path(f"data/raw/{amc}/{year}_{month:02d}")
    if raw_folder.exists():
        shutil.rmtree(raw_folder)
    raw_folder.mkdir(parents=True, exist_ok=True)
    
    output_folder = Path(f"data/output/merged excels/{amc}/{year}")
    if output_folder.exists():
        shutil.rmtree(output_folder)
    
    # Create a ZIP file with an Excel inside
    zip_file = raw_folder / "test_data.zip"
    excel_name = "extracted_data.xlsx"
    create_dummy_zip(zip_file, excel_name)
    
    print("\n--- TEST: ZIP Extraction and Merge ---")
    logger.info(f"Created ZIP file at: {zip_file}")
    
    # Run consolidation
    # This should:
    # 1. Detect ZIP
    # 2. Extract 'extracted_data.xlsx'
    # 3. Merge it into CONSOLIDATED...
    result = consolidate_amc_downloads(amc, year, month)
    
    # Verification
    extracted_excel = raw_folder / excel_name
    if extracted_excel.exists():
        print("✅ SUCCESS: Excel file extracted from ZIP.")
    else:
        print("❌ FAILED: Excel file NOT extracted.")
        return

    if result and result.exists():
        print(f"✅ SUCCESS: Consolidated file created at {result}")
    else:
        print("❌ FAILED: Consolidated file NOT created.")
        return

    print("\n🎉 ZIP CONSOLIDATION TEST PASSED!")

    # Cleanup
    if raw_folder.exists():
        shutil.rmtree(f"data/raw/{amc}")
    if output_folder.exists():
        shutil.rmtree(f"data/output/merged excels/{amc}")

if __name__ == "__main__":
    test_zip_consolidation()
