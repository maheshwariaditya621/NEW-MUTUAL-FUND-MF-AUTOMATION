import pandas as pd
import os
from src.extractors.generic_extractor import GenericExtractor
from src.config import logger

def test_icici_extraction():
    # Create valid data
    data = [
        ["Metadata", "", "", "", "", "", ""],
        ["More Metadata", "", "", "", "", "", ""],
        ["", "", "", "", "", "", ""],
        ["", "", "", "", "", "", ""],
        ["", "", "", "", "", "", ""],
        ["ISIN", "Scheme Name", "Company Name", "Folio Number", "NAV", "Quantity", "Market Value", "Date"], 
        ["INE002A01023", "ICICI Prudential Bluechip Fund", "Reliance Industries", "N/A", 0, 1000.0, 2500000.0, "31-Jan-26"],
        ["INE123B01056", "ICICI Prudential Value Discovery Fund", "HDFC Bank", "N/A", 0, 500.0, 800000.0, "31-Jan-26"],
        ["INVALID", "Bad Scheme", "Bad Co", "", 0, 0, 0, ""], 
    ]
    
    df = pd.DataFrame(data)
    
    file_path = "temp_icici_mock.xlsx"
    with pd.ExcelWriter(file_path) as writer:
        df.to_excel(writer, sheet_name="Portfolio Statement", header=False, index=False)
        
    print(f"Created mock file: {file_path}")
    
    try:
        # 2. Extract
        extractor = GenericExtractor("ICICI_PRU")
        results = extractor.extract(file_path)
        
        print(f"Extracted {len(results)} records.")
        for r in results:
            print(r)
            
        # 3. Assertions
        assert len(results) == 2, f"Expected 2 records, got {len(results)}"
        assert results[0]['isin'] == "INE002A01023"
        assert results[0]['market_value_inr'] == 2500000.0
        assert results[0]['company_name'] == "Reliance Industries"
        
        print("✅ ICICI Extraction Test Passed!")
        
    except Exception as e:
        print(f"❌ Test Failed: {e}")
        # Print full traceback
        import traceback
        traceback.print_exc()
        
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

if __name__ == "__main__":
    test_icici_extraction()
