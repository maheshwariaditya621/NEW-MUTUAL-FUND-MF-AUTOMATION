from src.extractors.generic_extractor import GenericExtractor
from src.config import logger
import json

def run_dry_run():
    file_path = r"d:\CODING\NEW MUTUAL FUND MF AUTOMATION\data\output\merged excels\icici\2025\CONSOLIDATED_ICICI_2025_12.xlsx"
    
    logger.info("Starting ICICI Dry Run...")
    extractor = GenericExtractor("ICICI_PRU")
    
    results = extractor.extract(file_path)
    
    print(f"Total Records Extracted: {len(results)}")
    
    if results:
        print("\n--- Sample Record ---")
        print(json.dumps(results[0], indent=2))
        
        # Check scheme name cleaning
        schemes = set(r['scheme_name'] for r in results)
        print(f"\nUnique Schemes Found: {len(schemes)}")
        print("Sample Schemes:", list(schemes)[:5])
        
        # Check Value sums
        total_val = sum(r['market_value_inr'] for r in results)
        print(f"\nTotal Market Value: {total_val:,.2f}")
    else:
        print("No records extracted.")

if __name__ == "__main__":
    run_dry_run()
