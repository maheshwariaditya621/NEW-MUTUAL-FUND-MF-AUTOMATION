import pandas as pd
from src.extractors.icici_extractor_v1 import ICICIExtractorV1
from src.config import logger
import logging

# Disable debug logs for now to see clearly
logger.setLevel(logging.INFO)

def check_integrity():
    icici_path = r"d:\CODING\NEW MUTUAL FUND MF AUTOMATION\data\output\merged excels\icici\2025\CONSOLIDATED_ICICI_2025_12.xlsx"
    extractor = ICICIExtractorV1()
    
    holdings = extractor.extract(icici_path)
    
    print(f"Total holdings extracted: {len(holdings)}")
    
    df = pd.DataFrame(holdings)
    
    # Check for missing company names
    missing_names = df[df['company_name'].isna() | (df['company_name'] == "N/A")]
    print(f"Holdings with missing company name (N/A): {len(missing_names)}")
    if not missing_names.empty:
        print("Sample missing names:")
        print(missing_names.head(10)[['scheme_name', 'isin', 'company_name']])

    # Check for empty ISINs (shouldn't happen with our filter)
    missing_isins = df[df['isin'].isna() | (df['isin'] == "")]
    print(f"Holdings with missing ISIN: {len(missing_isins)}")

    # Group by scheme and see counts
    scheme_counts = df.groupby('scheme_name').size()
    print("\nHoldings per Scheme (Top 20):")
    print(scheme_counts.sort_values(ascending=False).head(20))

    # Inspect a few random records
    print("\nSample records:")
    print(df.sample(min(10, len(df)))[['scheme_name', 'isin', 'company_name', 'quantity', 'market_value_inr', 'percent_to_nav']])

if __name__ == "__main__":
    check_integrity()
