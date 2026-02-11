import json
from pathlib import Path
from src.extractors.orchestrator import ExtractionOrchestrator
from src.config import logger

def run_hdfc_dry_run():
    orchestrator = ExtractionOrchestrator()
    
    amc_slug = "hdfc"
    year = 2025
    month = 12
    
    logger.info(f"Starting Dry-Run for {amc_slug.upper()} {year}-{month:02d}")
    
    result = orchestrator.process_amc_month(amc_slug, year, month, dry_run=True)
    
    # We need to peek into the extracted holdings. 
    # Since process_amc_month returns success/fail status, 
    # the actual holdings are not easily accessible unless we re-run extraction 
    # or modify it to return them.
    
    # For verification, let's re-run the extraction part directly 
    # to get the data for the 5-point check.
    from src.extractors.extractor_factory import ExtractorFactory
    extractor = ExtractorFactory.get_extractor(amc_slug, year, month)
    file_path = Path(f"data/output/merged excels/hdfc/2025/CONSOLIDATED_HDFC_2025_12.xlsx")
    
    holdings = extractor.extract(str(file_path))
    
    # Group by scheme
    schemes = {}
    for h in holdings:
        s_name = h['scheme_name']
        if s_name not in schemes:
            schemes[s_name] = []
        schemes[s_name].append(h)
        
    print(f"\n--- DRY RUN VERIFICATION REPORT ---")
    print(f"Total Schemes Detected: {len(schemes)}")
    
    for s_name, s_holdings in list(schemes.items())[:3]: # Check first 3 schemes
        print(f"\nScheme: {s_name}")
        print(f"Total Holdings: {len(s_holdings)}")
        
        # 3. Sum of % to NAV
        total_nav = sum(h.get('percent_to_nav', 0.0) for h in s_holdings)
        print(f"Total % to NAV: {total_nav:.2f}%")
        
        # 2. Top 5 Holdings
        sorted_h = sorted(s_holdings, key=lambda x: x.get('market_value_inr', 0), reverse=True)
        print(f"Top 5 Holdings:")
        for i, h in enumerate(sorted_h[:5]):
            print(f"  {i+1}. {h['company_name']} ({h['isin']}) - {h['percent_to_nav']}%")
            
        # 5. ISIN security code check (all must be 10)
        debt_found = [h['isin'] for h in s_holdings if h['isin'][8:10] != '10']
        if debt_found:
            print(f"  WARNING: Found debt ISINs: {debt_found[:3]}")
        else:
            print(f"  ISIN Security Code Check: PASS (All ISINs are equity/10)")

    # 4. Duplicate Companies Check (Canonicalization)
    for s_name, s_holdings in schemes.items():
        isins = [h['isin'] for h in s_holdings]
        if len(isins) != len(set(isins)):
            print(f"  WARNING: Duplicate ISINs found in {s_name}")

if __name__ == "__main__":
    run_hdfc_dry_run()
