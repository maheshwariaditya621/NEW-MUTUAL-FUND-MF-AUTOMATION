"""
ICICI Prudential December 2025 Verification Script

Verifies ICICI extractor functionality by:
1. Extracting data from December 2025 merged file
2. Displaying summary statistics
3. Validating data quality
"""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.extractors.icici_extractor_v1 import ICICIExtractorV1

# Setup logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

def verify_icici_dec2025():
    """Verify ICICI extraction for December 2025."""
    amc = "icici"
    year = 2025
    month = 12
    
    file_path = Path(f"data/output/merged excels/{amc}/{year}/CONSOLIDATED_ICICI_2025_12.xlsx")
    
    if not file_path.exists():
        print(f"❌ FAILED: Consolidated file not found: {file_path}")
        return False
    
    print(f"✅ Found file: {file_path}")
    print(f"\n{'='*60}")
    print("ICICI PRUDENTIAL - DECEMBER 2025 EXTRACTION")
    print(f"{'='*60}\n")
    
    # Extract data
    print("Extracting data...")
    extractor = ICICIExtractorV1()
    holdings = extractor.extract(str(file_path))
    
    if not holdings:
        print("❌ FAILED: No holdings extracted")
        return False
    
    # Calculate statistics
    total_holdings = len(holdings)
    schemes = {}
    for h in holdings:
        scheme = h['scheme_name']
        if scheme not in schemes:
            schemes[scheme] = []
        schemes[scheme].append(h)
    
    total_schemes = len(schemes)
    
    # Validation checks
    print(f"\n📊 EXTRACTION SUMMARY")
    print(f"{'─'*60}")
    print(f"Total Holdings:     {total_holdings:,}")
    print(f"Total Schemes:      {total_schemes}")
    print(f"Avg Holdings/Scheme: {total_holdings/total_schemes:.1f}")
    
    # NAV validation
    print(f"\n✓ NAV VALIDATION")
    print(f"{'─'*60}")
    valid_schemes = 0
    invalid_schemes = []
    
    for scheme_name, scheme_holdings in schemes.items():
        total_nav = sum(h['percent_to_nav'] for h in scheme_holdings)
        if 90 <= total_nav <= 105:
            valid_schemes += 1
        else:
            invalid_schemes.append((scheme_name, total_nav))
    
    print(f"Schemes passing NAV check (90-105%): {valid_schemes}/{total_schemes}")
    
    if invalid_schemes:
        print(f"\n⚠️  Schemes with NAV issues:")
        for scheme, nav in invalid_schemes[:5]:
            print(f"  - {scheme[:50]:50} NAV: {nav:6.2f}%")
    
    # ISIN validation
    print(f"\n✓ ISIN VALIDATION")
    print(f"{'─'*60}")
    valid_isins = sum(1 for h in holdings if h['isin'].startswith('INE') and len(h['isin']) == 12)
    print(f"Valid ISINs (INE + 12 chars): {valid_isins}/{total_holdings}")
    
    # Sample holdings
    print(f"\n📋 SAMPLE HOLDINGS (First 5)")
    print(f"{'─'*60}")
    for i, h in enumerate(holdings[:5], 1):
        print(f"\n{i}. {h['security_name'][:40]}")
        print(f"   Scheme: {h['scheme_name'][:45]}")
        print(f"   ISIN: {h['isin']}")
        print(f"   Market Value: ₹{h['market_value']:,.2f}")
        print(f"   NAV %: {h['percent_to_nav']:.4f}%")
    
    # Top schemes by holdings count
    print(f"\n📈 TOP 5 SCHEMES BY HOLDINGS COUNT")
    print(f"{'─'*60}")
    sorted_schemes = sorted(schemes.items(), key=lambda x: len(x[1]), reverse=True)
    for i, (scheme_name, scheme_holdings) in enumerate(sorted_schemes[:5], 1):
        total_nav = sum(h['percent_to_nav'] for h in scheme_holdings)
        print(f"{i}. {scheme_name[:45]:45} - {len(scheme_holdings):3} holdings, NAV: {total_nav:6.2f}%")
    
    # Success criteria
    print(f"\n{'='*60}")
    print("VERIFICATION RESULTS")
    print(f"{'='*60}\n")
    
    success = True
    
    if total_holdings < 1000:
        print(f"❌ FAILED: Too few holdings ({total_holdings}). Expected > 1000")
        success = False
    else:
        print(f"✅ Holdings count: {total_holdings:,}")
    
    if total_schemes < 50:
        print(f"❌ FAILED: Too few schemes ({total_schemes}). Expected > 50")
        success = False
    else:
        print(f"✅ Schemes count: {total_schemes}")
    
    if valid_schemes != total_schemes:
        print(f"⚠️  WARNING: {total_schemes - valid_schemes} schemes failed NAV validation")
    else:
        print(f"✅ All schemes pass NAV validation")
    
    if valid_isins != total_holdings:
        print(f"⚠️  WARNING: {total_holdings - valid_isins} invalid ISINs")
    else:
        print(f"✅ All ISINs valid")
    
    print()
    return success

if __name__ == "__main__":
    success = verify_icici_dec2025()
    sys.exit(0 if success else 1)
