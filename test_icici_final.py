from src.extractors.icici_extractor_v1 import ICICIExtractorV1
import logging

# Suppress debug logs
logging.getLogger('mf_analytics').setLevel(logging.WARNING)

ext = ICICIExtractorV1()
holdings = ext.extract('data/output/merged excels/icici/2025/CONSOLIDATED_ICICI_2025_12.xlsx')

print(f'✓ Total holdings extracted: {len(holdings)}')

if holdings:
    print(f'\n✓ First 5 holdings:')
    for h in holdings[:5]:
        print(f"  {h['scheme_name'][:40]:40} - NAV: {h['percent_to_nav']:6.2f}%")
    
    # Group by scheme
    schemes = {}
    for h in holdings:
        scheme = h['scheme_name']
        if scheme not in schemes:
            schemes[scheme] = []
        schemes[scheme].append(h)
    
    print(f'\n✓ Schemes with equity: {len(schemes)}')
    print(f'\n✓ First 5 schemes with NAV totals:')
    for i, (scheme, holdings_list) in enumerate(list(schemes.items())[:5]):
        total_nav = sum(h['percent_to_nav'] for h in holdings_list)
        print(f"  {scheme[:50]:50} - {len(holdings_list):3} holdings, NAV: {total_nav:6.2f}%")
    
    # Validation summary
    print(f'\n✓ Validation Summary:')
    valid_schemes = [s for s, hl in schemes.items() if 90 <= sum(h['percent_to_nav'] for h in hl) <= 105]
    print(f"  Schemes passing NAV validation (90-105%): {len(valid_schemes)}/{len(schemes)}")
    
    print(f'\n✓ Sample holding details:')
    sample = holdings[0]
    print(f"  Scheme: {sample['scheme_name']}")
    print(f"  Security: {sample['security_name']}")
    print(f"  ISIN: {sample['isin']}")
    print(f"  Market Value: ₹{sample['market_value']:,.2f}")
    print(f"  NAV %: {sample['percent_to_nav']:.4f}%")
    print(f"  Report Date: {sample['report_date']}")
else:
    print('✗ No holdings extracted!')
