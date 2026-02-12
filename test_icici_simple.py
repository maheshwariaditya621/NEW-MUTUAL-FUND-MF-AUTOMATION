from src.extractors.icici_extractor_v1 import ICICIExtractorV1
import logging

# Suppress debug logs
logging.getLogger('mf_analytics').setLevel(logging.WARNING)

ext = ICICIExtractorV1()
holdings = ext.extract('data/output/merged excels/icici/2025/CONSOLIDATED_ICICI_2025_12.xlsx')

print(f'Total holdings: {len(holdings)}')

if holdings:
    print(f'\nFirst 5 holdings:')
    for h in holdings[:5]:
        print(f"  {h['scheme_name'][:40]:40} - NAV: {h['nav_percentage']:6.2f}%")
    
    # Group by scheme
    schemes = {}
    for h in holdings:
        scheme = h['scheme_name']
        if scheme not in schemes:
            schemes[scheme] = []
        schemes[scheme].append(h)
    
    print(f'\nSchemes with equity: {len(schemes)}')
    print(f'\nFirst 3 schemes with NAV totals:')
    for i, (scheme, holdings_list) in enumerate(list(schemes.items())[:3]):
        total_nav = sum(h['nav_percentage'] for h in holdings_list)
        print(f"  {scheme[:40]:40} - {len(holdings_list):3} holdings, Total NAV: {total_nav:6.2f}%")
else:
    print('No holdings extracted!')
