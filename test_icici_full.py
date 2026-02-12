import pandas as pd
import json
from src.extractors.icici_extractor_v1 import ICICIExtractorV1

ext = ICICIExtractorV1()

# Extract from one sheet only for testing
holdings = ext.extract('data/output/merged excels/icici/2025/CONSOLIDATED_ICICI_2025_12.xlsx')

print(f'Total holdings extracted: {len(holdings)}')

if holdings:
    print('\nFirst holding:')
    print(json.dumps(holdings[0], indent=2))
    
    print(f'\nSample NAV percentages:')
    for i in range(min(5, len(holdings))):
        print(f"  {holdings[i]['security_name'][:30]:30} - {holdings[i]['nav_percentage']:.4f}%")
else:
    print('No holdings extracted!')
