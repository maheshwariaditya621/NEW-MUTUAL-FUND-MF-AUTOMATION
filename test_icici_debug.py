import pandas as pd
from src.extractors.icici_extractor_v1 import ICICIExtractorV1

ext = ICICIExtractorV1()
xls = pd.ExcelFile('data/output/merged excels/icici/2025/CONSOLIDATED_ICICI_2025_12.xlsx')

# Test single sheet
df = pd.read_excel(xls, sheet_name='Active Momentum MOMACT', skiprows=3)
df_mapped = ext._map_columns(df)

print(f'Total rows: {len(df_mapped)}')
print(f'Rows with ISIN: {df_mapped["isin"].notna().sum()}')

# Test equity filter
equity_df = ext.filter_equity_isins(df_mapped, 'isin')
print(f'Equity rows: {len(equity_df)}')

if len(equity_df) > 0:
    print('\nFirst 3 equity holdings:')
    print(equity_df[['isin', 'security_name', 'nav_percentage']].head(3))
else:
    print('\nNo equity rows found!')
    print('\nSample ISINs from original:')
    print(df_mapped[df_mapped['isin'].notna()]['isin'].head(10).tolist())
