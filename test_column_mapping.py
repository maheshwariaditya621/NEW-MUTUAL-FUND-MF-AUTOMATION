import pandas as pd
from src.extractors.icici_extractor_v1 import ICICIExtractorV1

ext = ICICIExtractorV1()
xls = pd.ExcelFile('data/output/merged excels/icici/2025/CONSOLIDATED_ICICI_2025_12.xlsx')

# Test Active Momentum sheet
sheet_name = 'Active Momentum MOMACT'
df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=30)
scheme_name = ext._extract_scheme_name(df_raw, sheet_name)

header_idx = ext.find_header_row(df_raw)
df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)

print(f"Original columns: {df.columns.tolist()}")
print(f"\nColumn mapping: {ext.column_mapping}")

df_mapped = ext._map_columns(df)
print(f"\nMapped columns: {df_mapped.columns.tolist()}")
print(f"\nIs 'nav_percentage' in mapped columns? {'nav_percentage' in df_mapped.columns}")
print(f"Is '% to Nav' in original columns? {'% to Nav' in df.columns}")

# Now filter
equity_df = ext.filter_equity_isins(df_mapped, 'isin')
print(f"\nEquity df columns: {equity_df.columns.tolist()}")
print(f"Is 'nav_percentage' in equity_df? {'nav_percentage' in equity_df.columns}")

if len(equity_df) > 0:
    print(f"\nFirst row from equity_df:")
    print(equity_df.iloc[0][['isin', 'security_name', 'nav_percentage']])
