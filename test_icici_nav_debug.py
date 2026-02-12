import pandas as pd
from src.extractors.icici_extractor_v1 import ICICIExtractorV1

ext = ICICIExtractorV1()
xls = pd.ExcelFile('data/output/merged excels/icici/2025/CONSOLIDATED_ICICI_2025_12.xlsx')

# Test single sheet manually
sheet_name = 'Active Momentum MOMACT'
df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=30)
scheme_name = ext._extract_scheme_name(df_raw, sheet_name)
report_date = ext._extract_report_date(df_raw, 'test.xlsx')

header_idx = ext.find_header_row(df_raw)
df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
df_mapped = ext._map_columns(df)

print(f'Scheme: {scheme_name}')
print(f'Report date: {report_date}')
print(f'Header index: {header_idx}')
print(f'\nMapped columns: {df_mapped.columns.tolist()}')

equity_df = ext.filter_equity_isins(df_mapped, 'isin')
print(f'\nEquity rows: {len(equity_df)}')

# Build holdings manually
holdings = []
for idx, row in equity_df.iterrows():
    nav_decimal = ext.safe_float(row.get('nav_percentage', 0))
    nav_pct = nav_decimal * 100
    
    holding = {
        'scheme_name': scheme_name,
        'security_name': ext.clean_company_name(row.get('security_name')),
        'nav_percentage': nav_pct
    }
    holdings.append(holding)
    
    if idx < 3:  # Print first 3
        print(f'\nRow {idx}:')
        print(f'  Raw nav_decimal: {nav_decimal}')
        print(f'  After *100: {nav_pct}')
        print(f'  In holding dict: {holding["nav_percentage"]}')

# Check total
total_nav = sum(h['nav_percentage'] for h in holdings)
print(f'\nTotal NAV: {total_nav:.2f}%')
print(f'Validation range: 90-105%')
print(f'Would pass: {90 <= total_nav <= 105}')
