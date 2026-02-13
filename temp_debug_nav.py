import pandas as pd
import openpyxl

# Load the Axis merged file
file_path = r"data\output\merged excels\axis\2025\CONSOLIDATED_AXIS_2025_12.xlsx"

# Read a sample equity sheet
sheet_name = "%20Portfolio %% AXISBCF"  # Axis Bluechip Fund
print(f"Examining sheet: {sheet_name}")
print("=" * 100)

# Read with pandas to find header
df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=10)

print("First 10 rows (raw):")
for idx in range(10):
    row_values = df_raw.iloc[idx].values
    print(f"Row {idx}: {[str(v)[:50] if pd.notna(v) else 'NaN' for v in row_values[:8]]}")

print("\n" + "=" * 100)

# Now read with header at row 3
df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=3, nrows=10)
print(f"\nColumns found: {list(df.columns)}")
print(f"\nFirst 5 data rows:")
print(df.head())

# Check specific column values
print("\n" + "=" * 100)
print("Checking NAV column values:")
for col in df.columns:
    if 'NAV' in str(col).upper() or 'NET' in str(col).upper() or 'ASSETS' in str(col).upper():
        print(f"\nColumn: {col}")
        print(f"Sample values: {df[col].head().tolist()}")
