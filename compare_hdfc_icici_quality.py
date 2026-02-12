"""
Compare HDFC vs ICICI company name quality in database.
"""
from src.db import get_connection

conn = get_connection()
cur = conn.cursor()

print("="*80)
print("HDFC vs ICICI COMPANY NAME QUALITY COMPARISON")
print("="*80)

# 1. Check HDFC company names
print("\n1. HDFC Company Names:")
cur.execute("""
    SELECT c.company_name, c.isin, COUNT(*) as holding_count
    FROM companies c
    JOIN equity_holdings eh ON c.company_id = eh.company_id
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    WHERE a.amc_name = 'HDFC Mutual Fund'
    GROUP BY c.company_name, c.isin
    ORDER BY holding_count DESC
    LIMIT 10
""")
print("   Top 10 HDFC companies:")
for row in cur.fetchall():
    print(f"     {row[0][:50]:50} | {row[1]} | {row[2]} holdings")

cur.execute("""
    SELECT COUNT(DISTINCT c.company_id)
    FROM companies c
    JOIN equity_holdings eh ON c.company_id = eh.company_id
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    WHERE a.amc_name = 'HDFC Mutual Fund'
    AND c.company_name = 'N/A'
""")
hdfc_na_count = cur.fetchone()[0]
print(f"\n   HDFC companies with 'N/A': {hdfc_na_count}")

# 2. Check ICICI company names
print("\n2. ICICI Company Names:")
cur.execute("""
    SELECT c.company_name, c.isin, COUNT(*) as holding_count
    FROM companies c
    JOIN equity_holdings eh ON c.company_id = eh.company_id
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
    GROUP BY c.company_name, c.isin
    ORDER BY holding_count DESC
    LIMIT 10
""")
print("   Top 10 ICICI companies:")
for row in cur.fetchall():
    print(f"     {row[0][:50]:50} | {row[1]} | {row[2]} holdings")

cur.execute("""
    SELECT COUNT(DISTINCT c.company_id)
    FROM companies c
    JOIN equity_holdings eh ON c.company_id = eh.company_id
    JOIN scheme_snapshots ss ON eh.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    WHERE a.amc_name = 'ICICI Prudential Mutual Fund'
    AND c.company_name = 'N/A'
""")
icici_na_count = cur.fetchone()[0]
print(f"\n   ICICI companies with 'N/A': {icici_na_count}")

# 3. Check if HDFC has actual company names in Excel
print("\n3. Checking HDFC Excel for company names...")
import pandas as pd
from pathlib import Path

hdfc_file = Path("data/output/merged excels/hdfc/2025/CONSOLIDATED_HDFC_2025_12.xlsx")
if hdfc_file.exists():
    xls = pd.ExcelFile(hdfc_file, engine='openpyxl')
    sheet_name = xls.sheet_names[0]
    
    # Read first sheet
    df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=30)
    
    # Find header
    header_idx = -1
    for idx, row in df_raw.iterrows():
        row_str = ' '.join([str(v) for v in row.values if pd.notna(v)]).upper()
        if 'ISIN' in row_str and ('NAME' in row_str or 'SECURITY' in row_str):
            header_idx = idx
            break
    
    if header_idx != -1:
        print(f"   HDFC Excel header row: {header_idx}")
        print(f"   Header columns: {df_raw.iloc[header_idx].values}")
        
        # Read with header
        df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=header_idx)
        print(f"\n   HDFC Excel columns: {df.columns.tolist()}")
        
        # Check if there's a company name column
        name_cols = [col for col in df.columns if 'NAME' in str(col).upper() or 'SECURITY' in str(col).upper()]
        print(f"   Name columns: {name_cols}")
        
        if name_cols:
            print(f"\n   Sample company names from HDFC Excel:")
            for i, val in enumerate(df[name_cols[0]].head(10)):
                if pd.notna(val):
                    print(f"     {i}: {val}")
else:
    print("   HDFC Excel file not found")

# 4. Check ISIN master update timestamps
print("\n4. ISIN Master Update Timestamps:")
cur.execute("""
    SELECT 
        DATE(updated_at) as update_date,
        COUNT(*) as count
    FROM isin_master
    WHERE canonical_name != 'N/A'
    GROUP BY DATE(updated_at)
    ORDER BY update_date DESC
    LIMIT 5
""")
print("   Recent updates (non-N/A entries):")
for row in cur.fetchall():
    print(f"     {row[0]}: {row[1]} ISINs updated")

cur.execute("""
    SELECT 
        DATE(updated_at) as update_date,
        COUNT(*) as count
    FROM isin_master
    WHERE canonical_name = 'N/A'
    GROUP BY DATE(updated_at)
    ORDER BY update_date DESC
    LIMIT 5
""")
print("\n   Recent updates (N/A entries):")
for row in cur.fetchall():
    print(f"     {row[0]}: {row[1]} ISINs updated")

cur.close()
conn.close()

print("\n" + "="*80)
