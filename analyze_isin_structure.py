"""
Analyze actual ISIN structure from real data
"""

# Real ISINs from Axis report
sample_isins = [
    "INE040A01034",  # HDFC Bank - Equity
    "INE002A01018",  # Reliance - Equity  
    "INE090A01021",  # ICICI Bank - Equity
    "INE118H01025",  # BSE Limited - Equity
    "INE158A01026",  # Hero MotoCorp - Equity
]

print("ISIN Structure Analysis:")
print("=" * 80)
print()

for isin in sample_isins:
    print(f"ISIN: {isin}")
    print(f"  Char breakdown: {'-'.join(isin)}")
    print(f"  [0:3]  = {isin[0:3]:12s} (Country + NSDL)")
    print(f"  [3:9]  = {isin[3:9]:12s} (Company code - 6 chars)")
    print(f"  [9:11] = {isin[9:11]:12s} (Security type)")
    print(f"  [11:12]= {isin[11:12]:12s} (Check digit)")
    print()

print("\nObservation:")
print("All these are EQUITY securities, but security codes are: 03, 01, 02, 02, 02")
print("NOT '10' as the validator expects!")
print()
print("The validator regex is WRONG. It expects positions [9:11] to be '10',")
print("but real equity ISINs have various codes like '01', '02', '03', etc.")
