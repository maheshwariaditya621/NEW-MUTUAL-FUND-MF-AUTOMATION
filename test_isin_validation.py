"""
Test ISIN validation logic
"""

import re

# From isin.py
EQUITY_ISIN_PATTERN = re.compile(r'^INE[A-Z0-9]{6}10[A-Z0-9]{1}$')

test_isins = [
    "INE040A01034",  # HDFC Bank
    "INE002A01018",  # Reliance
    "INE090A01021",  # ICICI Bank
]

print("Testing ISIN validation:")
print("=" * 60)

for isin in test_isins:
    match = EQUITY_ISIN_PATTERN.match(isin)
    print(f"ISIN: {isin}")
    print(f"  Length: {len(isin)}")
    print(f"  Positions 9-10: '{isin[9:11]}'")
    print(f"  Pattern match: {bool(match)}")
    
    # Manual breakdown
    print(f"  Breakdown:")
    print(f"    INE: {isin[0:3]}")
    print(f"    Company code (6 chars): {isin[3:9]}")
    print(f"    Security code (2 chars): {isin[9:11]}")
    print(f"    Check digit (1 char): {isin[11:12]}")
    print()
