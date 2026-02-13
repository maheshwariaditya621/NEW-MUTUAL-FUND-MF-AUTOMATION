"""
Test the regex pattern directly
"""

import re

# The pattern from isin.py
EQUITY_ISIN_PATTERN = re.compile(r'^INE[A-Z0-9]{6}10[A-Z0-9]{1}$')

test_isin = "INE040A01034"

print(f"Testing ISIN: {test_isin}")
print(f"Pattern: ^INE[A-Z0-9]{{6}}10[A-Z0-9]{{1}}$")
print()

# Manual breakdown
print("Manual pattern matching:")
print(f"  Starts with 'INE': {test_isin.startswith('INE')}")
print(f"  Length is 12: {len(test_isin) == 12}")
print(f"  Chars 3-8 (6 chars): '{test_isin[3:9]}' - all alphanumeric: {test_isin[3:9].isalnum()}")
print(f"  Chars 9-10: '{test_isin[8:10]}' - equals '10': {test_isin[8:10] == '10'}")
print(f"  Char 11: '{test_isin[10]}' - alphanumeric: {test_isin[10].isalnum()}")
print()

# Regex match
match = EQUITY_ISIN_PATTERN.match(test_isin)
print(f"Regex match result: {match}")
print()

# Try to understand why it's not matching
print("Debugging regex:")
print(f"  Pattern expects at position 9-10 (after INE + 6 chars): '10'")
print(f"  Actual ISIN breakdown:")
for i, char in enumerate(test_isin):
    print(f"    Position {i}: '{char}'")

print()
print("Wait... let me count the regex pattern:")
print("  ^INE = positions 0-2 (3 chars)")
print("  [A-Z0-9]{6} = positions 3-8 (6 chars)")
print("  10 = positions 9-10 (2 chars) - but wait, that's only 11 chars total!")
print("  [A-Z0-9]{1} = position 11 (1 char) - total 12 chars")
print()
print(f"So the pattern expects: INE + 6 chars + '10' + 1 char")
print(f"Let's check: {test_isin[0:3]} + {test_isin[3:9]} + {test_isin[9:11]} + {test_isin[11:12]}")
print(f"  = '{test_isin[0:3]}' + '{test_isin[3:9]}' + '{test_isin[9:11]}' + '{test_isin[11:12]}'")
print(f"  Position 9-10 is: '{test_isin[9:11]}' (should be '10' but it's '{test_isin[9:11]}')")
