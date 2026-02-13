"""
Verify ISIN indexing - 0-indexed vs 1-indexed
"""

isin = "INE040A01034"  # HDFC Bank

print(f"ISIN: {isin}")
print(f"Length: {len(isin)}")
print()

print("Character-by-character (0-indexed):")
for i, char in enumerate(isin):
    print(f"  Index {i:2d}: '{char}'")

print()
print("Checking different slice positions:")
print(f"  isin[8:10] = '{isin[8:10]}' (0-indexed positions 8-9)")
print(f"  isin[9:11] = '{isin[9:11]}' (0-indexed positions 9-10)")
print()

print("Based on documentation, equity ISINs should have '10' somewhere.")
print("Let me check if '10' appears in this ISIN:")
if '10' in isin:
    idx = isin.index('10')
    print(f"  Found '10' at index {idx}: isin[{idx}:{idx+2}] = '{isin[idx:idx+2]}'")
else:
    print("  '10' NOT found in this ISIN!")
