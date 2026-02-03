"""
Debug HSBC month filtering for Jan-Mar 2024
"""

import requests
import re
from datetime import datetime

# Fetch HTML
url = "https://www.assetmanagement.hsbc.co.in/en/mutual-funds/investor-resources/information-library"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

print("Fetching HTML...")
response = requests.get(url, headers=headers, timeout=60)
html = response.text

# Extract portfolio links
all_xlsx = re.findall(r'href="([^"]+\.xlsx)"', html, re.IGNORECASE)
portfolio_links = [link for link in all_xlsx if "/mutual-funds/portfolios/" in link.lower()]

print(f"Found {len(portfolio_links)} portfolio links\n")

# Parse dates
FILENAME_PATTERN = re.compile(
    r"/(?P<slug>hsbc-[a-z0-9\-]+)-(?P<day>\d{2})-(?P<mon>[a-z]+)-(?P<year>\d{4})\.xlsx$",
    re.IGNORECASE
)

month_map = {
    "jan": "Jan", "january": "Jan",
    "feb": "Feb", "february": "Feb",
    "mar": "Mar", "march": "Mar",
    "apr": "Apr", "april": "Apr",
    "may": "May",
    "jun": "Jun", "june": "Jun",
    "jul": "Jul", "july": "Jul",
    "aug": "Aug", "august": "Aug",
    "sep": "Sep", "september": "Sep",
    "oct": "Oct", "october": "Oct",
    "nov": "Nov", "november": "Nov",
    "dec": "Dec", "december": "Dec",
}

# Check Jan-Mar 2024
target_months = [(2024, 1), (2024, 2), (2024, 3)]

for year, month in target_months:
    print(f"\n{'='*70}")
    print(f"Checking {year}-{month:02d}")
    print('='*70)
    
    dates_found = []
    
    for url in portfolio_links:
        match = FILENAME_PATTERN.search(url)
        if not match:
            continue
        
        try:
            raw_mon = match.group("mon").lower()
            if raw_mon not in month_map:
                continue
            
            date_str = f"{match.group('day')}-{month_map[raw_mon]}-{match.group('year')}"
            date_obj = datetime.strptime(date_str, "%d-%b-%Y")
            
            # Check if this could be for our target month
            # Files for Jan 2024 might be dated in early Feb 2024
            if date_obj.year == year and date_obj.month in [month, month + 1]:
                dates_found.append((date_obj, url.split('/')[-1]))
        except:
            continue
    
    if dates_found:
        dates_found.sort()
        print(f"Found {len(dates_found)} files with dates in {year}-{month:02d} or {year}-{month+1:02d}:")
        for date_obj, filename in dates_found[:5]:  # Show first 5
            print(f"  {date_obj.strftime('%Y-%m-%d')} - {filename}")
        if len(dates_found) > 5:
            print(f"  ... and {len(dates_found) - 5} more")
    else:
        print(f"❌ No files found for {year}-{month:02d}")

print("\n" + "="*70)
print("ANALYSIS")
print("="*70)
print("If files for Jan 2024 have dates in Feb 2024, we need to:")
print("1. Use a grace window (like we initially tried)")
print("2. Or check the actual publication pattern")
