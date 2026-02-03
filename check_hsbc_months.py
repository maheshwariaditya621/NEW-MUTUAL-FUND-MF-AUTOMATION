"""Quick check: What's the earliest month available on HSBC website?"""
import requests
import re
from datetime import datetime
from collections import defaultdict

url = "https://www.assetmanagement.hsbc.co.in/en/mutual-funds/investor-resources/information-library"
headers = {"User-Agent": "Mozilla/5.0"}

print("Fetching HTML...")
try:
    response = requests.get(url, headers=headers, timeout=30)
    html = response.text
    
    all_xlsx = re.findall(r'href="([^"]+\.xlsx)"', html, re.IGNORECASE)
    portfolio_links = [link for link in all_xlsx if "/mutual-funds/portfolios/" in link.lower()]
    
    print(f"Found {len(portfolio_links)} portfolio links\n")
    
    # Parse all dates
    PATTERN = re.compile(r"-(\d{2})-([a-z]+)-(\d{4})\.xlsx$", re.IGNORECASE)
    
    month_counts = defaultdict(int)
    
    for link in portfolio_links:
        match = PATTERN.search(link)
        if match:
            try:
                day, mon, year = match.groups()
                date_str = f"{day}-{mon}-{year}"
                date_obj = datetime.strptime(date_str, "%d-%b-%Y")
                month_key = date_obj.strftime("%Y-%m")
                month_counts[month_key] += 1
            except:
                pass
    
    # Show all months sorted
    print("Available months (file count):")
    print("="*50)
    for month_key in sorted(month_counts.keys()):
        print(f"{month_key}: {month_counts[month_key]} files")
    
    print("\n" + "="*50)
    earliest = min(month_counts.keys())
    latest = max(month_counts.keys())
    print(f"Earliest: {earliest}")
    print(f"Latest: {latest}")
    
except Exception as e:
    print(f"Error: {e}")
