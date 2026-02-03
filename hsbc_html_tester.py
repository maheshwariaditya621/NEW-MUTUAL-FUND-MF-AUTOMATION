import requests
import re
from urllib.parse import urljoin
from datetime import datetime
from collections import defaultdict

BASE = "https://www.assetmanagement.hsbc.co.in"
URL = "https://www.assetmanagement.hsbc.co.in/en/mutual-funds/investor-resources/information-library"

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

html = requests.get(URL, headers=headers, timeout=60).text

all_links = re.findall(r'href="([^"]+\.xlsx)"', html)

portfolio_links = []
for link in all_links:
    if "/mutual-funds/portfolios/" in link.lower():
        portfolio_links.append(urljoin(BASE, link))

pattern = re.compile(
    r"/(?P<slug>hsbc-[a-z0-9\-]+)-(?P<day>\d{2})-(?P<mon>[a-z]{3})-(?P<year>\d{4})\.xlsx$",
    re.I
)

grouped = defaultdict(dict)  # fund_slug -> {date: url}

for url in portfolio_links:
    m = pattern.search(url)
    if not m:
        continue

    date_obj = datetime.strptime(
        f"{m.group('day')}-{m.group('mon').title()}-{m.group('year')}",
        "%d-%b-%Y"
    ).date()

    fund = m.group("slug").lower()
    grouped[fund][date_obj] = url  # auto-dedup by date

# convert to sorted lists
final_data = {}
for fund, date_map in grouped.items():
    final_data[fund] = [
        {"portfolio_date": d.isoformat(), "url": date_map[d]}
        for d in sorted(date_map)
    ]

print("Total funds:", len(final_data))

sample_fund = next(iter(final_data))
print("\nSample fund:", sample_fund)
print("Available months:", len(final_data[sample_fund]))
print("First 3 entries:")
for x in final_data[sample_fund][:3]:
    print(x)
