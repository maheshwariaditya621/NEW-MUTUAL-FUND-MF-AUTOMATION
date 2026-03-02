import requests
import json

def test_ownership_api():
    base_url = "http://127.0.0.1:8000/api/v1"
    # Testing with a known stock (RELIANCE or similar)
    # I'll search for 'RELIANCE' first to get an ISIN
    print("Searching for RELIANCE...")
    res = requests.get(f"{base_url}/stocks/search?q=RELIANCE")
    if res.status_code != 200:
        print(f"Search failed: {res.status_code}")
        return
    
    data = res.json()
    if not data['results']:
        print("No results for RELIANCE")
        return
    
    isin = data['results'][0]['isin']
    print(f"Found ISIN: {isin}")
    
    print(f"Fetching holdings for {isin}...")
    res = requests.get(f"{base_url}/stocks/{isin}/holdings?months=4")
    if res.status_code != 200:
        print(f"Holdings fetch failed: {res.status_code}")
        return
    
    data = res.json()
    print(f"Summary Ownership %: {data.get('ownership_percent')}%")
    
    trend = data.get('monthly_trend', [])
    for m in trend:
        print(f"  Overall {m['month']}: {m['ownership_percent']}%")
    
    holdings = data.get('holdings', [])
    if holdings:
        first_scheme = holdings[0]
        print(f"\nFirst Scheme: {first_scheme['scheme_name']}")
        for h in first_scheme['history'][:2]:
            print(f"  {h['month']}: {h['ownership_percent']}% (Shares: {h['num_shares']})")

    if data.get('ownership_percent') is not None:
        print("\nVerification SUCCESS: ownership_percent is present.")
    else:
        print("\nVerification FAILED: ownership_percent is None. Check if shares_outstanding is missing for this company.")

if __name__ == "__main__":
    test_ownership_api()
