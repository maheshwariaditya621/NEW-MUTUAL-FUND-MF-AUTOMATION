import requests
import json

def test_nifty_backend():
    url = "https://www.niftyindices.com/Backends/Indices_Historical_Report/TotalReturnIndex_HistoricalBySymbol"
    
    payload = {
        "name": "NIFTY 50",
        "startDate": "01-Jan-2024",
        "endDate": "31-Jan-2024"
    }
    
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.niftyindices.com",
        "Referer": "https://www.niftyindices.com/reports/historical-data"
    }
    
    try:
        print(f"POST {url}")
        print(f"Payload: {payload}")
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text[:500]}...")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Data records: {len(data)}")
            if len(data) > 0:
                print(f"First record: {data[0]}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_nifty_backend()
