import os
import requests
import json
res = requests.get('http://127.0.0.1:8000/api/v1/schemes/portfolio?q=19263')
try:
    data = res.json()
    print("KEYS:", data.keys())
    print("MONTHLY AUM:", data.get('monthly_aum'))
    if 'holdings' in data and len(data['holdings']) > 0:
        print("FIRST HOLDING:", json.dumps(data['holdings'][0], indent=2))
except Exception as e:
    print("Error:", e, res.text)
