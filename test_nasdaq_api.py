import requests
import json

url = "https://api.nasdaq.com/api/ipo/calendar?date=2024-05"
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}
response = requests.get(url, headers=headers)
if response.status_code == 200:
    data = response.json()
    priced = data.get('data', {}).get('priced', {})
    rows = priced.get('rows', [])
    if rows:
        print("First row keys:", rows[0].keys())
        print("First row data:", rows[0])
    else:
        print("No priced IPOs found for 2024-05")
else:
    print(f"Error {response.status_code}")
