import yfinance as yf
from datetime import datetime
import pandas as pd

ticker = "AAPL"
date_str = "2023-12-27"
start_date = datetime.strptime(date_str, "%Y-%m-%d")
fetch_start = "2023-12-20"
fetch_end = "2024-01-05"

df = yf.download(ticker, start=fetch_start, end=fetch_end, progress=False)
print("Index type:", type(df.index))
print("Index example:", df.index[0])
df.index = df.index.normalize()
print("Normalized index example:", df.index[0])
print("Start date:", start_date)
print("Is start_date in index?", start_date in df.index)

# If it fails, try with timezone awareness
if start_date not in df.index:
    print("Trying with tz-naive check...")
    df.index = df.index.tz_localize(None)
    print("Is start_date in index after tz_localize(None)?", start_date in df.index)
