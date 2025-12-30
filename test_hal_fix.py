import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

ticker = "HAL"
ipo_date_str = "2024-05-18"
start_date = datetime.strptime(ipo_date_str, "%Y-%m-%d")
fetch_start = start_date - timedelta(days=10)
end_date = start_date + timedelta(days=5)

df = yf.download(ticker, start=fetch_start, end=end_date, progress=False, auto_adjust=True)
df.index = df.index.normalize()

trading_days = df.index[df.index >= start_date]
start_date = trading_days[0]
day_data = df.loc[start_date]

print("Day data:")
print(day_data)
print("day_data['Volume'] type:", type(day_data['Volume']))
print("day_data['Volume'] value:", day_data['Volume'])

# How to fix it?
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.get_level_values(0)
    print("Fixed columns:", df.columns)
    day_data = df.loc[start_date]
    print("New day_data['Volume']:", day_data['Volume'])
