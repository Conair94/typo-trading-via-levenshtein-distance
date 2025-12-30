import yfinance as yf
from datetime import datetime, timedelta

ticker = "HAL"
ipo_date_str = "2024-05-18"
start_date = datetime.strptime(ipo_date_str, "%Y-%m-%d")
fetch_start = start_date - timedelta(days=10)
end_date = start_date + timedelta(days=5)

print(f"Fetching {ticker} from {fetch_start} to {end_date}")
df = yf.download(ticker, start=fetch_start, end=end_date, progress=False, auto_adjust=True)

if df.empty:
    print("DataFrame is empty!")
else:
    print("DataFrame head:")
    print(df.head())
    print("Index type:", type(df.index))
    print("Is IPO date in index?", start_date in df.index)
    
    # Check normalized
    df.index = df.index.normalize()
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    print("Is normalized IPO date in index?", start_date in df.index)
    
    trading_days = df.index[df.index >= start_date]
    if not trading_days.empty:
        print("Next trading day:", trading_days[0])
