import pandas as pd
import requests
import io

def get_ticker_data():
    print("Downloading ticker lists...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
    }
    
    tickers_dict = {}

    # NASDAQ listed
    url_nasdaq = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
    try:
        s = requests.get(url_nasdaq, headers=headers).content
        df_nasdaq = pd.read_csv(io.BytesIO(s), sep="|")
        print("Columns in Nasdaq file:", df_nasdaq.columns.tolist())
        for _, row in df_nasdaq.iterrows():
            sym = row['Symbol']
            name = row['Security Name']
            if isinstance(sym, str) and not sym.startswith("File Creation Time"):
                tickers_dict[sym] = str(name)
    except Exception as e:
        print(f"Error downloading NASDAQ list: {e}")

    # Other listed
    url_other = "http://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    try:
        s = requests.get(url_other, headers=headers).content
        df_other = pd.read_csv(io.BytesIO(s), sep="|")
        print("Columns in Other file:", df_other.columns.tolist())
        for _, row in df_other.iterrows():
            sym = row['ACT Symbol']
            name = row['Security Name']
            if isinstance(sym, str) and not sym.startswith("File Creation Time"):
                tickers_dict[sym] = str(name)
    except Exception as e:
        print(f"Error downloading Other listed list: {e}")

    return tickers_dict

def main():
    data = get_ticker_data()
    
    test_tickers = ['TSLA', 'TSLL', 'ETH', 'ETHA', 'SPY', 'SPXL']
    
    print("\n--- Check Results ---")
    for t in test_tickers:
        if t in data:
            print(f"Ticker: {t} | Name: {data[t]}")
        else:
            print(f"Ticker: {t} NOT FOUND")

if __name__ == "__main__":
    main()
