import pandas as pd
import requests
import io
from rapidfuzz import distance
import yfinance as yf
import time
import os

def get_ticker_data():
    """
    Downloads ticker lists from NASDAQ Trader.
    Returns a list of tickers.
    """
    print("Downloading ticker lists...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
    }
    
    # NASDAQ listed
    url_nasdaq = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
    try:
        s = requests.get(url_nasdaq, headers=headers).content
        df_nasdaq = pd.read_csv(io.BytesIO(s), sep="|")
        # Filter out test stocks if needed, keeping only 'Symbol'
        nasdaq_tickers = df_nasdaq['Symbol'].tolist()
        # Remove the file creation time entry at the bottom if present
        nasdaq_tickers = [x for x in nasdaq_tickers if isinstance(x, str) and not x.startswith("File Creation Time")]
    except Exception as e:
        print(f"Error downloading NASDAQ list: {e}")
        nasdaq_tickers = []

    # Other listed (NYSE, NYSE American, etc.)
    url_other = "http://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    try:
        s = requests.get(url_other, headers=headers).content
        df_other = pd.read_csv(io.BytesIO(s), sep="|")
        other_tickers = df_other['ACT Symbol'].tolist()
        other_tickers = [x for x in other_tickers if isinstance(x, str) and not x.startswith("File Creation Time")]
    except Exception as e:
        print(f"Error downloading Other listed list: {e}")
        other_tickers = []

    all_tickers = list(set(nasdaq_tickers + other_tickers))
    print(f"Found {len(all_tickers)} total tickers.")
    return all_tickers

def get_top_volume_tickers(tickers, limit=100):
    """
    Fetches volume data for tickers and returns the top 'limit' by volume.
    """
    print("Fetching volume data (this may take a moment)...")
    
    # Batch processing to avoid URL length limits or timeouts
    # yfinance is better with batches, but 8000 might be too many for one call
    batch_size = 1000
    ticker_data = []
    
    # We only need the last few days to get an average volume
    # Or even just the most recent day. Let's try to get 'Average Volume' from info if possible,
    # but 'info' is slow because it requires 1 request per ticker.
    # Faster: use yf.download for last 5 days and take average volume.
    
    chunks = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    
    volume_series_list = []

    for i, chunk in enumerate(chunks):
        print(f"Processing batch {i+1}/{len(chunks)}...")
        try:
            # Download last 5 days of data
            data = yf.download(chunk, period="5d", progress=False)['Volume']
            
            # If data is a Series (only one ticker valid), convert to DataFrame
            if isinstance(data, pd.Series):
                data = data.to_frame()
                
            # Calculate mean volume for the period
            mean_vol = data.mean()
            volume_series_list.append(mean_vol)
        except Exception as e:
            print(f"Error in batch {i+1}: {e}")
            
    if not volume_series_list:
        return []

    full_volume_series = pd.concat(volume_series_list)
    
    # Sort by volume descending
    top_tickers = full_volume_series.sort_values(ascending=False).head(limit)
    
    print(f"\nTop {limit} stocks by volume:")
    print(top_tickers.head())
    
    return top_tickers.index.tolist()

def calculate_distances(target_tickers, all_tickers, threshold=1):
    """
    Calculates Damerau-Levenshtein distance between target tickers and all tickers.
    Returns a DataFrame of matches within threshold.
    """
    results = []
    
    print(f"\nCalculating distances for {len(target_tickers)} targets against {len(all_tickers)} candidates...")
    
    for target in target_tickers:
        for candidate in all_tickers:
            if target == candidate:
                continue
            
            # Calculate Damerau-Levenshtein distance
            dist = distance.DamerauLevenshtein.distance(target, candidate)
            
            if dist <= threshold:
                results.append({
                    'Target_Ticker': target,
                    'Candidate_Ticker': candidate,
                    'Distance': dist
                })
                
    return pd.DataFrame(results)

def main():
    # 1. Get all tickers
    all_tickers = get_ticker_data()
    if not all_tickers:
        print("No tickers found. Exiting.")
        return

    # 2. Identify top 100 by volume
    # For testing purposes, if yfinance fails or takes too long, 
    # one might hardcode a few known high volume stocks, but we'll try to fetch.
    top_100 = get_top_volume_tickers(all_tickers, limit=100)
    
    if not top_100:
        print("Could not identify top volume tickers.")
        return

    # 3. Calculate distances
    # We are looking for "low" distance. Distance of 1 is a single typo.
    # Distance of 2 might be interesting but significantly more noisy.
    df_results = calculate_distances(top_100, all_tickers, threshold=1)
    
    # 4. Save results
    output_file = "typo_candidates.csv"
    df_results.to_csv(output_file, index=False)
    print(f"\nFound {len(df_results)} pairs. Saved to {output_file}")
    
    # Also save the list of top 100 for reference
    pd.Series(top_100, name="Ticker").to_csv("top_100_volume.csv", index=False)
    print("Saved top 100 tickers to top_100_volume.csv")

if __name__ == "__main__":
    main()
