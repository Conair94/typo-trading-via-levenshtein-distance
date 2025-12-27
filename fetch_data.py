import pandas as pd
import requests
import io
from rapidfuzz import distance
import yfinance as yf
import re

def get_ticker_data():
    """
    Downloads ticker lists from NASDAQ Trader.
    Returns a dictionary of {Ticker: Security Name}.
    """
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
        # Keep Symbol and Security Name
        # Filter out test stocks and file creation time
        for _, row in df_nasdaq.iterrows():
            sym = row['Symbol']
            name = row['Security Name']
            if isinstance(sym, str) and not sym.startswith("File Creation Time"):
                tickers_dict[sym] = str(name)
    except Exception as e:
        print(f"Error downloading NASDAQ list: {e}")

    # Other listed (NYSE, NYSE American, etc.)
    url_other = "http://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    try:
        s = requests.get(url_other, headers=headers).content
        df_other = pd.read_csv(io.BytesIO(s), sep="|")
        for _, row in df_other.iterrows():
            sym = row['ACT Symbol']
            name = row['Security Name']
            if isinstance(sym, str) and not sym.startswith("File Creation Time"):
                tickers_dict[sym] = str(name)
    except Exception as e:
        print(f"Error downloading Other listed list: {e}")

    print(f"Found {len(tickers_dict)} total tickers.")
    return tickers_dict

def get_top_volume_tickers(tickers, limit=100):
    """
    Fetches volume data for tickers and returns the top 'limit' by volume.
    Takes a list of tickers.
    """
    print("Fetching volume data (this may take a moment)...")
    
    batch_size = 1000
    
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

def is_correlated_by_design(target, candidate, candidate_name):
    """
    Checks if the candidate is likely correlated by design to the target.
    Logic:
    1. If candidate name contains the target ticker.
       - If target len > 3, substring match is sufficient (e.g. 'TSLA' in 'TSLA Bull').
       - If target len <= 3, require whole word match to avoid false positives (e.g. 'M' in 'MGM').
    2. Specific exclusion for Crypto/Currency trust confusion (e.g. ETH vs ETHA).
    """
    
    # Normalize
    cand_name_upper = candidate_name.upper()
    target_upper = target.upper()
    
    # 1. Ticker in Name Check
    if len(target_upper) > 3:
        if target_upper in cand_name_upper:
            return True
    else:
        # Use regex boundary \b for short tickers
        if re.search(r'\b' + re.escape(target_upper) + r'\b', cand_name_upper):
            return True

    # 2. Crypto/Currency Heuristics
    # If one is a trust/ETF for the other's underlying asset class (often implied by similar tickers starting with same letters)
    # Example: ETH (stock) vs ETHA (Ethereum Trust). 
    # Heuristic: If candidate name contains "BITCOIN", "ETHER", "CRYPTO", "TRUST", "ETF" 
    # AND the tickers share the first 2-3 letters, it's likely a product relation, not a typo.
    
    suspect_keywords = ["BITCOIN", "ETHER", "ETHEREUM", "CRYPTO", "TRUST", "ETF", "FUND", "SHARES"]
    if any(k in cand_name_upper for k in suspect_keywords):
        # If tickers start with same 2 chars (e.g. ET..), assume relation if one is a Fund.
        if target_upper[:2] == candidate[:2]:
            return True

    return False

def calculate_distances(target_tickers, all_tickers_dict, threshold=1):
    """
    Calculates Damerau-Levenshtein distance between target tickers and all tickers.
    Returns a DataFrame of matches within threshold.
    """
    results = []
    all_tickers_list = list(all_tickers_dict.keys())
    
    print(f"\nCalculating distances for {len(target_tickers)} targets against {len(all_tickers_list)} candidates...")
    
    for target in target_tickers:
        for candidate in all_tickers_list:
            if target == candidate:
                continue
            
            # Calculate Damerau-Levenshtein distance
            dist = distance.DamerauLevenshtein.distance(target, candidate)
            
            if dist <= threshold:
                # Check for intentional correlation
                candidate_name = all_tickers_dict.get(candidate, "")
                target_name = all_tickers_dict.get(target, "")
                
                if is_correlated_by_design(target, candidate, candidate_name):
                    # print(f"Skipping correlated pair: {target} vs {candidate} ({candidate_name})")
                    continue
                
                results.append({
                    'Target_Ticker': target,
                    'Target_Name': target_name,
                    'Candidate_Ticker': candidate,
                    'Candidate_Name': candidate_name,
                    'Distance': dist
                })
                
    return pd.DataFrame(results)

import os
from datetime import datetime

def main():
    # 1. Get all tickers (dict with names)
    all_tickers_dict = get_ticker_data()
    if not all_tickers_dict:
        print("No tickers found. Exiting.")
        return

    all_tickers_list = list(all_tickers_dict.keys())

    # 2. Identify top 100 by volume
    top_100 = get_top_volume_tickers(all_tickers_list, limit=100)
    
    if not top_100:
        print("Could not identify top volume tickers.")
        return

    # 3. Calculate distances with filtering
    df_results = calculate_distances(top_100, all_tickers_dict, threshold=1)
    
    # 4. Save results to timestamped folder
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = os.path.join("data", timestamp)
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, "typo_candidates.csv")
    df_results.to_csv(output_file, index=False)
    print(f"\nFound {len(df_results)} pairs. Saved to {output_file}")
    
    # Also save the list of top 100 for reference
    top_100_file = os.path.join(output_dir, "top_100_volume.csv")
    pd.Series(top_100, name="Ticker").to_csv(top_100_file, index=False)
    print(f"Saved top 100 tickers to {top_100_file}")
    print(f"All data for this run is in: {output_dir}")

if __name__ == "__main__":
    main()