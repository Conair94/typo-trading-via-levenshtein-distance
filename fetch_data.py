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

def is_correlated_by_design(target, candidate, target_name, candidate_name):
    """
    Checks if the candidate is likely correlated by design to the target.
    Logic:
    1. If candidate name contains the target ticker.
    2. If both names contain specific asset keywords (e.g. BITCOIN, ETHER).
    3. If candidate is a fund/trust and tickers share a prefix.
    """
    
    # Normalize
    target = target.upper()
    candidate = candidate.upper()
    target_name = target_name.upper()
    candidate_name = candidate_name.upper()
    
    # 1. Ticker in Name Check
    if len(target) > 3:
        if target in candidate_name:
            return True
    else:
        # Use regex boundary \b for short tickers
        if re.search(r'\b' + re.escape(target) + r'\b', candidate_name):
            return True

    # 2. Shared Asset Class Check
    # If both names mention the same specific underlying asset, they are correlated.
    asset_keywords = ["BITCOIN", "ETHER", "ETHEREUM", "CRYPTO", "GOLD", "SILVER", "OIL", "VIX", "TREASURY", "BOND"]
    for asset in asset_keywords:
        if asset in target_name and asset in candidate_name:
            return True

    # 3. ETF/Trust Ticker Similarity Heuristic
    # If candidate is an ETF/Trust/Fund and shares ticker prefix with target
    fund_keywords = ["ETF", "TRUST", "FUND", "SHARES", "STRATEGY", "ETN", "NOTE", "COIN"]
    if any(k in candidate_name for k in fund_keywords):
        # If tickers share first 2 chars (e.g. ET..), assume relation.
        if target[:2] == candidate[:2]:
            return True

    return False

# QWERTY Keyboard Adjacency Map (Approximate)
# Includes horizontal, vertical, and diagonal neighbors
KEYBOARD_ADJACENCY = {
    'Q': 'WA', 'W': 'QESA', 'E': 'WRSD', 'R': 'ETDF', 'T': 'RYFG', 'Y': 'TUGH', 'U': 'YIHJ', 'I': 'UOJK', 'O': 'IPKL', 'P': 'OL',
    'A': 'QWSZ', 'S': 'WEADZX', 'D': 'ERSFXC', 'F': 'RTDGCV', 'G': 'TYFHVB', 'H': 'YUGJBN', 'J': 'UIHKNM', 'K': 'IOJLM', 'L': 'OPK',
    'Z': 'ASX', 'X': 'SDZC', 'C': 'DFXV', 'V': 'FGCB', 'B': 'GHVN', 'N': 'HJBM', 'M': 'JKN'
}

def check_keyboard_proximity(ticker1, ticker2):
    """
    Checks if ticker2 is a single-character substitution of ticker1
    where the swapped character is a physical neighbor on a QWERTY keyboard.
    Example: 'TSLA' vs 'TALA' (S and A are neighbors).
    """
    # Proximity only applies to substitution (same length)
    if len(ticker1) != len(ticker2):
        return False
    
    # Find mismatches
    diffs = [(c1, c2) for c1, c2 in zip(ticker1, ticker2) if c1 != c2]
    
    # Must be exactly one substitution
    if len(diffs) != 1:
        return False
        
    char1, char2 = diffs[0]
    
    # Check adjacency
    # We check both directions just in case, though map should be symmetric-ish
    neighbors1 = KEYBOARD_ADJACENCY.get(char1, "")
    neighbors2 = KEYBOARD_ADJACENCY.get(char2, "")
    
    return char2 in neighbors1 or char1 in neighbors2

def validate_active_tickers(tickers):
    """
    Checks if tickers have recent trading data (non-zero volume in last 5 days).
    Returns a set of valid tickers.
    """
    if not tickers:
        return set()

    print(f"\nValidating {len(tickers)} candidates for data availability...")
    valid_tickers = set()
    
    # Batch process
    batch_size = 500
    chunks = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    
    for i, chunk in enumerate(chunks):
        try:
            # Download last 5 days
            # auto_adjust=True helps get cleaner data
            df = yf.download(chunk, period="5d", progress=False, auto_adjust=True)
            
            if df.empty:
                continue

            # Extract Volume
            # yfinance structure varies by version and number of tickers
            # Ideally we look for 'Volume'
            vol = None
            
            if 'Volume' in df.columns:
                vol = df['Volume']
            elif isinstance(df.columns, pd.MultiIndex) and 'Volume' in df.columns.get_level_values(0):
                 # Try to access top level
                 vol = df['Volume']
            
            if vol is None:
                continue
                
            # Check volume sums
            if isinstance(vol, pd.Series):
                # Single ticker case
                if vol.sum() > 0:
                    valid_tickers.add(chunk[0])
            else:
                # DataFrame case (Tickers as columns)
                sums = vol.sum()
                # Filter tickers with volume > 0
                active = sums[sums > 0].index.tolist()
                valid_tickers.update(active)
                
        except Exception as e:
            print(f"Warning: Validation batch {i+1} failed: {e}")
            
    print(f"  {len(valid_tickers)} out of {len(tickers)} candidates have active trading data.")
    return valid_tickers

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
                
                if is_correlated_by_design(target, candidate, target_name, candidate_name):
                    # print(f"Skipping correlated pair: {target} vs {candidate} ({candidate_name})")
                    continue
                
                is_proximate = check_keyboard_proximity(target, candidate)

                results.append({
                    'Target_Ticker': target,
                    'Target_Name': target_name,
                    'Candidate_Ticker': candidate,
                    'Candidate_Name': candidate_name,
                    'Distance': dist,
                    'Keyboard_Proximate': is_proximate
                })
    
    # --- New Validation Step ---
    if not results:
        return pd.DataFrame(results)

    print("Filtering candidates for data availability...")
    # Extract unique candidates
    initial_df = pd.DataFrame(results)
    candidates_to_check = initial_df['Candidate_Ticker'].unique().tolist()
    
    # Validate
    valid_candidates = validate_active_tickers(candidates_to_check)
    
    # Filter results
    final_df = initial_df[initial_df['Candidate_Ticker'].isin(valid_candidates)]
    
    print(f"Removed {len(initial_df) - len(final_df)} candidates due to lack of data.")
    
    return final_df

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