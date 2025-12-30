import pandas as pd
import requests
import yfinance as yf
from rapidfuzz import distance
import datetime
from datetime import datetime as dt, timedelta
import time
import os
import re
import json
import sys

# Import helper functions from existing script
# We assume fetch_data.py is in the same directory
try:
    from fetch_data import get_ticker_data, check_keyboard_proximity, is_correlated_by_design
except ImportError:
    print("Error: Could not import from fetch_data.py. Make sure it is in the same directory.")
    sys.exit(1)

def get_ipos_range(start_year, end_year):
    """
    Fetches priced IPOs from NASDAQ API for a range of years.
    Returns a DataFrame of IPOs.
    """
    all_ipos = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/"
    }
    
    print(f"Fetching IPOs from {start_year} to {end_year}...")
    
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            date_str = f"{year}-{month:02d}"
            url = f"https://api.nasdaq.com/api/ipo/calendar?date={date_str}"
            
            try:
                # Rate limit politeness
                time.sleep(0.2)
                
                resp = requests.get(url, headers=headers)
                data = resp.json()
                
                if data.get('data') and data['data'].get('priced'):
                    rows = data['data']['priced']['rows']
                    if rows:
                        for row in rows:
                            # Clean up data
                            ticker = row.get('proposedTickerSymbol')
                            name = row.get('companyName')
                            date_priced = row.get('pricedDate')
                            
                            if ticker and date_priced:
                                all_ipos.append({
                                    'IPO_Ticker': ticker,
                                    'IPO_Name': name,
                                    'IPO_Date': date_priced
                                })
            except Exception as e:
                print(f"Error fetching {date_str}: {e}")
                
            # Progress indicator
            print(f"Processed {date_str}", end='\r')
            
    print(f"\nFound {len(all_ipos)} total priced IPOs.")
    return pd.DataFrame(all_ipos)

def find_typos_for_ipo(ipo_ticker, ipo_name, all_tickers_dict, threshold=1):
    """
    Finds typo candidates for a single IPO ticker.
    Returns a list of dictionaries.
    """
    candidates = []
    ipo_ticker = str(ipo_ticker).upper()
    ipo_name = str(ipo_name)
    
    # Heuristic: Skip very short tickers (1-2 chars) to avoid noise?
    # Or keep them but expect many matches.
    if len(ipo_ticker) < 2:
        return []

    for cand_ticker, cand_name in all_tickers_dict.items():
        cand_ticker = str(cand_ticker).upper()
        
        if cand_ticker == ipo_ticker:
            continue
            
        # Optimization: Length check
        if abs(len(cand_ticker) - len(ipo_ticker)) > threshold:
            continue

        dist = distance.DamerauLevenshtein.distance(ipo_ticker, cand_ticker)
        
        if dist <= threshold:
            # Check for intentional correlation (from fetch_data.py)
            if is_correlated_by_design(ipo_ticker, cand_ticker, ipo_name, cand_name):
                continue
            
            is_proximate = check_keyboard_proximity(ipo_ticker, cand_ticker)
            
            candidates.append({
                'IPO_Ticker': ipo_ticker,
                'IPO_Name': ipo_name,
                'Typo_Ticker': cand_ticker,
                'Typo_Name': cand_name,
                'Distance': dist,
                'Keyboard_Proximate': is_proximate
            })
            
    return candidates

def analyze_market_reaction(ipo_ticker, typo_ticker, ipo_date_str):
    """
    Analyzes market data for the Typo Ticker on the IPO Date.
    """
    try:
        ipo_dt = dt.strptime(ipo_date_str, "%m/%d/%Y")
    except ValueError:
        try:
             ipo_dt = dt.strptime(ipo_date_str, "%Y-%m-%d")
        except:
            return None

    # Handle weekend IPOs (shift to next Monday)
    if ipo_dt.weekday() >= 5: # 5=Sat, 6=Sun
        ipo_dt += timedelta(days=(7 - ipo_dt.weekday()))
    
    start_date = ipo_dt
    end_date = ipo_dt + timedelta(days=1) # Fetch one day
    
    # Determine granularity
    now = dt.now()
    days_diff = (now - start_date).days
    
    # We primarily want to see if the Typo Ticker spiked.
    # We fetch the Typo Ticker data.
    # Note: We assume the IPO ticker might not have historical data *before* the IPO date,
    # but the Typo ticker MUST have data to be traded by mistake.
    
    stats = {
        'Date': start_date.strftime("%Y-%m-%d"),
        'Data_Source': 'Daily'
    }

    try:
        # Fetch Daily Data for Typo
        # We need previous close to calculate gap
        fetch_start = start_date - timedelta(days=5)
        df = yf.download(typo_ticker, start=fetch_start, end=end_date, progress=False, auto_adjust=True)
        
        if df.empty:
            return None
            
        # Get the specific IPO day row
        # Normalize index to midnight for matching
        df.index = df.index.normalize()
        
        if start_date not in df.index:
            # Maybe it wasn't trading?
            return None
            
        day_data = df.loc[start_date]
        
        # Calculate Metrics
        # 1. Volume Spike: Compare to previous 5 days average
        prev_data = df[df.index < start_date]
        if not prev_data.empty:
            avg_vol = prev_data['Volume'].tail(5).mean()
            vol_spike = day_data['Volume'] / avg_vol if avg_vol > 0 else 1.0
        else:
            vol_spike = 1.0 # No history
            
        # 2. Price Spike (High vs Open/PrevClose)
        # Did it gap up?
        if not prev_data.empty:
            prev_close = prev_data['Close'].iloc[-1]
            gap_pct = (day_data['Open'] - prev_close) / prev_close
        else:
            prev_close = day_data['Open']
            gap_pct = 0.0
            
        # Intraday Pop: (High - Open) / Open
        intraday_high_pct = (day_data['High'] - day_data['Open']) / day_data['Open']
        
        # Reversion: (Close - High) / High (Should be negative if it popped and dropped)
        reversion_pct = (day_data['Close'] - day_data['High']) / day_data['High']
        
        # Total Day Return
        day_return = (day_data['Close'] - prev_close) / prev_close
        
        stats.update({
            'Typo_Open': float(day_data['Open']),
            'Typo_High': float(day_data['High']),
            'Typo_Close': float(day_data['Close']),
            'Typo_Volume': int(day_data['Volume']),
            'Avg_Volume_5d': float(avg_vol) if not prev_data.empty else 0.0,
            'Volume_Spike_Ratio': float(vol_spike),
            'Gap_Up_Pct': float(gap_pct),
            'Intraday_High_Pct': float(intraday_high_pct),
            'Reversion_From_High_Pct': float(reversion_pct),
            'Day_Return': float(day_return)
        })
        
        return stats
        
    except Exception as e:
        # print(f"Error analyzing {typo_ticker} on {start_date}: {e}")
        return None

def main():
    # 1. Fetch Universe of Tickers (Potential Typos)
    # We use current universe as proxy.
    print("--- Step 1: Loading Ticker Universe ---")
    all_tickers = get_ticker_data()
    if not all_tickers:
        print("Failed to load tickers.")
        return

    # 2. Fetch IPOs (Last 2 Years)
    print("\n--- Step 2: Fetching IPO Data ---")
    current_year = dt.now().year
    
    # Handle CLI arguments for years
    if len(sys.argv) > 2:
        try:
            start_year = int(sys.argv[1])
            end_year = int(sys.argv[2])
        except ValueError:
            print("Invalid years provided. Using default 2-year range.")
            start_year = current_year - 1
            end_year = current_year
    else:
        start_year = current_year - 1
        end_year = current_year
        
    df_ipos = get_ipos_range(start_year, end_year)
    
    if df_ipos.empty:
        print("No IPOs found.")
        return
        
    # Filter for valid tickers (some might be empty)
    df_ipos = df_ipos[df_ipos['IPO_Ticker'].notna() & (df_ipos['IPO_Ticker'] != '')]

    # 3. Identify Typo Pairs
    print(f"\n--- Step 3: Identify Keyboard-Proximate Typo Pairs for {len(df_ipos)} IPOs ---")
    typo_pairs = []
    
    # Process in batches or show progress
    total = len(df_ipos)
    for i, row in df_ipos.iterrows():
        if i % 50 == 0:
            print(f"Scanning IPO {i}/{total}...", end='\r')
            
        candidates = find_typos_for_ipo(row['IPO_Ticker'], row['IPO_Name'], all_tickers)
        for cand in candidates:
            # ONLY keep keyboard proximate pairs as requested
            if cand.get('Keyboard_Proximate'):
                cand['IPO_Date'] = row['IPO_Date']
                typo_pairs.append(cand)
            
    print(f"\nFound {len(typo_pairs)} keyboard-proximate typo pairs.")
    
    if not typo_pairs:
        return

    # 4. Analyze Market Reaction
    print("\n--- Step 4: Analyzing Market Reaction ---")
    
    timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = os.path.join("data", f"ipo_study_{timestamp}")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "ipo_typo_results.csv")
    
    # Initialize CSV with headers
    pd.DataFrame(columns=[
        'IPO_Ticker', 'IPO_Name', 'Typo_Ticker', 'Typo_Name', 'Distance', 'Keyboard_Proximate', 'IPO_Date',
        'Date', 'Data_Source', 'Typo_Open', 'Typo_High', 'Typo_Close', 'Typo_Volume', 'Avg_Volume_5d',
        'Volume_Spike_Ratio', 'Gap_Up_Pct', 'Intraday_High_Pct', 'Reversion_From_High_Pct', 'Day_Return'
    ]).to_csv(output_file, index=False)
    
    print(f"Results will be incrementally saved to {output_file}")

    results = []
    
    for i, pair in enumerate(typo_pairs):
        if i % 10 == 0:
            print(f"Analyzing pair {i+1}/{len(typo_pairs)}...", end='\r')
            
        stats = analyze_market_reaction(pair['IPO_Ticker'], pair['Typo_Ticker'], pair['IPO_Date'])
        
        if stats:
            # Merge stats into pair dict
            pair.update(stats)
            results.append(pair)
            
            # Save batch every 10 results
            if len(results) >= 10:
                pd.DataFrame(results).to_csv(output_file, mode='a', header=False, index=False)
                results = [] # Clear buffer

    # Save remaining results
    if results:
        pd.DataFrame(results).to_csv(output_file, mode='a', header=False, index=False)
    
    print(f"\nAnalysis complete. Results saved to {output_file}")
    
    # Generate Summary (Load full file)
    if os.path.exists(output_file):
        df_results = pd.read_csv(output_file)
        if not df_results.empty:
            # Filter for significant events (e.g. Volume Spike > 2x OR Price Spike > 5%)
            significant = df_results[
                (df_results['Volume_Spike_Ratio'] > 2.0) | 
                (df_results['Intraday_High_Pct'] > 0.05)
            ].sort_values(by='Volume_Spike_Ratio', ascending=False)
            
            print("\n--- Top Significant Events ---")
            print(significant[['IPO_Date', 'IPO_Ticker', 'Typo_Ticker', 'Volume_Spike_Ratio', 'Intraday_High_Pct', 'Reversion_From_High_Pct']].head(10))
            
            sig_file = os.path.join(output_dir, "significant_ipo_events.csv")
            significant.to_csv(sig_file, index=False)
            print(f"Significant events saved to {sig_file}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        try:
            start_year_arg = int(sys.argv[1])
            end_year_arg = int(sys.argv[2])
            print(f"Running for custom range: {start_year_arg}-{end_year_arg}")
            
            # Monkey-patching main's year logic implies refactoring, but for minimal change:
            # We will just re-implement the year selection logic inside main or pass it.
            # Ideally, main() should accept args.
            # Let's just update the hardcoded logic in the file to read args if present.
            pass 
        except:
            print("Usage: python analyze_ipo_typos.py [start_year] [end_year]")
            sys.exit(1)
            
    main()
