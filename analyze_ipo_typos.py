import pandas as pd
import yfinance as yf
from datetime import datetime as dt, timedelta
import os
import requests
import time
from rapidfuzz import distance
import sys

# Import components from fetch_data.py
try:
    from fetch_data import get_ticker_data, is_correlated_by_design, check_keyboard_proximity
except ImportError:
    print("Error: Could not import from fetch_data.py. Ensure dependencies are installed.")
    sys.exit(1)

def find_typos_for_ipo(target_ticker, target_name, all_tickers_dict, threshold=1):
    """
    Finds potential typos for a single ticker.
    Returns a list of candidate dictionaries.
    """
    results = []
    target_ticker = target_ticker.upper()
    target_name = target_name.upper()
    
    for candidate, candidate_name in all_tickers_dict.items():
        if target_ticker == candidate:
            continue
        
        # Calculate Damerau-Levenshtein distance
        dist = distance.DamerauLevenshtein.distance(target_ticker, candidate)
        
        if dist <= threshold:
            # Check for intentional correlation
            if is_correlated_by_design(target_ticker, candidate, target_name, candidate_name):
                continue
            
            is_proximate = check_keyboard_proximity(target_ticker, candidate)

            results.append({
                'IPO_Ticker': target_ticker,
                'IPO_Name': target_name,
                'Typo_Ticker': candidate,
                'Typo_Name': candidate_name,
                'Distance': dist,
                'Keyboard_Proximate': is_proximate
            })
    return results

def get_ipos_range(start_year, end_year):
    """Fetch IPOs for a range of years from NASDAQ API."""
    all_ipos = []
    
    # Months to fetch
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # Don't fetch future months beyond today
            if year == dt.now().year and month > dt.now().month:
                break
                
            month_str = f"{year}-{month:02d}"
            print(f"Fetching IPOs for {month_str}...", end='\r')
            
            url = f"https://api.nasdaq.com/api/ipo/calendar?date={month_str}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            }
            
            try:
                response = requests.get(url, headers=headers, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    # print(f"DEBUG: Response keys for {month_str}: {data.keys() if data else 'None'}")
                    if data and 'data' in data and data['data'] and 'priced' in data['data']:
                        priced = data['data']['priced']
                        if priced and 'rows' in priced and priced['rows']:
                            # print(f"DEBUG: First row for {month_str}: {priced['rows'][0]}")
                            for row in priced['rows']:
                                all_ipos.append({
                                    'IPO_Ticker': row.get('proposedTickerSymbol'),
                                    'IPO_Name': row.get('companyName'),
                                    'IPO_Date': row.get('pricedDate'),
                                    'IPO_Price': row.get('proposedSharePrice')
                                })
                time.sleep(0.3) # Avoid rate limits
            except Exception as e:
                print(f"\nError fetching {month_str}: {e}")
                
    print(f"\nFound {len(all_ipos)} total priced IPOs.")
    return pd.DataFrame(all_ipos)

def analyze_market_reaction(ipo_ticker, typo_ticker, ipo_date_str):
    """
    Check if the typo ticker experienced a spike on the IPO day.
    """
    if not ipo_date_str:
        return None
        
    stats = {}
    
    try:
        # NASDAQ API gives date as M/D/YYYY or sometimes other formats
        try:
            start_date = dt.strptime(ipo_date_str, "%Y-%m-%d")
        except ValueError:
            # Try M/D/YYYY
            start_date = dt.strptime(ipo_date_str, "%m/%d/%Y")
            
        end_date = start_date + timedelta(days=5)
        
        # Buffer to get previous days for volume average
        fetch_start = start_date - timedelta(days=10)
        df = yf.download(typo_ticker, start=fetch_start, end=end_date, progress=False, auto_adjust=True)
        
        if df.empty:
            return None
            
        # Flatten MultiIndex columns if they exist (yfinance 0.2.x+ behavior)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        # Normalize index to midnight for matching
        df.index = df.index.normalize()
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        
        if start_date not in df.index:
            # Maybe it wasn't trading on that exact day? Try next available day within 3 days.
            trading_days = df.index[df.index >= start_date]
            if trading_days.empty or (trading_days[0] - start_date).days > 3:
                return None
            start_date = trading_days[0]
            
        day_data = df.loc[start_date]
        
        # Calculate Metrics
        prev_data = df[df.index < start_date]
        if not prev_data.empty:
            avg_vol = prev_data['Volume'].tail(5).mean()
            vol_spike = day_data['Volume'] / avg_vol if avg_vol > 0 else 1.0
            prev_close = prev_data['Close'].iloc[-1]
            gap_pct = (day_data['Open'] - prev_close) / prev_close if prev_close > 0 else 0.0
        else:
            avg_vol = 0.0
            vol_spike = 1.0
            prev_close = day_data['Open']
            gap_pct = 0.0
            
        intraday_high_pct = (day_data['High'] - day_data['Open']) / day_data['Open'] if day_data['Open'] > 0 else 0.0
        reversion_pct = (day_data['Close'] - day_data['High']) / day_data['High'] if day_data['High'] > 0 else 0.0
        day_return = (day_data['Close'] - prev_close) / prev_close if prev_close > 0 else 0.0
        
        stats.update({
            'Typo_Open': float(day_data['Open']),
            'Typo_High': float(day_data['High']),
            'Typo_Close': float(day_data['Close']),
            'Typo_Volume': int(day_data['Volume']),
            'Avg_Volume_5d': float(avg_vol),
            'Volume_Spike_Ratio': float(vol_spike),
            'Gap_Up_Pct': float(gap_pct),
            'Intraday_High_Pct': float(intraday_high_pct),
            'Reversion_From_High_Pct': float(reversion_pct),
            'Day_Return': float(day_return),
            'Date': start_date.strftime("%Y-%m-%d"),
            'Data_Source': 'yfinance'
        })
        
        return stats
        
    except Exception as e:
        return None

def main():
    print("--- Step 1: Loading Ticker Universe ---")
    all_tickers = get_ticker_data()
    if not all_tickers:
        print("Failed to load tickers.")
        return

    print("\n--- Step 2: Fetching IPO Data ---")
    current_year = dt.now().year
    
    # Handle CLI arguments
    if len(sys.argv) > 2:
        try:
            start_year = int(sys.argv[1])
            end_year = int(sys.argv[2])
        except ValueError:
            start_year, end_year = current_year - 1, current_year
    else:
        start_year, end_year = current_year - 1, current_year
        
    df_ipos = get_ipos_range(start_year, end_year)
    if df_ipos.empty:
        print("No IPOs found.")
        return
    
    df_ipos = df_ipos[df_ipos['IPO_Ticker'].notna() & (df_ipos['IPO_Ticker'] != '')]

    print(f"\n--- Step 3: Identify Keyboard-Proximate Typo Pairs for {len(df_ipos)} IPOs ---")
    typo_pairs = []
    total = len(df_ipos)
    for i, row in df_ipos.iterrows():
        if i % 50 == 0:
            print(f"Scanning IPO {i}/{total}...", end='\r')
        candidates = find_typos_for_ipo(row['IPO_Ticker'], row['IPO_Name'], all_tickers)
        for cand in candidates:
            if cand.get('Keyboard_Proximate'):
                cand['IPO_Date'] = row['IPO_Date']
                typo_pairs.append(cand)
            
    print(f"\nFound {len(typo_pairs)} keyboard-proximate typo pairs.")
    if typo_pairs:
        print("Sample pairs:")
        for p in typo_pairs[:5]:
            print(f"  {p['IPO_Ticker']} -> {p['Typo_Ticker']} ({p['Typo_Name']})")
    else:
        return

    print("\n--- Step 4: Analyzing Market Reaction ---")
    timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = os.path.join("data", f"ipo_study_{timestamp}")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "ipo_typo_results.csv")
    
    # Define columns explicitly
    cols = [
        'IPO_Ticker', 'IPO_Name', 'Typo_Ticker', 'Typo_Name', 'Distance', 'Keyboard_Proximate', 'IPO_Date',
        'Date', 'Data_Source', 'Typo_Open', 'Typo_High', 'Typo_Close', 'Typo_Volume', 'Avg_Volume_5d',
        'Volume_Spike_Ratio', 'Gap_Up_Pct', 'Intraday_High_Pct', 'Reversion_From_High_Pct', 'Day_Return'
    ]
    pd.DataFrame(columns=cols).to_csv(output_file, index=False)
    
    print(f"Results will be incrementally saved to {output_file}")
    buffer = []
    for i, pair in enumerate(typo_pairs):
        if i % 10 == 0:
            print(f"Analyzing pair {i+1}/{len(typo_pairs)}...", end='\r')
        stats = analyze_market_reaction(pair['IPO_Ticker'], pair['Typo_Ticker'], pair['IPO_Date'])
        if stats:
            pair.update(stats)
            buffer.append(pair)
            if len(buffer) >= 10:
                pd.DataFrame(buffer)[cols].to_csv(output_file, mode='a', header=False, index=False)
                buffer = []
    if buffer:
        pd.DataFrame(buffer)[cols].to_csv(output_file, mode='a', header=False, index=False)
    
    print(f"\nAnalysis complete. Results saved to {output_file}")
    
    if os.path.exists(output_file):
        df_results = pd.read_csv(output_file)
        if not df_results.empty:
            significant = df_results[
                (df_results['Volume_Spike_Ratio'] > 2.0) | 
                (df_results['Intraday_High_Pct'] > 0.05)
            ].sort_values(by='Volume_Spike_Ratio', ascending=False)
            
            if not significant.empty:
                print("\n--- Top Significant Events ---")
                print(significant[['IPO_Date', 'IPO_Ticker', 'Typo_Ticker', 'Volume_Spike_Ratio', 'Intraday_High_Pct', 'Day_Return']].head(10))
                sig_file = os.path.join(output_dir, "significant_ipo_events.csv")
                significant.to_csv(sig_file, index=False)
            else:
                print("\nNo significant events found.")

if __name__ == "__main__":
    main()
