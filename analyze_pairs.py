import pandas as pd
import yfinance as yf
import sqlite3
import numpy as np
from datetime import datetime, timedelta

def load_candidates(file_path="typo_candidates.csv"):
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"File {file_path} not found. Please run fetch_data.py first.")
        return pd.DataFrame()

def fetch_history(ticker, period="1y"):
    """
    Fetches historical data for a single ticker.
    """
    try:
        # download returns a DataFrame with MultiIndex columns if not flattened, 
        # but for single ticker it's usually simple.
        df = yf.download(ticker, period=period, progress=False)
        return df
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return pd.DataFrame()

def analyze_correlation(target_df, candidate_df, target_ticker, candidate_ticker):
    """
    Analyzes correlation between target and candidate, specifically focusing on
    high volume days of the target.
    """
    if target_df.empty or candidate_df.empty:
        return None

    # Align dates
    # 'Adj Close' and 'Volume' are standard columns
    
    # Handle yfinance multi-level column issue if present
    # (Recent yfinance versions sometimes return (Price, Ticker) columns)
    # Helper to get series and handle multi-index
    def get_col(df, name):
        if name in df.columns:
            s = df[name]
            if isinstance(s, pd.DataFrame):
                return s.iloc[:, 0]
            return s
        return None

    target_vol = get_col(target_df, 'Volume')
    candidate_vol = get_col(candidate_df, 'Volume')
    
    target_close = get_col(target_df, 'Adj Close')
    if target_close is None:
        target_close = get_col(target_df, 'Close')

    candidate_close = get_col(candidate_df, 'Adj Close')
    if candidate_close is None:
        candidate_close = get_col(candidate_df, 'Close')

    if target_vol is None or candidate_vol is None or target_close is None or candidate_close is None:
        return None

    # Create a combined dataframe
    df = pd.DataFrame({
        'Target_Close': target_close,
        'Target_Vol': target_vol,
        'Candidate_Close': candidate_close,
        'Candidate_Vol': candidate_vol
    }).dropna()

    if df.empty:
        return None

    # Identify high volume days for Target (e.g., > 2 std dev above rolling mean)
    # Using a 20-day rolling mean
    df['Vol_Mean'] = df['Target_Vol'].rolling(window=20).mean()
    df['Vol_Std'] = df['Target_Vol'].rolling(window=20).std()
    df['High_Vol_Event'] = df['Target_Vol'] > (df['Vol_Mean'] + 2 * df['Vol_Std'])

    high_vol_days = df[df['High_Vol_Event']]
    
    if high_vol_days.empty:
        return {
            'Target': target_ticker,
            'Candidate': candidate_ticker,
            'Correlation_All': df['Target_Close'].corr(df['Candidate_Close']),
            'High_Vol_Count': 0,
            'Correlation_High_Vol': None
        }

    # Correlation on high volume days (price movement)
    # We look at daily returns correlation
    df['Target_Ret'] = df['Target_Close'].pct_change()
    df['Candidate_Ret'] = df['Candidate_Close'].pct_change()
    
    corr_all = df['Target_Ret'].corr(df['Candidate_Ret'])
    
    # Correlation specifically on the high volume days
    # (Note: Sample size might be small)
    high_vol_corr = df.loc[df['High_Vol_Event'], 'Target_Ret'].corr(df.loc[df['High_Vol_Event'], 'Candidate_Ret'])

    return {
        'Target': target_ticker,
        'Candidate': candidate_ticker,
        'Correlation_All': corr_all,
        'High_Vol_Count': len(high_vol_days),
        'Correlation_High_Vol': high_vol_corr
    }

def save_to_db(results, db_name="typo_trading.db"):
    conn = sqlite3.connect(db_name)
    df = pd.DataFrame(results)
    df.to_sql("correlation_study", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Results saved to SQLite database: {db_name}")

def main():
    df_candidates = load_candidates()
    if df_candidates.empty:
        return

    results = []
    
    # Iterate through unique pairs
    # Note: fetch_data.py produces Target, Candidate, Distance
    
    total_pairs = len(df_candidates)
    print(f"Analyzing {total_pairs} pairs...")
    
    # Cache data to avoid re-downloading same target multiple times
    data_cache = {}

    for idx, row in df_candidates.iterrows():
        target = row['Target_Ticker']
        candidate = row['Candidate_Ticker']
        
        print(f"Processing {idx+1}/{total_pairs}: {target} vs {candidate}")
        
        # Fetch Target Data
        if target not in data_cache:
            data_cache[target] = fetch_history(target)
        
        # Fetch Candidate Data
        if candidate not in data_cache:
            data_cache[candidate] = fetch_history(candidate)
            
        res = analyze_correlation(data_cache[target], data_cache[candidate], target, candidate)
        if res:
            res['Distance'] = row['Distance']
            results.append(res)
            
    # Save results
    if results:
        save_to_db(results)
        
        # Also CSV for easy viewing
        pd.DataFrame(results).to_csv("study_results.csv", index=False)
        print("Results saved to study_results.csv")
    else:
        print("No results generated.")

if __name__ == "__main__":
    main()
