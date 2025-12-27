import pandas as pd
import yfinance as yf
import sqlite3
import numpy as np
import os
import sys
import time
from datetime import datetime, timedelta

def get_latest_data_dir(base_dir="data"):
    if not os.path.exists(base_dir):
        return None
    subdirs = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    if not subdirs:
        return None
    return max(subdirs, key=os.path.getmtime)

def load_candidates(file_path):
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return pd.DataFrame()

def fetch_history(ticker, period="5d", interval="1m"):
    """
    Fetches intraday historical data for a single ticker.
    Default: Last 5 days, 1-minute interval.
    """
    try:
        # Rate limit protection
        time.sleep(0.5) 
        df = yf.download(ticker, period=period, interval=interval, progress=False)
        return df
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return pd.DataFrame()

def analyze_intraday_correlation(target_df, candidate_df, target_ticker, candidate_ticker):
    """
    Analyzes intraday correlation, bucketed by time of day.
    Focuses on 'Buying Pressure' (Target > 0).
    """
    if target_df.empty or candidate_df.empty:
        return None

    # Helper to get series
    def get_col(df, name):
        if name in df.columns:
            s = df[name]
            if isinstance(s, pd.DataFrame):
                return s.iloc[:, 0]
            return s
        return None

    target_close = get_col(target_df, 'Close') # Intraday 'Adj Close' is same as 'Close'
    candidate_close = get_col(candidate_df, 'Close')
    
    if target_close is None or candidate_close is None:
        return None

    # Align timestamps (inner join)
    df = pd.DataFrame({
        'Target_Close': target_close,
        'Candidate_Close': candidate_close
    }).dropna()

    if df.empty:
        return None

    # Calculate 1-minute returns
    df['Target_Ret'] = df['Target_Close'].pct_change()
    df['Candidate_Ret'] = df['Candidate_Close'].pct_change()
    df = df.dropna()

    # Filter: Baseline Check (Total Period)
    # If the stocks track perfectly continuously (e.g. > 0.9), it's likely not a typo trade
    overall_corr = df['Target_Ret'].corr(df['Candidate_Ret'])
    if abs(overall_corr) > 0.9:
        return None

    # Add Time Buckets (30 min intervals)
    # df.index is DatetimeIndex. 
    # We want strictly the time component (09:30, 10:00...)
    df['Time'] = df.index.time
    
    # Bucket logic: Round down to nearest 30 mins
    def floor_time(t):
        minutes = t.minute
        floored_min = (minutes // 30) * 30
        return t.replace(minute=floored_min, second=0, microsecond=0)

    df['Time_Bucket'] = df['Time'].apply(floor_time)

    # Calculate Correlation per Bucket
    # We aggregate all days (5 days) into these time slots
    # Explicitly select columns to silence FutureWarning about grouping keys
    bucket_corrs = df.groupby('Time_Bucket')[['Target_Ret', 'Candidate_Ret']].apply(
        lambda x: x['Target_Ret'].corr(x['Candidate_Ret'])
    )
    
    # Calculate "Buying Pressure" Correlation (Target Returns > 0) per bucket
    bucket_corrs_buy = df[df['Target_Ret'] > 0].groupby('Time_Bucket')[['Target_Ret', 'Candidate_Ret']].apply(
        lambda x: x['Target_Ret'].corr(x['Candidate_Ret'])
    )

    # Find the Best Time Bucket (Max Correlation)
    # We focus on the "Buying Pressure" scenario as requested
    if bucket_corrs_buy.empty or bucket_corrs_buy.isna().all():
        return {
            'Target': target_ticker,
            'Candidate': candidate_ticker,
            'Overall_Corr': overall_corr,
            'Best_Time': "N/A",
            'Best_Time_Corr': 0.0,
            'Buying_Pressure_Corr_All': df[df['Target_Ret'] > 0]['Target_Ret'].corr(df[df['Target_Ret'] > 0]['Candidate_Ret'])
        }
        
    # Get max correlation time slot
    best_time = bucket_corrs_buy.idxmax()
    max_corr = bucket_corrs_buy.max()
    
    # Calculate Correlation Lift (How much better is the specific time vs overall?)
    # This supports the hypothesis that the effect is short-term/transient.
    corr_lift = max_corr - overall_corr

    return {
        'Target': target_ticker,
        'Candidate': candidate_ticker,
        'Overall_Corr': overall_corr,
        'Best_Time': str(best_time),
        'Best_Time_Corr': max_corr,
        'Correlation_Lift': corr_lift,
        # Also return correlation during that time slot for ALL moves (not just up) for context
        'General_Corr_At_Best_Time': bucket_corrs.get(best_time, np.nan)
    }

def save_to_db(results, db_path):
    conn = sqlite3.connect(db_path)
    df = pd.DataFrame(results)
    df.to_sql("intraday_study", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Results saved to SQLite database: {db_path}")

def generate_summary_readme(df, data_dir):
    readme_path = os.path.join(data_dir, "README_INTRADAY.md")
    
    # Calculate stats
    total_pairs = len(df)
    avg_best_corr = df['Best_Time_Corr'].mean()
    
    # Most common "Best Time"
    if 'Best_Time' in df.columns:
        common_times = df['Best_Time'].value_counts().head(3)
        common_times_str = ", ".join([f"{t} ({c})" for t, c in common_times.items()])
    else:
        common_times_str = "N/A"
    
    content = f"""# Intraday Typo Analysis Summary (1-Minute Intervals)
**Run Date:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
**Data Period:** Last 5 Days (Intraday)

## Hypotheses
1. **Market Open / Retail Action:** Large cap stocks with pre-market news often see "typo" stock correlation spike early in the trading day (09:30 - 10:00 AM) due to retail trader execution errors.
2. **Buying Pressure:** Correlations are expected to be stronger when the target stock is experiencing positive returns (buying pressure), as FOMO drives rapid, error-prone entry.
3. **Mean Reversion:** These correlations are temporary inefficiencies. As traders realize errors, the "typo" stock price should revert, making this a short-term mean-reversion play.

## Overview
* **Total Pairs Analyzed:** {total_pairs}
* **Focus:** Correlation of returns when Target stock is **Buying (Up)**.
* **Average Max Correlation:** {avg_best_corr:.4f}

## Time of Day Analysis
**When is the correlation strongest?**
* **Top Time Buckets:** {common_times_str}

## Top 10 Pairs with Highest Intraday Buying Pressure Correlation
"""
    # Helper to safe get string
    def safe_str(val):
        s = str(val)
        return s if s != 'nan' else 'N/A'

    # Top 10 Positive
    top_10 = df.sort_values(by='Best_Time_Corr', ascending=False).head(10)
    
    content += "| Target | Name | Candidate | Name | Best Time | Buying Corr (Best Time) | Overall Corr |\n"
    content += "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n"
    
    for _, row in top_10.iterrows():
        t_name = safe_str(row.get('Target_Name', 'N/A'))
        c_name = safe_str(row.get('Candidate_Name', 'N/A'))
        content += f"| {row['Target']} | {t_name} | {row['Candidate']} | {c_name} | {row['Best_Time']} | {row['Best_Time_Corr']:.4f} | {row['Overall_Corr']:.4f} |\n"

    with open(readme_path, "w") as f:
        f.write(content)
    print(f"Summary README generated at: {readme_path}")

def main():
    # 1. Determine data directory
    if len(sys.argv) > 1:
        data_dir = sys.argv[1]
        if not os.path.exists(data_dir):
            print(f"Error: Directory '{data_dir}' does not exist.")
            return
    else:
        # Find latest data directory
        data_dir = get_latest_data_dir()
        if not data_dir:
            print("No data directory found. Please run fetch_data.py first.")
            return
    
    print(f"Using data from: {data_dir}")
    
    candidates_file = os.path.join(data_dir, "typo_candidates.csv")
    df_candidates = load_candidates(candidates_file)
    
    if df_candidates.empty:
        return

    results = []
    
    total_pairs = len(df_candidates)
    print(f"Analyzing {total_pairs} pairs (Intraday 1m)...")
    
    data_cache = {}

    for idx, row in df_candidates.iterrows():
        target = row['Target_Ticker']
        candidate = row['Candidate_Ticker']
        
        print(f"Processing {idx+1}/{total_pairs}: {target} vs {candidate}")
        
        if target not in data_cache:
            data_cache[target] = fetch_history(target)
        
        if candidate not in data_cache:
            data_cache[candidate] = fetch_history(candidate)
            
        res = analyze_intraday_correlation(data_cache[target], data_cache[candidate], target, candidate)
        if res:
            res['Distance'] = row['Distance']
            res['Target_Name'] = row.get('Target_Name', 'N/A')
            res['Candidate_Name'] = row.get('Candidate_Name', 'N/A')
            results.append(res)
            
    if results:
        db_path = os.path.join(data_dir, "typo_trading.db")
        save_to_db(results, db_path)
        
        csv_path = os.path.join(data_dir, "intraday_results.csv")
        df_results = pd.DataFrame(results)
        df_results.to_csv(csv_path, index=False)
        print(f"Results saved to {csv_path}")
        
        generate_summary_readme(df_results, data_dir)
    else:
        print("No results generated.")

if __name__ == "__main__":
    main()