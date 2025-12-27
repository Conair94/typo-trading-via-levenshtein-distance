import pandas as pd
import yfinance as yf
import sqlite3
import numpy as np
import os
import sys
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

    # Calculate returns for the whole period
    df['Target_Ret'] = df['Target_Close'].pct_change()
    df['Candidate_Ret'] = df['Candidate_Close'].pct_change()
    
    # Calculate baseline correlation
    corr_all = df['Target_Ret'].corr(df['Candidate_Ret'])
    
    # Filter out pairs with implausibly high baseline correlation (likely derivatives or sector peers)
    if abs(corr_all) > 0.9:
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
            'Correlation_All': corr_all,
            'High_Vol_Count': 0,
            'Correlation_High_Vol': None
        }

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

def save_to_db(results, db_path):
    conn = sqlite3.connect(db_path)
    df = pd.DataFrame(results)
    df.to_sql("correlation_study", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Results saved to SQLite database: {db_path}")

def generate_summary_readme(df, data_dir):
    readme_path = os.path.join(data_dir, "README.md")
    
    # Calculate stats
    total_pairs = len(df)
    
    # Baseline stats
    avg_corr_all = df['Correlation_All'].mean()
    std_corr_all = df['Correlation_All'].std()
    
    # High Vol stats (filter for non-null)
    high_vol_series = df['Correlation_High_Vol'].dropna()
    avg_corr_hv = high_vol_series.mean() if not high_vol_series.empty else np.nan
    std_corr_hv = high_vol_series.std() if not high_vol_series.empty else np.nan
    count_hv = len(high_vol_series)
    
    content = f"""# Analysis Summary
**Run Date:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Overview
* **Total Pairs Analyzed:** {total_pairs}
* **Pairs with High Volume Events:** {count_hv} ({count_hv/total_pairs*100:.1f}%)

## Statistical Metrics

### Baseline Correlation (All Days)
* **Mean:** {avg_corr_all:.4f}
* **Std Dev:** {std_corr_all:.4f}

### High Volume Event Correlation
* **Mean:** {avg_corr_hv:.4f}
* **Std Dev:** {std_corr_hv:.4f}

## Top 5 Positively Correlated Pairs (High Volume Events)
"""
    # Helper to safe get string
    def safe_str(val):
        s = str(val)
        return s if s != 'nan' else 'N/A'

    # Top 5 Positive
    if not high_vol_series.empty:
        top_5 = df.sort_values(by='Correlation_High_Vol', ascending=False).head(5)
        content += "| Target (Ticker) | Target Name | Candidate (Ticker) | Candidate Name | Distance | High Vol Corr | Baseline Corr |\n"
        content += "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n"
        for _, row in top_5.iterrows():
            t_name = safe_str(row.get('Target_Name', 'N/A'))
            c_name = safe_str(row.get('Candidate_Name', 'N/A'))
            content += f"| {row['Target']} | {t_name} | {row['Candidate']} | {c_name} | {row['Distance']} | {row['Correlation_High_Vol']:.4f} | {row['Correlation_All']:.4f} |\n"
    else:
        content += "No high volume events found.\n"
        
    content += "\n## Top 5 Negatively Correlated Pairs (High Volume Events)\n"
    if not high_vol_series.empty:
        bot_5 = df.sort_values(by='Correlation_High_Vol', ascending=True).head(5)
        content += "| Target (Ticker) | Target Name | Candidate (Ticker) | Candidate Name | Distance | High Vol Corr | Baseline Corr |\n"
        content += "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n"
        for _, row in bot_5.iterrows():
            t_name = safe_str(row.get('Target_Name', 'N/A'))
            c_name = safe_str(row.get('Candidate_Name', 'N/A'))
            content += f"| {row['Target']} | {t_name} | {row['Candidate']} | {c_name} | {row['Distance']} | {row['Correlation_High_Vol']:.4f} | {row['Correlation_All']:.4f} |\n"


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
    
    # Iterate through unique pairs
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
            # Pass through names if available
            res['Target_Name'] = row.get('Target_Name', 'N/A')
            res['Candidate_Name'] = row.get('Candidate_Name', 'N/A')
            results.append(res)
            
    # Save results to the same directory
    if results:
        db_path = os.path.join(data_dir, "typo_trading.db")
        save_to_db(results, db_path)
        
        csv_path = os.path.join(data_dir, "study_results.csv")
        df_results = pd.DataFrame(results)
        df_results.to_csv(csv_path, index=False)
        print(f"Results saved to {csv_path}")
        
        # Generate Summary
        generate_summary_readme(df_results, data_dir)
    else:
        print("No results generated.")

if __name__ == "__main__":
    main()
