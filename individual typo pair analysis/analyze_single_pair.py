import pandas as pd
import yfinance as yf
import numpy as np
import sys
import os
from datetime import datetime

def fetch_data(target, candidate, period="7d", interval="1m"):
    """
    Fetches historical data for the target and candidate.
    Uses '1m' interval. Note: 1m data is limited to the last 7 days by Yahoo Finance.
    """
    print(f"\nFetching data for {target} and {candidate}...")
    print(f"Period: {period}, Interval: {interval}")
    
    tickers = f"{target} {candidate}"
    try:
        df = yf.download(tickers, period=period, interval=interval, group_by='ticker', progress=True, auto_adjust=True)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def analyze_pair(df, target, candidate):
    if df.empty:
        print("No data found.")
        return None

    # Handle MultiIndex columns from yfinance group_by='ticker'
    try:
        target_close = df[target]['Close']
        candidate_close = df[candidate]['Close']
    except KeyError:
        print("Could not find Close price data for one or both tickers.")
        return None

    # Align and calculate returns
    data = pd.DataFrame({
        'Target_Close': target_close,
        'Candidate_Close': candidate_close
    }).dropna()

    if data.empty:
        print("No overlapping data found.")
        return None

    data['Target_Ret'] = data['Target_Close'].pct_change()
    data['Candidate_Ret'] = data['Candidate_Close'].pct_change()
    data = data.dropna()

    # --- 1. General Stats ---
    overall_corr = data['Target_Ret'].corr(data['Candidate_Ret'])
    
    # --- 2. Time of Day Analysis (Market Open Hypothesis) ---
    data['Time'] = data.index.time
    
    def floor_time(t):
        minutes = t.minute
        floored_min = (minutes // 30) * 30
        return t.replace(minute=floored_min, second=0, microsecond=0)

    data['Time_Bucket'] = data['Time'].apply(floor_time)
    
    def safe_corr(x, min_obs=10): 
        if len(x) < min_obs:
            return np.nan
        if x['Target_Ret'].std() == 0 or x['Candidate_Ret'].std() == 0:
            return np.nan
        return x['Target_Ret'].corr(x['Candidate_Ret'])

    bucket_corrs = data.groupby('Time_Bucket')[['Target_Ret', 'Candidate_Ret']].apply(safe_corr)
    
    # --- 3. Buying Pressure Analysis (Target > 0) ---
    buying_pressure_data = data[data['Target_Ret'] > 0]
    buying_overall_corr = buying_pressure_data['Target_Ret'].corr(buying_pressure_data['Candidate_Ret'])
    
    bucket_corrs_buy = buying_pressure_data.groupby('Time_Bucket')[['Target_Ret', 'Candidate_Ret']].apply(safe_corr)
    
    best_time = "N/A"
    best_time_corr = 0.0
    hedging_stats = {}

    if not bucket_corrs_buy.empty and not bucket_corrs_buy.isna().all():
        best_time = bucket_corrs_buy.idxmax()
        best_time_corr = bucket_corrs_buy.max()
        
        # --- 4. Alpha Generation / Hedging Simulation ---
        # Strategy: During the Best Time Bucket, Long Target and Short Candidate (Hedge Ratio = Correlation)
        # Only trade when Target is expected to move (or continuously in that bucket)
        # Here we simulate continuously being in the trade during that 30m window every day.
        
        # Filter for the best time bucket
        strategy_data = data[data['Time_Bucket'] == best_time].copy()
        
        t_ret = strategy_data['Target_Ret']
        c_ret = strategy_data['Candidate_Ret']
        
        # Hedge Ratio (Naive)
        hedge_ratio = best_time_corr
        
        # Returns
        unhedged_ret = t_ret
        hedged_ret = t_ret - (hedge_ratio * c_ret)
        
        # Stats
        avg_unhedged = unhedged_ret.mean()
        avg_hedged = hedged_ret.mean()
        
        std_unhedged = unhedged_ret.std()
        std_hedged = hedged_ret.std()
        
        sharpe_unhedged = avg_unhedged / std_unhedged if std_unhedged > 0 else 0
        sharpe_hedged = avg_hedged / std_hedged if std_hedged > 0 else 0
        
        alpha_bps = (avg_hedged - avg_unhedged) * 10000
        
        hedging_stats = {
            'Hedge_Ratio': hedge_ratio,
            'Unhedged_Mean_Ret': avg_unhedged,
            'Hedged_Mean_Ret': avg_hedged,
            'Unhedged_Sharpe': sharpe_unhedged,
            'Hedged_Sharpe': sharpe_hedged,
            'Alpha_Bps': alpha_bps,
            'Vol_Reduction': (1 - (std_hedged / std_unhedged)) * 100 if std_unhedged > 0 else 0
        }

    return {
        'target': target,
        'candidate': candidate,
        'period_days': len(data.index.normalize().unique()),
        'total_obs': len(data),
        'overall_corr': overall_corr,
        'buying_corr': buying_overall_corr,
        'bucket_corrs': bucket_corrs,
        'bucket_corrs_buy': bucket_corrs_buy,
        'best_time': best_time,
        'best_time_corr': best_time_corr,
        'hedging_stats': hedging_stats
    }

def print_report(res):
    if not res:
        return

    print("\n" + "="*50)
    print(f"  INDIVIDUAL TYPO ANALYSIS: {res['target']} vs {res['candidate']}")
    print("="*50)
    print(f"Data Points: {res['total_obs']} ({res['period_days']} trading days)")
    print(f"Overall Correlation: {res['overall_corr']:.4f}")
    print(f"Buying Pressure Correlation (Target > 0): {res['buying_corr']:.4f}")
    
    print("\n--- Time of Day Analysis (Hypothesis 1) ---")
    print("Correlation by 30m Bucket:")
    print(res['bucket_corrs'].to_string())
    
    print("\n--- Buying Pressure by Time (Hypothesis 2) ---")
    print("Correlation when Target is Up, by Bucket:")
    print(res['bucket_corrs_buy'].to_string())
    
    if res['hedging_stats']:
        hs = res['hedging_stats']
        print(f"\n--- Alpha Generation (Best Time: {res['best_time']}) ---")
        print(f"Simulated Strategy: Long {res['target']} / Short {res['candidate']}")
        print(f"Hedge Ratio: {hs['Hedge_Ratio']:.2f}")
        print(f"Alpha (Excess Return): {hs['Alpha_Bps']:.2f} bps per interval")
        print(f"Volatility Reduction: {hs['Vol_Reduction']:.2f}%")
        print(f"Sharpe Ratio Improvement: {hs['Unhedged_Sharpe']:.4f} -> {hs['Hedged_Sharpe']:.4f}")
    else:
        print("\nNo valid hedging window found.")
    
    print("="*50)

def main():
    print("--- Individual Typo Pair Analysis Tool ---")
    
    if len(sys.argv) == 3:
        target = sys.argv[1].upper()
        candidate = sys.argv[2].upper()
    else:
        target = input("Enter Target Ticker (e.g., TSLA): ").strip().upper()
        candidate = input("Enter Candidate Ticker (e.g., TSLL): ").strip().upper()
    
    # Defaults for 1m resolution
    period = "7d"
    interval = "1m"
    
    print(f"\nAnalyzing {target} vs {candidate} over last {period} ({interval} intervals)...")
    
    df = fetch_data(target, candidate, period, interval)
    results = analyze_pair(df, target, candidate)
    
    print_report(results)
    
    # Save to file
    filename = f"analysis_{target}_vs_{candidate}.txt"
    with open(filename, "w") as f:
        sys.stdout = f
        print_report(results)
        sys.stdout = sys.__stdout__
    print(f"\nReport saved to {os.getcwd()}/{filename}")

if __name__ == "__main__":
    main()
