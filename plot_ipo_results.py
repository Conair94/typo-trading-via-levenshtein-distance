import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import seaborn as sns
import yfinance as yf
from datetime import datetime, timedelta

# Set style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = [10, 6]

def load_latest_results():
    list_of_files = glob.glob('data/ipo_study_*/ipo_typo_results.csv')
    if not list_of_files:
        print("No result files found.")
        return None
    latest_file = max(list_of_files, key=os.path.getctime)
    return pd.read_csv(latest_file)

def plot_scatter(df, output_path):
    """
    Scatter plot of Volume Spike Ratio vs Intraday High Pct.
    Highlights high conviction trades.
    """
    plt.figure(figsize=(10, 6))
    
    # Filter for reasonable visualization (exclude extreme outliers if any)
    plot_df = df[df['Volume_Spike_Ratio'] < 50].copy() 
    
    # Create scatter
    sns.scatterplot(
        data=plot_df, 
        x='Volume_Spike_Ratio', 
        y='Intraday_High_Pct',
        hue='Keyboard_Proximate',
        style='Keyboard_Proximate',
        palette='viridis',
        s=100,
        alpha=0.7
    )
    
    # Highlight Significant Events
    sig_events = df[(df['Volume_Spike_Ratio'] > 3.0) & (df['Intraday_High_Pct'] > 0.02)]
    
    for i, row in sig_events.iterrows():
        plt.text(
            row['Volume_Spike_Ratio'] + 0.1, 
            row['Intraday_High_Pct'], 
            f"{row['IPO_Ticker']}->{row['Typo_Ticker']}", 
            fontsize=9, 
            weight='bold',
            color='red'
        )

    plt.title('Volume Buying Pressure vs. Price Spike Magnitude', fontsize=14)
    plt.xlabel('Volume Spike Ratio (vs 5-Day Avg)', fontsize=12)
    plt.ylabel('Intraday High % (Max Gain)', fontsize=12)
    plt.axhline(0, color='black', linewidth=0.5, linestyle='--')
    plt.axvline(1, color='black', linewidth=0.5, linestyle='--')
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    print(f"Saved scatter plot to {output_path}")

def plot_intraday_example(ipo_ticker, typo_ticker, date_str, output_path):
    """
    Plots minute-level intraday chart for a specific event to show the spike & revert.
    """
    print(f"Fetching intraday data for {typo_ticker} on {date_str}...")
    
    # Need to handle date parsing carefully
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        target_date = datetime.strptime(date_str, "%m/%d/%Y")
        
    start_date = target_date.strftime("%Y-%m-%d")
    end_date = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    
    # yfinance only provides 1m data for the last 7 days usually.
    # For historical dates (2024), we can't get 1m data via standard free API.
    # We will fetch daily data for context (OHLC) instead, or mock the shape if old.
    # However, since we want a "figure", showing the Daily OHLC bar with previous days context is better than nothing.
    
    # Fetch Daily Context (1 month)
    context_start = (target_date - timedelta(days=20)).strftime("%Y-%m-%d")
    context_end = (target_date + timedelta(days=5)).strftime("%Y-%m-%d")
    
    df = yf.download(typo_ticker, start=context_start, end=context_end, progress=False, auto_adjust=True)
    
    if df.empty:
        print("No data found for chart.")
        return

    # Flatten MultiIndex if exists
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    plt.figure(figsize=(12, 6))
    
    # Plot Close Price
    plt.plot(df.index, df['Close'], label='Close Price', color='blue', marker='o')
    
    # Highlight the IPO Day
    if target_date in df.index:
        day_data = df.loc[target_date]
        plt.scatter([target_date], [day_data['High']], color='red', s=150, zorder=5, label='Intraday High (Sell Point)')
        plt.scatter([target_date], [day_data['Open']], color='green', s=100, zorder=5, label='Open (Buy Point)')
        
        # Annotate
        plt.annotate(
            f"IPO Event: {ipo_ticker}", 
            xy=(target_date, day_data['High']), 
            xytext=(target_date, day_data['High'] * 1.05),
            arrowprops=dict(facecolor='black', shrink=0.05),
            ha='center'
        )
        
    plt.title(f"Market Reaction: {typo_ticker} (Typo) on {ipo_ticker} IPO Date", fontsize=14)
    plt.ylabel("Price ($)", fontsize=12)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    print(f"Saved intraday/daily context plot to {output_path}")

def main():
    os.makedirs("images", exist_ok=True)
    
    df = load_latest_results()
    if df is not None:
        # 1. Scatter Plot
        plot_scatter(df, "images/volume_vs_return_scatter.png")
        
        # 2. Example Trade (HDL -> BDL from our results)
        # Check if date format in CSV is YYYY-MM-DD or M/D/YYYY
        # Our CSV has Date as YYYY-MM-DD usually, but IPO_Date might be varying.
        # We'll use the row data.
        example_row = df[df['IPO_Ticker'] == 'HDL']
        if not example_row.empty:
            row = example_row.iloc[0]
            # Use 'Date' column which is the actual trading date found
            plot_intraday_example('HDL', 'BDL', str(row['Date']), "images/hdl_bdl_event.png")
        else:
            # Fallback
            print("HDL example not found, plotting first significant event.")
            sig = df[df['Volume_Spike_Ratio'] > 3.0].iloc[0]
            plot_intraday_example(sig['IPO_Ticker'], sig['Typo_Ticker'], str(sig['Date']), "images/example_event.png")

if __name__ == "__main__":
    main()
