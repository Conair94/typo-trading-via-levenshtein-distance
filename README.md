# Typo Trading Analysis

This project identifies stock tickers that are visually similar (low Damerau-Levenshtein distance) to top volume traded stocks and analyzes if there is a correlation in their price movements during high-volume events.

## Setup

1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

### 1. Fetch Data & Identify Typo Candidates
Run the `fetch_data.py` script to download NASDAQ/NYSE ticker lists, identify the top 100 stocks by volume, and find similar tickers.
```bash
python3 fetch_data.py
```
**Output:** `typo_candidates.csv`, `top_100_volume.csv`

### 2. Analyze Correlations
Run `analyze_pairs.py` to fetch historical data for the identified pairs and check for price correlation, specifically on days where the major stock has high volume.
```bash
python3 analyze_pairs.py
```
**Output:** `study_results.csv`, `typo_trading.db` (SQLite database)

### 3. Upload to Google Cloud (Optional)
If you have a Google Cloud Project and want to create a searchable database in BigQuery:
```bash
python3 gcp_upload.py <YOUR_PROJECT_ID>
```
*Note: Requires Google Cloud credentials to be set up (e.g., `export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"`).*

## Methodology
- **Distance Metric:** Damerau-Levenshtein distance (accounts for insertions, deletions, substitutions, and transpositions).
- **High Volume Event:** defined as volume > 2 standard deviations above the 20-day rolling mean.