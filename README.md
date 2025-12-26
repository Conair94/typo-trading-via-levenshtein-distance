# Typo Trading: Quantifying "Fat Finger" Alpha via Levenshtein Distance

### Executive Summary
This project investigates a niche market inefficiency hypothesis: that execution errors ("fat finger" trades) or algorithmic confusion can cause price co-movement between major liquid stocks and visually similar, lower-volume tickers. 

By systematically identifying ticker pairs with low Damerau-Levenshtein edit distance (e.g., `TSLA` vs `TSL`) and analyzing their price correlation during high-volume anomaly events, this tool seeks to isolate potential alpha generation opportunities derived from market microstructure friction.

### The Thesis
In high-frequency and high-volume trading environments, execution errors occur. A trader or algorithm intending to trade a high-volume stock (Target) may erroneously route orders to a visually similar ticker (Candidate). 
*   **Hypothesis:** During periods of extreme volume on the Target stock ($>2\sigma$), the Candidate stock will exhibit statistically significant price correlation due to spillover liquidity or erroneous order flow.
*   **Counter-Thesis:** Most correlations are spurious or due to sector-wide movements.
*   **Solution:** This pipeline filters for intentional correlations (e.g., Bull/Bear ETFs) to isolate purely "typo-based" or visually induced relationships.

### Technical Architecture
The solution is built as a modular Python ETL (Extract, Transform, Load) pipeline designed for reproducibility and scalability.

*   **Data Acquisition:** Automated retrieval of 8,000+ tickers from NASDAQ/NYSE and Yahoo Finance API.
*   **Algorithmic Matching:** Utilizes **Damerau-Levenshtein distance** (metric space for string editing) to identify "typo" candidates within a distance of 1 (single insertion, deletion, substitution, or transposition).
*   **Signal Processing:** 
    *   Rolling statistical windows (20-day mean/std dev) to identify volume anomalies.
    *   Pearson correlation coefficient calculation on returns during identified event windows.
*   **Data Engineering:** 
    *   Timestamped data versioning for audit trails.
    *   Local SQLite caching for rapid prototyping.
    *   **Google BigQuery** integration for scalable, cloud-native data warehousing.

### Key Features
*   **Smart Filtering:** Systematically excludes "intentional" correlations (e.g., `TSLA` vs `TSLL` - Direxion Daily TSLA Bull) using regex-based heuristic analysis of security names.
*   **Event-Driven Analysis:** Focuses specifically on high-volume days, avoiding the noise of long-term beta.
*   **Cloud Ready:** Includes a dedicated module to push processed datasets to Google Cloud Platform for visualization in Looker or further analysis in SQL.

### Installation & Usage

#### Prerequisites
*   Python 3.8+
*   Google Cloud Account (Optional, for BigQuery features)

#### 1. Setup Environment
```bash
pip install -r requirements.txt
```

#### 2. Data Ingestion & Candidate Generation
Downloads fresh ticker lists and identifies potential typo pairs, filtering out ETFs and derivatives.
```bash
python3 fetch_data.py
```
*   *Output:* Creates a timestamped directory (e.g., `data/2023-10-27_14-00-00/`) containing `typo_candidates.csv`.

### 3. Quantitative Analysis
Fetches historical OHLCV data for identified pairs and computes correlation metrics during volume spikes.
```bash
python3 analyze_pairs.py [OPTIONAL_DATA_DIR]
```
*   *Output:*
    *   `study_results.csv`: Detailed correlation metrics for every pair.
    *   `typo_trading.db`: SQLite database for SQL queries.
    *   `README.md`: **Auto-generated statistical summary** including average correlations, standard deviation, and the top 5 most correlated pairs during high-volume events.
*   *Note:* By default, analyzes the most recent timestamped folder in `data/`. You can specify a different folder as an argument.

#### 4. Data Warehousing (Optional)
Uploads the results to a Google BigQuery dataset for enterprise-grade querying.
```bash
python3 gcp_upload.py <YOUR_PROJECT_ID>
```

### Future Roadmap
*   **Intraday Analysis:** Move from daily OHLCV to minute-level bars to capture fleeting execution errors.
*   **Causal Inference:** Implement Granger Causality tests to determine if the Target volume *predicts* the Candidate price movement.
*   **Live Scanning:** Convert the batch script into a real-time monitor using a websocket feed.

---
*Created by [Your Name]*
