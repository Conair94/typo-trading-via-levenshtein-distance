# Intraday Typo Analysis Summary (1-Minute Intervals)
**Run Date:** 2025-12-26 18:50:52
**Data Period:** Last 5 Days (Intraday)

## Hypotheses
1. **Market Open / Retail Action:** Large cap stocks with pre-market news often see "typo" stock correlation spike early in the trading day (09:30 - 10:00 AM) due to retail trader execution errors.
2. **Buying Pressure:** Correlations are expected to be stronger when the target stock is experiencing positive returns (buying pressure), as FOMO drives rapid, error-prone entry.
3. **Mean Reversion:** These correlations are temporary inefficiencies. As traders realize errors, the "typo" stock price should revert, making this a short-term mean-reversion play.
4. **Keyboard Proximity:** Typos involving keys that are physically adjacent on a QWERTY keyboard (e.g., 'R' vs 'T') are more likely to be genuine execution errors, potentially offering a stronger correlation signal.

## Overview
* **Total Pairs Analyzed:** 934
* **Focus:** Correlation of returns when Target stock is **Buying (Up)**.
* **Average Max Correlation (All):** 0.2869

## Keyboard Proximity Analysis
Comparing general Levenshtein distance matches vs. specific "fat finger" keyboard adjacency matches.

| Metric | All Pairs | Keyboard Proximate Pairs |
| :--- | :--- | :--- |
| **Count** | 934 | 109 |
| **Avg Max Correlation** | 0.2869 | 0.2740 |

**Portfolio Hedging Note:**
If "Keyboard Proximate" pairs demonstrate higher average correlation, they represent a higher-quality signal universe. In quantitative hedging, this allows for more efficient capital allocation—hedging the "typo" risk (or exploiting the mean reversion) with higher confidence and potentially lower basis risk.

## Time of Day Analysis
**When is the correlation strongest?**
* **Top Time Buckets:** 14:30:00 (185), N/A (151), 15:30:00 (67)

## Top 10 Pairs with Highest Intraday Buying Pressure Correlation

## Theoretical Hedging Performance (Top 10 Pairs)
Simulating a **Long Target / Short Candidate** strategy (Hedge Ratio = Correlation) during the Best Time Bucket.
*   **Average Alpha (Excess Return):** -32.73 bps per minute
| Target | Name | Candidate | Name | Best Time | Buying Corr | Alpha (bps) | Hedged Sharpe |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| IBIT | iShares Bitcoin Trust ETF | ILIT | iShares Lithium Miners and Producers ETF | 14:30:00 | 0.9560 | -25.62 | -0.37 |
| CRWG | Leverage Shares 2X Long CRWV Daily ETF | CRWV | CoreWeave, Inc. - Class A Common Stock | 14:30:00 | 0.9556 | -0.20 | -0.01 |
| AIRE | reAlpha Tech Corp. - Common Stock | JIRE | JPMorgan International Research Enhanced Equity ETF | 14:30:00 | 0.9044 | -1.23 | 0.13 |
| SOXL | Direxion Daily Semiconductor Bull 3X Shares | SPXL | Direxion Daily S&P 500 Bull 3X Shares | 14:30:00 | 0.8941 | -1.94 | 0.09 |
| QUBT | Quantum Computing Inc. - Common Stock | UBT | ProShares Ultra 20+ Year Treasury | 15:30:00 | 0.8804 | -3.20 | -0.05 |
| AG | First Majestic Silver Corp. Ordinary Shares (Canada) | IAG | Iamgold Corporation Ordinary Shares | 14:30:00 | 0.8644 | -2.31 | -0.00 |
| AG | First Majestic Silver Corp. Ordinary Shares (Canada) | AU | AngloGold Ashanti PLC Ordinary Shares | 14:30:00 | 0.8507 | -1.89 | 0.01 |
| AG | First Majestic Silver Corp. Ordinary Shares (Canada) | NG | Novagold Resources Inc. | 14:30:00 | 0.8171 | -2.29 | 0.00 |
| TRIB | Trinity Biotech plc - American Depositary Shares | TRIP | TripAdvisor, Inc. - Common Stock | 14:30:00 | 0.8164 | -0.98 | 0.08 |
| ATPC | Agape ATP Corporation - Common Stock | ASPC | A SPAC III Acquisition Corp. - Class A Ordinary Shares | 14:30:00 | 0.8089 | -287.61 | -0.23 |
