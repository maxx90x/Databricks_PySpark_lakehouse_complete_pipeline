# Global Macro-Financial Analytics Pipeline

A data engineering project that investigates the correlations between macroeconomic growth (GDP) and equity market performance across five major global economies (**US, EU, China, Japan, and India**) using Azure Databricks, Delta Lake, and PySpark.

---

## Architecture & Tech Stack

*   **Platform:** Azure Databricks (Serverless / Single-Node Compute)
*   **Storage Framework:** Delta Lake (Medallion Architecture: Bronze, Silver, Gold)
*   **Languages:** Python, PySpark, SQL
*   **Version Control:** Git / GitHub

---

## Data Sources

| Market | Asset / Index | Ticker (`yfinance`) | Macroeconomic Data Source |
| :--- | :--- | :--- | :--- |
| **US** | S&P 500 | `^GSPC` | FRED (Federal Reserve Economic Data) / World Bank |
| **EU** | STOXX Europe 600 | `^STOXX` | Eurostat / OECD API |
| **China** | CSI 300 | `000300.SS` | World Bank / IMF API |
| **Japan** | Nikkei 225 | `^N225` | World Bank / OECD API |
| **India** | NIFTY 50 | `^NSEI` | World Bank / RBI API |

---

## Project Workplan & Phases

### Phase 1: Ingestion & Bronze Layer (Raw Data)
*   [ ] Initialize GitHub repository structure and link to Databricks Repos.
*   [ ] Write a modular PySpark notebook script to pull historical daily stock prices for all 5 indices via `yfinance`.
*   [ ] Fetch historical quarterly GDP growth data for the 5 target countries using public APIs.
*   [ ] Land raw API responses directly into **Bronze Delta tables** without transformations to preserve data lineage.

### Phase 2: Cleaning & Silver Layer (Harmonization)
*   [ ] Clean and standardize schemas across disparate data sources.
*   [ ] Handle missing values, trading holidays, and timestamp inconsistencies.
*   [ ] **Temporal Alignment:** Aggregate daily stock prices into quarterly returns to match quarterly GDP reporting cycles.
*   [ ] Write cleaned data into structured **Silver Delta tables**.

### Phase 3: Analytical Modeling & Gold Layer (Aggregations)
*   [ ] Join harmonized stock return data with GDP growth metrics.
*   [ ] Apply currency normalization rules (local currency vs. USD cross-comparison).
*   [ ] Compute lagging correlation coefficients to evaluate how equity markets track economic expansion/contraction.
*   [ ] Store final modeling-ready dataset in an optimized **Gold Delta table**.

### Phase 4: Visualization & Reporting
*   [ ] Build analytical query scripts using PySpark SQL.
*   [ ] Generate correlation matrices and comparative line charts using Python visualization libraries (`seaborn` / `matplotlib` / `plotly`) inside Databricks notebooks.
*   [ ] Document key financial findings and project takeaways in the final README.

---

## Repository Structure

```text
├── bronze/
│   ├── ingest_stock_data.py
│   └── ingest_gdp_data.py
├── silver/
│   └── clean_and_align_data.py
├── gold/
│   └── correlation_modeling.sql
├── notebooks/
│   └── exploratory_analysis.ipynb
└── README.md
