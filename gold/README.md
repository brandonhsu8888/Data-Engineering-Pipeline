# Phase 5: Gold Layer (OLAP Views)

## Overview

Phase 5 creates **4 Gold views** on top of the Silver layer Parquet data in DuckDB.
Each view provides a different analytical perspective on S&P 500 data (2004-2024).

| View | Description | Owner |
|------|-------------|-------|
| `v_market_daily_summary` | Daily market aggregates (avg close, return, volume) | Person A |
| `v_ticker_profile` | Latest ticker snapshot (company, sector, price) | Person A |
| `v_fundamental_snapshot` | Latest financial metrics per ticker | **Person B** |
| `v_sentiment_price_view` | Transcript sentiment + price reaction | **Person B** |

## Directory Structure

```
gold/
├── build_gold_layer.py      # One-click Gold layer builder
├── requirements.txt         # Python dependencies
├── README.md                # This file
├── sql/
│   ├── create_gold_views.sql  # Master DDL (all 4 views + Silver table loading)
│   ├── person_a_views.sql     # Person A's 2 views (standalone)
│   └── person_b_views.sql     # Person B's 2 views (standalone)
└── tests/
    └── test_gold_views.py     # 12-check validation test
```

## Quick Start

### Prerequisites
- Python 3.10+
- Silver layer data must exist in `output/silver/` (price, fundamentals, sentiment Parquet files)
- DuckDB database at `duckdb/spx_analytics.duckdb`

### Install Dependencies
```bash
pip install -r gold/requirements.txt
```

### Build Gold Layer
```bash
cd <repo_root>
python gold/build_gold_layer.py
```

### Verify Views
```bash
python gold/tests/test_gold_views.py
```

Expected output: `12 passed, 0 failed`

### Verify Only (no rebuild)
```bash
python gold/build_gold_layer.py --verify-only
```

## View Definitions

### v_fundamental_snapshot (Person B)
**Purpose**: Latest financial snapshot per ticker

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Stock ticker symbol |
| latest_report_date | DATE | Most recent report date |
| revenue | DOUBLE | Total revenue (TotalRevenue) |
| net_income | DOUBLE | Net income (NetIncome) |
| assets | DOUBLE | Total assets (TotalAssets) |
| liabilities | DOUBLE | Total liabilities (TotalLiabilitiesNetMinorityInterest) |

**Key Logic**: Uses `ROW_NUMBER()` to select latest report per ticker, then pivots metrics from rows to columns using exact metric name matching.

### v_sentiment_price_view (Person B)
**Purpose**: Links transcript sentiment to price reaction

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Stock ticker symbol |
| transcript_date | DATE | Transcript publication date |
| sentiment_score | DOUBLE | Polarity score (-1 to +1) |
| close_on_event_date | DOUBLE | Closing price on/near transcript date |
| next_1d_return | DOUBLE | 1-day forward return |
| next_5d_return | DOUBLE | 5-day forward return |

**Key Logic**: Uses DuckDB `ASOF LEFT JOIN` to match transcripts to the nearest prior trading day (handles weekends/holidays). Forward returns computed via `LEAD()` window function.

### v_market_daily_summary (Person A)
Daily market-wide aggregates: avg close, avg return, total volume, ticker count.

### v_ticker_profile (Person A)
Latest snapshot per ticker: company name, sector, most recent close/volume.

## Data Quality Notes

| View | Rows (20yr) | Completeness | Notes |
|------|-------------|-------------|-------|
| v_market_daily_summary | 5,284 | 99.98% | 1 NULL in avg_return (first day, no prior close) |
| v_ticker_profile | 818 | ~72% | Some tickers lack sector/company from fundamentals |
| v_fundamental_snapshot | 595 | 97.8% | 13 tickers missing revenue/net_income |
| v_sentiment_price_view | 32,036 | 93.3% | 6.74% NULL close (pre-listing transcripts) |

## Integration with Existing Pipeline

This Gold layer sits on top of Phases 1-4:
```
Phase 1-2: DataProvider + Simulator → landing_zone/
Phase 3:   Ingestion Engine → Bronze (DuckDB tables)
Phase 4:   ELT Pipeline → Silver (Parquet files)
Phase 5:   Gold Layer (this) → DuckDB Views
Phase 6:   Streamlit Dashboard (future)
```

The `build_gold_layer.py` script handles Silver→Gold:
1. Loads Silver Parquet files into DuckDB tables
2. Executes `create_gold_views.sql` to create all 4 views
3. Validates row counts and column schemas
