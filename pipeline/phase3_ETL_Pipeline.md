# Phase 3: ELT Pipeline (Bronze -> Silver)

## Overview

Phase 3 implements the ELT (Extract-Load-Transform) pipeline that reads raw data from the Bronze layer in DuckDB, applies deduplication and quality validation using DuckDB SQL, and writes clean Silver-layer Parquet files partitioned by date (price) and ticker (fundamentals).

**Module**: `pipeline/elt_pipeline.py`
**Class**: `ELTPipeline`
**Technology**: DuckDB SQL + Python (pandas)

## Architecture

```
Bronze (DuckDB tables)                    Silver (Parquet on disk)
┌─────────────────────┐                   ┌──────────────────────────────────┐
│ raw_price_stream    │──dedup──validate──>│ output/silver/price/             │
│                     │         │          │   date=YYYY-MM-DD/data_0.parquet │
└─────────────────────┘         │          └──────────────────────────────────┘
                                v
                      ┌──────────────────────┐
                      │ silver_quality_issues │
                      │ (DuckDB audit table)  │
                      └──────────────────────┘

┌─────────────────────┐                   ┌──────────────────────────────────┐
│raw_fundamental_index│──dedup──unpivot──>│ output/silver/fundamentals/      │
│ (index + CSV files) │  read CSVs         │   ticker=XXX/data.parquet        │
└─────────────────────┘                   └──────────────────────────────────┘
```

## Execution Flow

### Stage 1: `ensure_silver_objects()`

Creates the `silver_quality_issues` table in DuckDB for logging data quality violations. Uses `IF NOT EXISTS` for idempotency.

### Stage 2: `transform_price()`

Transforms `raw_price_stream` into Hive-partitioned Silver Parquet files.

1. **Deduplicate**: Bronze has no `UNIQUE(ticker, date)` constraint, so duplicate rows can exist from re-ingestion. A DuckDB SQL window function (`ROW_NUMBER` partitioned by `ticker, date`, ordered by `received_at DESC`) keeps only the most recent row per ticker+date.

2. **Quality Validation**: Runs 11 quality rules against the deduped data, all defined in the design spec:
   - `open, high, low, close, adj_close > 0` (when not NULL)
   - `volume >= 0`
   - `high >= low`, `high >= open`, `high >= close`
   - `low <= open`, `low <= close`

   Violations are **logged** to `silver_quality_issues` — rows are **never dropped**.

3. **Parquet Export**: Uses DuckDB `COPY TO` with `PARTITION_BY (date)` for efficient Hive-partitioned output. The existing Silver price directory is wiped first for idempotent reruns.

### Stage 3: `transform_fundamentals()`

Transforms `raw_fundamental_index` entries into normalised long-format Parquet.

1. **Deduplicate Index**: Keeps the latest `received_at` per `ticker + report_type`.

2. **Read & Unpivot**: For each index entry, reads the landing-zone CSV at `file_path`. Two shapes are handled:
   - **Financial statements** (income, balance, cashflow): Wide format where columns are fiscal dates and rows are metrics. Unpivoted via `pandas.melt()` into `(ticker, report_type, metric, period_date, value)`.
   - **Profile metadata**: Already key-value format. Stored with `period_date = NULL`.

3. **Per-Ticker Parquet**: Groups all unpivoted rows by ticker and writes one `data.parquet` per ticker directory.

## Silver Output Schema

### Price (`output/silver/price/date=YYYY-MM-DD/`)

| Column     | Type          | Description             |
|------------|---------------|-------------------------|
| ticker     | VARCHAR       | Stock symbol            |
| open       | DECIMAL(18,6) | Opening price           |
| high       | DECIMAL(18,6) | Day high                |
| low        | DECIMAL(18,6) | Day low                 |
| close      | DECIMAL(18,6) | Closing price           |
| adj_close  | DECIMAL(18,6) | Adjusted close          |
| volume     | BIGINT        | Trading volume          |
| received_at| TIMESTAMP     | Bronze ingestion time   |

The `date` column is encoded in the Hive partition directory name.

### Fundamentals (`output/silver/fundamentals/ticker=XXX/`)

| Column           | Type      | Description                          |
|------------------|-----------|--------------------------------------|
| report_type      | VARCHAR   | e.g. `income_quarterly`, `profile_metadata` |
| metric           | VARCHAR   | Metric name (e.g. `EBITDA`, `sector`)       |
| period_date      | VARCHAR   | Fiscal period date (NULL for profile)       |
| value            | VARCHAR   | Metric value (stored as string)             |
| source_file_path | VARCHAR   | Landing-zone CSV path                       |
| received_at      | TIMESTAMP | Bronze ingestion time                       |

The `ticker` column is encoded in the Hive partition directory name.

### Quality Issues (`silver_quality_issues` DuckDB table)

| Column         | Type      | Description                     |
|----------------|-----------|---------------------------------|
| id             | BIGINT    | Auto-increment primary key      |
| layer          | VARCHAR   | Always `'silver'`               |
| dataset        | VARCHAR   | `'price'` or `'fundamentals'`   |
| ticker         | VARCHAR   | Stock symbol                    |
| business_date  | DATE      | Market date of the violation    |
| rule_name      | VARCHAR   | Which rule was violated         |
| issue_details  | VARCHAR   | Actual values that failed       |
| record_payload | VARCHAR   | Row identifier (`ticker\|date`) |
| logged_at      | TIMESTAMP | When the issue was logged       |

## Idempotency

The pipeline is safe to re-run:

- **Price Parquet**: Target directory is wiped and rewritten each run.
- **Fundamentals Parquet**: Per-ticker files are overwritten in place.
- **Quality Issues**: Previous `dataset='price'` issues are deleted before re-inserting.
- **Deduplication**: Bronze duplicates from re-ingestion are resolved in SQL before any output.

## Usage

```bash
# Full pipeline (price + fundamentals)
python pipeline/elt_pipeline.py

# Price only
python pipeline/elt_pipeline.py --resource price

# Fundamentals only
python pipeline/elt_pipeline.py --resource fundamentals
```

## Querying Silver Data

```sql
-- Price
SELECT * FROM read_parquet('output/silver/price/*/*.parquet', hive_partitioning=true)
WHERE ticker = 'AAPL' ORDER BY date LIMIT 10;

-- Fundamentals
SELECT * FROM read_parquet('output/silver/fundamentals/*/data.parquet', hive_partitioning=true)
WHERE ticker = 'BIGGQ' AND report_type = 'income_quarterly' LIMIT 10;

-- Quality issues
SELECT * FROM silver_quality_issues;
```

## Results (current run)

| Metric                    | Value   |
|---------------------------|---------|
| Bronze price rows         | 615,139 |
| Deduped price rows        | 615,139 |
| Price date partitions     | 753     |
| Unique tickers            | 818     |
| Quality issues logged     | 4       |
| Fundamental index entries | 7       |
| Unpivoted fundamental rows| 658     |
| Tickers with fundamentals | 2       |
| Pipeline runtime          | < 1 sec |

## What Phase 3 Does NOT Cover

These are deferred to Phase 4 (Silver Layer — Parquet + Sentiment):

- PDF text extraction from `raw_transcript_index`
- TextBlob sentiment analysis (polarity, subjectivity)
- `silver_transcript_text` and `silver_transcript_sentiment` outputs
