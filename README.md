# SPX 500 Data Pipeline

NUS MQF (Master of Quantitative Finance) QF5214 Data Engineering Course Project.

A production-like SPX 500 data pipeline with Medallion architecture (Bronze → Silver → Gold) and a simulated financial data API.

## Architecture

```
Existing Dataset (CSV/PDF)
    ↓
DataProvider API (simulates Yahoo Finance)
    ↓
Bronze Layer (OLTP - watchdog ingestion)
    ↓
ELT Pipeline (Bronze → Silver transform)
    ↓
Silver Layer (clean Parquet + sentiment)
    ↓
Gold Layer (OLAP views + Streamlit)
```

## Data Scale

| Data | Path | Scale |
|------|------|-------|
| Price (OHLCV) | `data/price/spx_20yr_ohlcv_data.csv` | 818 tickers, 5284 trading days (2004-2024) |
| Fundamentals | `data/fundamental/SPX_Fundamental_History/` | 5726 files, annual + quarterly |
| PDF Transcripts | `data/transcript/SPX_20yr_PDF_Library_10GB/` | 32036 files |
| Tickers | `data/reference/tickers.csv` | 947 entries |

## Project Structure

```
5214_Project_SPX_Index_Raw_Data/
├── data/                              # Raw data (read-only)
│   ├── price/spx_20yr_ohlcv_data.csv
│   ├── fundamental/SPX_Fundamental_History/
│   ├── transcript/SPX_20yr_PDF_Library_10GB/
│   └── reference/tickers.csv
├── pipeline/                           # Pipeline source code
│   ├── data_provider.py               # Simulated financial API
│   ├── ingestion_engine.py            # Bronze layer (watchdog-based)
│   ├── elt_pipeline.py                # Bronze → Silver transform
│   └── simulators/                   # Virtual clock simulators
├── output/
│   ├── landing_zone/                  # Simulator output
│   │   ├── prices/price_YYYY-MM-DD.csv
│   │   ├── fundamentals/YYYY-MM-DD/
│   │   └── transcripts/
│   └── silver/                       # Silver layer Parquet
├── duckdb/                            # Gold layer SQL + DB file
├── docs/superpowers/specs/           # Design documents
├── notebooks/
├── STANDARDS.md
└── README.md
```

## Quick Start

### 1. Activate Environment

```bash
conda activate qf5214_project
```

### 2. Test DataProvider API

```bash
python -c "from pipeline.data_provider import SPXDataProvider; p = SPXDataProvider(); print(p.get_ticker_list()[:5])"
```

### 3. Run Simulator (Backfill Mode)

```bash
python pipeline/simulators/comprehensive_simulator.py --mode backfill --start 2004-01-02 --end 2024-12-30
```

### 4. Run Ingestion Engine (in another terminal)

```bash
python pipeline/ingestion_engine.py --mode watch
```

Or scan mode for one-time backfill:

```bash
python pipeline/ingestion_engine.py --mode scan
```

## Core Components

### DataProvider API (`pipeline/data_provider.py`)

Simulates Yahoo Finance API behavior. All data access goes through this class.

| Method | Returns | Description |
|--------|---------|-------------|
| `get_price(ticker, date)` | DataFrame | OHLCV price data |
| `get_fundamentals(ticker, freq)` | dict | Fundamental data (income, balance, cashflow) |
| `get_transcript(ticker, date)` | bytes | Raw PDF transcript |
| `list_transcripts(ticker, year)` | list | Available transcripts |
| `get_trading_dates(start, end)` | list | Trading dates in range |
| `get_ticker_list()` | list | All ticker symbols |

### Simulator (`pipeline/simulators/comprehensive_simulator.py`)

Virtual clock that drives data generation. Advances through trading dates and emits data to landing zone.

```bash
# Backfill mode (batch historical load)
python simulator.py --mode backfill --start 2004-01-02 --end 2024-12-30

# Realtime mode (continuous with delay)
python simulator.py --mode realtime --start 2024-01-02 --delay 1.0
```

### Ingestion Engine (`pipeline/ingestion_engine.py`)

Watchdog-based monitoring of landing zone. Ingests raw data into Bronze tables.

```bash
# Watch mode (continuous monitoring)
python ingestion_engine.py --mode watch

# Scan mode (one-time backfill)
python ingestion_engine.py --mode scan
```

## Implementation Phases

| Phase | Task | Status |
|-------|------|--------|
| 1 | DataProvider API | ✅ Completed |
| 2 | Bronze Layer (Ingestion Engine) | ✅ Completed |
| 3 | ELT Pipeline (Transform Jobs) | ✅ Completed |
| 4 | Silver Layer (Parquet + Sentiment) | ✅ Completed (by Phase 3 ELT) |
| 5 | Gold Layer (OLAP Views) | 🔜 Pending |
| 6 | Streamlit Dashboard | 🔜 Pending |

### ELT Pipeline (`pipeline/elt_pipeline.py`)

Bronze → Silver transforms:

```bash
# Run all transforms
python pipeline/elt_pipeline.py

# Run specific transform
python pipeline/elt_pipeline.py --resource price
python pipeline/elt_pipeline.py --resource fundamentals
python pipeline/elt_pipeline.py --resource transcripts
python pipeline/elt_pipeline.py --resource sentiment
```

| Transform | Source | Output |
|-----------|--------|--------|
| `price` | raw_price_stream | output/silver/price/date=YYYY-MM-DD/*.parquet |
| `fundamentals` | raw_fundamental_index | output/silver/fundamentals/ticker=XXX/data.parquet |
| `transcripts` | raw_transcript_index | output/silver/transcript_text/ticker=XXX/date=YYYY-MM-DD/content.txt |
| `sentiment` | transcript_text | output/silver/transcript_sentiment/ticker=XXX/date=YYYY-MM-DD/sentiment.parquet |

## Technology Stack

| Component | Technology |
|-----------|------------|
| Data Access | Python class (DataProvider) |
| Ingestion | pandas + DuckDB + watchdog |
| Database | DuckDB (OLAP optimized) |
| ELT | DuckDB SQL + Python |
| Sentiment | TextBlob |
| Monitoring | Streamlit |
| Environment | conda (qf5214_project) |

## Documentation

| Document | Purpose |
|----------|---------|
| `docs/superpowers/specs/` | Technical design (source of truth) |
| `STANDARDS.md` | Development standards |

## License

Course project - NUS MQF QF5214
