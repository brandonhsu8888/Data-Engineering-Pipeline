# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NUS MQF (Master of Quantitative Finance) course project — a production-like SPX 500 data pipeline with Medallion architecture (Bronze → Silver → Gold) and a simulated financial data API.

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
├── ARCHIVE/
├── STANDARDS.md
└── CLAUDE.md
```

## Data Architecture

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

## Available Data

All data is already present locally — no downloads needed.

| Data | Path | Scale |
|------|------|-------|
| Price (OHLCV) | `data/price/spx_20yr_ohlcv_data.csv` | 818 tickers, 5284 trading days (2004-2024) |
| Fundamentals | `data/fundamental/SPX_Fundamental_History/*.csv` | 5726 files, annual + quarterly |
| PDF Transcripts | `data/transcript/SPX_20yr_PDF_Library_10GB/*.pdf` | 32036 files |
| Tickers | `data/reference/tickers.csv` | 947 entries |

## Running the Pipeline

```bash
# Activate environment
conda activate qf5214_project

# Phase 1: Test DataProvider API
python -c "from pipeline.data_provider import SPXDataProvider; p = SPXDataProvider(); print(p.get_ticker_list()[:5])"

# Phase 2: Run simulator (backfill mode)
python pipeline/simulators/comprehensive_simulator.py --mode backfill --start 2004-01-02 --end 2024-12-30

# Phase 3: Ingestion engine (run in separate terminal)
python pipeline/ingestion_engine.py

# Phase 4: ELT transform
python pipeline/elt_pipeline.py

# Phase 5: Query Gold layer
duckdb duckdb/spx_analytics.duckdb -c "SELECT * FROM gold.v_sentiment_price_analysis LIMIT 10;"

# Phase 6: Streamlit dashboard
streamlit run pipeline/dashboard.py
```

## Documentation

| Document | Purpose |
|----------|---------|
| `docs/superpowers/specs/` | **Technical design (source of truth)** — architecture, API, schema, pipeline logic |
| `STANDARDS.md` | Development standards — code style, naming, testing, logging |
| `CLAUDE.md` | Quick reference — project structure, commands, dependencies |

**Technical Design**: `docs/superpowers/specs/2026-03-20-spx-data-pipeline-design.md`

## Dependencies

Core packages (conda environment `qf5214_project` or `data_analysis`):
- `pandas`, `duckdb`, `sqlalchemy`, `watchdog`, `streamlit`
- `pypdf` or `pdfminer` for PDF text extraction
- `textblob` for sentiment analysis

## Known Issues

`stream_simulator.py` has a performance bug: reads all 4900+ fundamental file headers every day. Use `comprehensive_simulator.py` instead.
