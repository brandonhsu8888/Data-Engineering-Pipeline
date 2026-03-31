-- ============================================================
-- Phase 5: Gold Layer (OLAP Views)
-- All Gold views created on top of Silver layer data in DuckDB
-- ============================================================

-- Load Silver Parquet data into DuckDB tables (if not already loaded)
-- Price
CREATE TABLE IF NOT EXISTS silver_price AS
SELECT * FROM read_parquet('output/silver/price/**/*.parquet', hive_partitioning=true);

-- Fundamentals
CREATE TABLE IF NOT EXISTS silver_fundamentals AS
SELECT * FROM read_parquet('output/silver/fundamentals/**/*.parquet', hive_partitioning=true);

-- Sentiment
CREATE TABLE IF NOT EXISTS silver_sentiment AS
SELECT * FROM read_parquet('output/silver/transcript_sentiment/**/*.parquet', hive_partitioning=true);

-- ============================================================
-- Person A Views: Price & Profile
-- ============================================================

-- 1. Market Daily Summary — per-day market aggregate
CREATE OR REPLACE VIEW v_market_daily_summary AS
SELECT
    date AS trade_date,
    COUNT(DISTINCT ticker) AS number_of_tickers,
    ROUND(AVG(close), 4) AS avg_close,
    ROUND(AVG(
        CASE WHEN prev_close IS NOT NULL AND prev_close > 0
             THEN (close - prev_close) / prev_close
             ELSE NULL
        END
    ), 6) AS avg_return,
    SUM(volume) AS total_volume
FROM (
    SELECT
        ticker, date, close, volume,
        LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
    FROM silver_price
) sub
GROUP BY date
ORDER BY date;

-- 2. Ticker Profile — latest snapshot per ticker (with company_name & sector from fundamentals)
CREATE OR REPLACE VIEW v_ticker_profile AS
WITH latest_price AS (
    SELECT
        ticker,
        date AS latest_trade_date,
        close AS latest_close,
        volume AS latest_volume,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
    FROM silver_price
),
profile_data AS (
    SELECT
        ticker,
        COALESCE(
            MAX(CASE WHEN metric = 'longName' THEN value END),
            MAX(CASE WHEN metric = 'shortName' THEN value END),
            MAX(CASE WHEN metric ILIKE '%company%name%' THEN value END)
        ) AS company_name,
        MAX(CASE WHEN metric = 'sector' THEN value END) AS sector
    FROM silver_fundamentals
    WHERE report_type LIKE '%profile%'
    GROUP BY ticker
)
SELECT
    lp.ticker,
    pd.company_name,
    pd.sector,
    lp.latest_close,
    lp.latest_volume,
    lp.latest_trade_date
FROM latest_price lp
LEFT JOIN profile_data pd
    ON lp.ticker = pd.ticker
WHERE lp.rn = 1
ORDER BY lp.ticker;

-- ============================================================
-- Person B Views: Text & Financial Fusion
-- ============================================================

-- 3. Fundamental Snapshot — latest financials per ticker
CREATE OR REPLACE VIEW v_fundamental_snapshot AS
WITH latest_fundamentals AS (
    SELECT
        ticker,
        report_type,
        metric,
        period_date,
        value,
        ROW_NUMBER() OVER (PARTITION BY ticker, report_type, metric ORDER BY period_date DESC) AS rn
    FROM silver_fundamentals
    WHERE period_date IS NOT NULL
)
SELECT
    ticker,
    MAX(period_date) AS latest_report_date,
    MAX(CASE WHEN metric = 'TotalRevenue' AND report_type LIKE '%income%' THEN CAST(value AS DOUBLE) END) AS revenue,
    MAX(CASE WHEN metric = 'NetIncome' AND report_type LIKE '%income%' THEN CAST(value AS DOUBLE) END) AS net_income,
    MAX(CASE WHEN metric = 'TotalAssets' AND report_type LIKE '%balance%' THEN CAST(value AS DOUBLE) END) AS assets,
    MAX(CASE WHEN metric = 'TotalLiabilitiesNetMinorityInterest' AND report_type LIKE '%balance%' THEN CAST(value AS DOUBLE) END) AS liabilities
FROM latest_fundamentals
WHERE rn = 1
GROUP BY ticker
ORDER BY ticker;

-- 4. Sentiment-Price View — transcript sentiment + price reaction
-- Uses ASOF join logic: if transcript falls on non-trading day, match to nearest prior trading day
CREATE OR REPLACE VIEW v_sentiment_price_view AS
WITH price_with_future AS (
    SELECT
        ticker,
        date,
        close,
        LEAD(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS close_next_1d,
        LEAD(close, 5) OVER (PARTITION BY ticker ORDER BY date) AS close_next_5d
    FROM silver_price
),
sentiment_with_date AS (
    SELECT
        ticker,
        event_date,
        sentiment_polarity,
        CAST(event_date AS DATE) AS event_dt
    FROM silver_sentiment
    WHERE sentiment_polarity IS NOT NULL
)
SELECT
    s.ticker,
    s.event_date AS transcript_date,
    s.sentiment_polarity AS sentiment_score,
    p.close AS close_on_event_date,
    ROUND((p.close_next_1d - p.close) / NULLIF(p.close, 0), 6) AS next_1d_return,
    ROUND((p.close_next_5d - p.close) / NULLIF(p.close, 0), 6) AS next_5d_return
FROM sentiment_with_date s
ASOF LEFT JOIN price_with_future p
    ON s.ticker = p.ticker
    AND p.date <= s.event_dt
ORDER BY s.ticker, s.event_date;
