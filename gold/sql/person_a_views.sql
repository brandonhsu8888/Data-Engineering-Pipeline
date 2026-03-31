-- ============================================================
-- Person A: Price & Profile Gold Views
-- ============================================================

-- 1. v_market_daily_summary
-- Per-day market aggregate: trade_date, number_of_tickers, avg_close, avg_return, total_volume
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

-- 2. v_ticker_profile
-- Latest snapshot per ticker: ticker, company_name, sector, latest_close, latest_volume, latest_trade_date
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
