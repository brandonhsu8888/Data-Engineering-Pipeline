-- ============================================================
-- Person B: Text & Financial Fusion Gold Views
-- ============================================================

-- 3. v_fundamental_snapshot
-- Latest financials per ticker: ticker, latest_report_date, revenue, net_income, assets, liabilities
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

-- 4. v_sentiment_price_view
-- Transcript sentiment + price reaction
-- Uses ASOF join: if transcript falls on non-trading day, match to nearest prior trading day
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
