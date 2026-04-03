from pathlib import Path
import importlib
import sys

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "duckdb" / "spx_analytics.duckdb"
GOLD_CSV_DIR = PROJECT_ROOT / "data" / "gold"

# CSV mode: used on Streamlit Cloud where DuckDB file is not present
_USE_CSV = not DB_PATH.exists()

if not _USE_CSV:
    # Avoid local `duckdb/` directory shadowing the duckdb package import.
    sys.path = [p for p in sys.path if p not in ("", str(PROJECT_ROOT))]
    duckdb = importlib.import_module("duckdb")

EXPECTED_VIEWS = [
    "v_market_daily_summary",
    "v_ticker_profile",
    "v_fundamental_snapshot",
    "v_sentiment_price_view",
]


@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)


def check_views(conn) -> tuple[list[str], list[str]]:
    rows = conn.execute(
        """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'main'
        """
    ).fetchall()
    existing = {r[0] for r in rows}
    available = [v for v in EXPECTED_VIEWS if v in existing]
    missing = [v for v in EXPECTED_VIEWS if v not in existing]
    return available, missing


@st.cache_data(ttl=300)
def load_market_daily() -> pd.DataFrame:
    if _USE_CSV:
        return pd.read_csv(GOLD_CSV_DIR / "market_daily.csv", parse_dates=["trade_date"])
    return get_connection().execute(
        "SELECT * FROM v_market_daily_summary ORDER BY trade_date"
    ).df()


@st.cache_data(ttl=300)
def load_ticker_profile() -> pd.DataFrame:
    if _USE_CSV:
        return pd.read_csv(GOLD_CSV_DIR / "ticker_profile.csv")
    return get_connection().execute(
        "SELECT * FROM v_ticker_profile ORDER BY ticker"
    ).df()


@st.cache_data(ttl=300)
def load_fundamental_snapshot() -> pd.DataFrame:
    if _USE_CSV:
        return pd.read_csv(GOLD_CSV_DIR / "fundamental_snapshot.csv")
    return get_connection().execute(
        "SELECT * FROM v_fundamental_snapshot ORDER BY ticker"
    ).df()


@st.cache_data(ttl=300)
def load_sentiment_price(ticker: str | None, row_limit: int) -> pd.DataFrame:
    if _USE_CSV:
        df = pd.read_csv(GOLD_CSV_DIR / "sentiment_price.csv", parse_dates=["transcript_date"])
        if ticker:
            df = df[df["ticker"] == ticker]
        return df.sort_values("transcript_date", ascending=False).head(row_limit)
    conn = get_connection()
    if ticker:
        return conn.execute(
            "SELECT * FROM v_sentiment_price_view WHERE ticker = ? ORDER BY transcript_date DESC LIMIT ?",
            [ticker, row_limit],
        ).df()
    return conn.execute(
        "SELECT * FROM v_sentiment_price_view ORDER BY transcript_date DESC LIMIT ?",
        [row_limit],
    ).df()


def render_overview(
    market_daily: pd.DataFrame,
    ticker_profile: pd.DataFrame,
    fundamental_snapshot: pd.DataFrame,
    sentiment_price: pd.DataFrame,
) -> None:
    st.subheader("Pipeline Overview")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Trading Days", f"{len(market_daily):,}")
    c2.metric("Tickers", f"{ticker_profile['ticker'].nunique():,}")
    c3.metric(
        "Fundamental Snapshots",
        f"{fundamental_snapshot['ticker'].nunique():,}",
    )
    c4.metric("Sentiment Records", f"{len(sentiment_price):,}")

    st.markdown("---")
    st.markdown("#### Market Trend")
    chart_df = market_daily.copy()
    chart_df["trade_date"] = pd.to_datetime(chart_df["trade_date"])
    st.line_chart(chart_df.set_index("trade_date")["avg_close"])

    st.markdown("#### Average Daily Return")
    st.line_chart(chart_df.set_index("trade_date")["avg_return"])


def render_market_daily(market_daily: pd.DataFrame) -> None:
    st.subheader("v_market_daily_summary")
    st.dataframe(market_daily, use_container_width=True, hide_index=True)


def render_ticker_profile(ticker_profile: pd.DataFrame, selected_ticker: str | None) -> None:
    st.subheader("v_ticker_profile")
    df = ticker_profile
    if selected_ticker:
        df = df[df["ticker"] == selected_ticker]
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown("#### Sector Distribution (Top 15)")
    sector_counts = (
        ticker_profile["sector"]
        .fillna("Unknown")
        .value_counts()
        .head(15)
        .rename_axis("sector")
        .reset_index(name="count")
    )
    st.bar_chart(sector_counts.set_index("sector")["count"])


def render_fundamental_snapshot(
    fundamental_snapshot: pd.DataFrame, selected_ticker: str | None
) -> None:
    st.subheader("v_fundamental_snapshot")
    df = fundamental_snapshot
    if selected_ticker:
        df = df[df["ticker"] == selected_ticker]
    st.dataframe(df, use_container_width=True, hide_index=True)


def render_sentiment_price(sentiment_price: pd.DataFrame) -> None:
    st.subheader("v_sentiment_price_view")
    st.dataframe(sentiment_price, use_container_width=True, hide_index=True)

    if not sentiment_price.empty:
        st.markdown("#### Sentiment vs 1-Day Return")
        corr_df = sentiment_price[["sentiment_score", "next_1d_return"]].dropna()
        if not corr_df.empty:
            st.scatter_chart(corr_df, x="sentiment_score", y="next_1d_return")
        else:
            st.info("No non-null pairs for sentiment and 1-day return.")


def main() -> None:
    st.set_page_config(page_title="SPX Gold Dashboard", layout="wide")
    st.title("SPX 500 Data Pipeline - Phase 6 Dashboard")
    st.caption("Source: DuckDB Gold Views" if not _USE_CSV else "Source: CSV (Streamlit Cloud)")

    if _USE_CSV:
        # Cloud mode: load directly from CSV files
        available_views = EXPECTED_VIEWS
    else:
        conn = get_connection()
        available_views, missing_views = check_views(conn)
        if missing_views:
            st.warning(
                "Missing Gold views: "
                + ", ".join(missing_views)
                + ". Run `python gold/build_gold_layer.py` first."
            )
        if not available_views:
            st.stop()

    ticker_profile = load_ticker_profile() if "v_ticker_profile" in available_views else pd.DataFrame()
    ticker_list = ticker_profile["ticker"].dropna().sort_values().unique().tolist() if not ticker_profile.empty else []

    st.sidebar.header("Controls")
    page = st.sidebar.selectbox(
        "Page",
        [
            "Overview",
            "Market Daily Summary",
            "Ticker Profile",
            "Fundamental Snapshot",
            "Sentiment Price View",
        ],
    )
    ticker_option = st.sidebar.selectbox("Ticker (optional)", ["All"] + ticker_list)
    selected_ticker = None if ticker_option == "All" else ticker_option
    row_limit = st.sidebar.slider("Sentiment row limit", 100, 20000, 2000, step=100)

    market_daily = load_market_daily() if "v_market_daily_summary" in available_views else pd.DataFrame()
    fundamental_snapshot = (
        load_fundamental_snapshot() if "v_fundamental_snapshot" in available_views else pd.DataFrame()
    )
    sentiment_price = (
        load_sentiment_price(selected_ticker, row_limit)
        if "v_sentiment_price_view" in available_views
        else pd.DataFrame()
    )

    if page == "Overview":
        render_overview(market_daily, ticker_profile, fundamental_snapshot, sentiment_price)
    elif page == "Market Daily Summary":
        render_market_daily(market_daily)
    elif page == "Ticker Profile":
        render_ticker_profile(ticker_profile, selected_ticker)
    elif page == "Fundamental Snapshot":
        render_fundamental_snapshot(fundamental_snapshot, selected_ticker)
    elif page == "Sentiment Price View":
        render_sentiment_price(sentiment_price)


if __name__ == "__main__":
    main()
