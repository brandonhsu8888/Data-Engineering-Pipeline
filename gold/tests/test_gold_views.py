"""
Phase 5 Gold Layer — View Validation Tests

Run: python tests/test_gold_views.py
Requires: DuckDB with Silver data loaded and Gold views created.
"""

import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)

DB_PATH = Path(__file__).resolve().parent.parent.parent / "duckdb" / "spx_analytics.duckdb"

def test_view_exists(con, view_name: str) -> bool:
    """Check if a view exists in the database."""
    result = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{view_name}'"
    ).fetchone()[0]
    return result > 0

def test_view_not_empty(con, view_name: str) -> int:
    """Check view returns rows. Returns row count."""
    count = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
    return count

def test_view_columns(con, view_name: str, expected_cols: list) -> list:
    """Check view has expected columns. Returns missing columns."""
    actual = [row[0] for row in con.execute(f"DESCRIBE {view_name}").fetchall()]
    missing = [c for c in expected_cols if c not in actual]
    return missing

def main():
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        sys.exit(1)

    con = duckdb.connect(str(DB_PATH), read_only=True)
    
    views = {
        "v_market_daily_summary": ["trade_date", "number_of_tickers", "avg_close", "avg_return", "total_volume"],
        "v_ticker_profile": ["ticker", "company_name", "sector", "latest_close", "latest_volume", "latest_trade_date"],
        "v_fundamental_snapshot": ["ticker", "latest_report_date", "revenue", "net_income", "assets", "liabilities"],
        "v_sentiment_price_view": ["ticker", "transcript_date", "sentiment_score", "close_on_event_date", "next_1d_return", "next_5d_return"],
    }

    passed = failed = 0

    for view_name, expected_cols in views.items():
        print(f"\n--- {view_name} ---")

        # Test 1: exists
        if test_view_exists(con, view_name):
            print(f"  [PASS] View exists")
            passed += 1
        else:
            print(f"  [FAIL] View does NOT exist")
            failed += 1
            continue

        # Test 2: not empty
        count = test_view_not_empty(con, view_name)
        if count > 0:
            print(f"  [PASS] Has {count:,} rows")
            passed += 1
        else:
            print(f"  [WARN] View is empty (0 rows)")
            failed += 1

        # Test 3: columns
        missing = test_view_columns(con, view_name, expected_cols)
        if not missing:
            print(f"  [PASS] All expected columns present")
            passed += 1
        else:
            print(f"  [FAIL] Missing columns: {missing}")
            failed += 1

    con.close()

    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
