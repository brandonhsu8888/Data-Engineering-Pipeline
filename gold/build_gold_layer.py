"""
Phase 5 — Standalone Gold Layer Builder
Loads Silver Parquet → DuckDB tables → Creates Gold views.

This script ONLY handles the Gold phase. For the full pipeline
(simulator → ingestion → ELT → gold), use run_pipeline.py instead.

Usage:
  python build_gold_layer.py
  python build_gold_layer.py --db-path /custom/path/to/spx_analytics.duckdb
  python build_gold_layer.py --verify-only  (just check if views exist and have data)
  python build_gold_layer.py --sql-file /custom/path/to/create_gold_views.sql
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Path auto-detection
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent          # gold/
PROJECT_ROOT = SCRIPT_DIR.parent                      # repo root
DEFAULT_DB_PATH = PROJECT_ROOT / "duckdb" / "spx_analytics.duckdb"
DEFAULT_GOLD_SQL = SCRIPT_DIR / "sql" / "create_gold_views.sql"

# Silver layer tables to refresh (dropped before re-creation)
SILVER_TABLES = ("silver_price", "silver_fundamentals", "silver_sentiment")

# Gold views we expect after SQL execution
GOLD_VIEWS = (
    "v_market_daily_summary",
    "v_ticker_profile",
    "v_fundamental_snapshot",
    "v_sentiment_price_view",
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("build_gold_layer")


# ===========================================================================
# Pretty-print helpers
# ===========================================================================

def _print_summary_table(rows: list[tuple[str, str, str]]):
    """Print a Unicode box-drawing summary table.

    Each row is (view_name, row_count_str, status_str).
    """
    # Column widths (minimum)
    col_w = [24, 10, 21]
    # Adjust to actual content
    for name, cnt, status in rows:
        col_w[0] = max(col_w[0], len(name) + 2)
        col_w[1] = max(col_w[1], len(cnt) + 2)
        col_w[2] = max(col_w[2], len(status) + 2)

    def _row_line(left, mid, right, fill="═"):
        parts = [fill * w for w in col_w]
        return left + mid.join(parts) + right

    header_names = ("View", "Rows", "Status")
    # Top border
    print(_row_line("╔", "╦", "╗"))
    # Header
    print(
        "║"
        + "║".join(
            f" {h:<{col_w[i] - 1}}" for i, h in enumerate(header_names)
        )
        + "║"
    )
    # Header-body separator
    print(_row_line("╠", "╬", "╣"))
    # Data rows
    for name, cnt, status in rows:
        print(
            "║"
            + f" {name:<{col_w[0] - 1}}"
            + "║"
            + f" {cnt:>{col_w[1] - 1}}"
            + "║"
            + f" {status:<{col_w[2] - 1}}"
            + "║"
        )
    # Bottom border
    print(_row_line("╚", "╩", "╝"))


# ===========================================================================
# Core logic
# ===========================================================================

def _check_silver_parquet_exists() -> dict[str, int]:
    """Return a dict of {label: file_count} for each Silver dataset."""
    silver_dir = PROJECT_ROOT / "output" / "silver"
    result = {}
    for label, subdir in [
        ("price", "price"),
        ("fundamentals", "fundamentals"),
        ("sentiment", "transcript_sentiment"),
    ]:
        d = silver_dir / subdir
        if d.exists():
            count = len(list(d.rglob("*.parquet")))
            result[label] = count
        else:
            result[label] = 0
    return result


def _connect_duckdb(db_path: Path):
    """Import duckdb and return a connection. Exits on failure."""
    try:
        import duckdb
    except ImportError:
        logger.error(
            "duckdb package not installed. Run:  pip install duckdb"
        )
        sys.exit(1)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Connecting to DuckDB: {db_path}")
    return duckdb.connect(str(db_path))


def _drop_silver_tables(con) -> None:
    """Drop existing silver_* tables so they are refreshed from Parquet."""
    for table in SILVER_TABLES:
        try:
            con.execute(f"DROP TABLE IF EXISTS {table}")
            logger.info(f"  Dropped table (if existed): {table}")
        except Exception as exc:
            logger.warning(f"  Could not drop {table}: {exc}")


def _execute_gold_sql(con, sql_path: Path) -> dict[str, str]:
    """Execute the Gold SQL file statement-by-statement.

    Returns a dict of {statement_summary: 'OK' | error_message}.
    """
    sql_text = sql_path.read_text(encoding="utf-8")
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    results: dict[str, str] = {}

    for i, stmt in enumerate(statements):
        # Skip blocks that are purely comments
        meaningful_lines = [
            ln for ln in stmt.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
        ]
        if not meaningful_lines:
            continue

        # Extract a short description from the first meaningful SQL line
        desc = meaningful_lines[0].strip()[:80]

        try:
            con.execute(stmt)
            results[desc] = "OK"
            logger.info(f"  [{i + 1}] OK  — {desc}")
        except Exception as exc:
            err_msg = str(exc).splitlines()[0][:120]
            results[desc] = err_msg
            logger.error(f"  [{i + 1}] FAIL — {desc}")
            logger.error(f"         {err_msg}")

    return results


def _verify_views(con) -> list[tuple[str, int | None, str]]:
    """Query each Gold view for row count.

    Returns list of (view_name, row_count_or_None, status_string).
    """
    view_results = []
    for vname in GOLD_VIEWS:
        try:
            row = con.execute(f"SELECT COUNT(*) FROM {vname}").fetchone()
            count = row[0] if row else 0
            status = "✓ OK" if count > 0 else "⚠ EMPTY"
            view_results.append((vname, count, status))
            logger.info(f"  {vname}: {count:,} rows")
        except Exception as exc:
            err_short = str(exc).splitlines()[0][:60]
            view_results.append((vname, None, f"✗ {err_short}"))
            logger.error(f"  {vname}: FAILED — {err_short}")
    return view_results


def _print_sample_data(con, view_results: list[tuple[str, int | None, str]]):
    """Print a few sample rows from each successfully queried view."""
    for vname, count, status in view_results:
        if count is None or count == 0:
            continue
        try:
            sample = con.execute(f"SELECT * FROM {vname} LIMIT 3").fetchdf()
            logger.info(f"\n  Sample from {vname}:")
            for line in sample.to_string(index=False).splitlines():
                logger.info(f"    {line}")
        except Exception:
            pass  # non-critical


# ===========================================================================
# Entry points
# ===========================================================================

def build_gold(db_path: Path, sql_path: Path) -> bool:
    """Full Gold build: drop silver tables → execute SQL → verify views."""
    t0 = time.perf_counter()

    # 1. Check Silver Parquet availability
    logger.info("Checking Silver Parquet files...")
    parquet_counts = _check_silver_parquet_exists()
    for label, count in parquet_counts.items():
        if count == 0:
            logger.warning(f"  Silver/{label}: NO Parquet files found")
        else:
            logger.info(f"  Silver/{label}: {count} Parquet file(s)")

    if all(c == 0 for c in parquet_counts.values()):
        logger.error(
            "No Silver Parquet data found at all. "
            "Run the ELT phase first (python run_pipeline.py --phase elt)."
        )
        return False

    # 2. Validate SQL file
    if not sql_path.exists():
        logger.error(f"Gold SQL file not found: {sql_path}")
        return False
    logger.info(f"Gold SQL file: {sql_path}")

    # 3. Change CWD so relative Parquet paths in SQL resolve correctly
    original_cwd = os.getcwd()
    os.chdir(str(PROJECT_ROOT))
    logger.info(f"Working directory set to: {PROJECT_ROOT}")

    try:
        # 4. Connect to DuckDB
        con = _connect_duckdb(db_path)

        # 5. Drop existing silver tables (force refresh)
        logger.info("Dropping existing Silver tables for refresh...")
        _drop_silver_tables(con)

        # 6. Execute Gold SQL
        logger.info(f"Executing Gold SQL ({sql_path.name})...")
        exec_results = _execute_gold_sql(con, sql_path)

        failures = [k for k, v in exec_results.items() if v != "OK"]
        if failures:
            logger.warning(
                f"{len(failures)} SQL statement(s) failed — "
                "continuing with verification..."
            )

        # 7. Verify Gold views
        logger.info("Verifying Gold views...")
        view_results = _verify_views(con)

        # 8. Sample data
        _print_sample_data(con, view_results)

        con.close()

    finally:
        os.chdir(original_cwd)

    elapsed = time.perf_counter() - t0

    # 9. Summary table
    print()
    logger.info(f"Gold layer build completed in {elapsed:.1f}s")
    print()
    summary_rows = []
    for vname, count, status in view_results:
        count_str = f"{count:,}" if count is not None else "—"
        summary_rows.append((vname, count_str, status))
    _print_summary_table(summary_rows)
    print()

    all_ok = all(
        count is not None and count > 0
        for _, count, _ in view_results
    )
    return all_ok


def verify_only(db_path: Path) -> bool:
    """Just check if Gold views exist and have data — no rebuild."""
    logger.info("Verify-only mode: checking existing Gold views...")

    if not db_path.exists():
        logger.error(f"DuckDB file not found: {db_path}")
        return False

    # CWD not needed for pure SELECT queries, but set it just in case
    # views reference relative paths (they don't — they query tables).
    con = _connect_duckdb(db_path)
    view_results = _verify_views(con)
    _print_sample_data(con, view_results)
    con.close()

    print()
    summary_rows = []
    for vname, count, status in view_results:
        count_str = f"{count:,}" if count is not None else "—"
        summary_rows.append((vname, count_str, status))
    _print_summary_table(summary_rows)
    print()

    all_ok = all(
        count is not None and count > 0
        for _, count, _ in view_results
    )
    return all_ok


# ===========================================================================
# CLI
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Phase 5 — Standalone Gold Layer Builder",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python build_gold_layer.py                      # build with defaults
  python build_gold_layer.py --verify-only        # just check existing views
  python build_gold_layer.py --db-path D:/my.duckdb
  python build_gold_layer.py --sql-file custom_gold.sql
        """,
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help=f"Path to DuckDB file (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--sql-file",
        type=str,
        default=None,
        help=f"Path to Gold SQL file (default: {DEFAULT_GOLD_SQL})",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify existing Gold views — do not rebuild",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path) if args.db_path else DEFAULT_DB_PATH
    sql_path = Path(args.sql_file) if args.sql_file else DEFAULT_GOLD_SQL

    # Banner
    sep = "=" * 60
    logger.info(sep)
    logger.info("  Phase 5 — Standalone Gold Layer Builder")
    logger.info(sep)
    logger.info(f"  DuckDB:      {db_path}")
    logger.info(f"  Gold SQL:    {sql_path}")
    logger.info(f"  Project root:{PROJECT_ROOT}")
    logger.info(f"  Python:      {sys.executable}")
    logger.info(f"  Mode:        {'verify-only' if args.verify_only else 'full build'}")
    logger.info(sep)
    print()

    if args.verify_only:
        ok = verify_only(db_path)
    else:
        ok = build_gold(db_path, sql_path)

    if ok:
        logger.info("All Gold views are present and populated. ✓")
    else:
        logger.warning(
            "Some Gold views are missing or empty. "
            "Check logs above for details."
        )

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
