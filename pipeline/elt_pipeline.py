"""
ELT Pipeline - Bronze to Silver Transform

Reads Bronze tables from DuckDB, applies deduplication and quality validation,
writes Silver Parquet files partitioned by date (price) and ticker (fundamentals).

Design: docs/superpowers/specs/2026-03-20-spx-data-pipeline-design.md
"""

import argparse
import logging
import shutil
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration (mirrors ingestion_engine.py layout)
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent.parent
DB_PATH = DATA_DIR / "duckdb" / "spx_analytics.duckdb"
SILVER_DIR = DATA_DIR / "output" / "silver"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("elt_pipeline")

# ---------------------------------------------------------------------------
# Price quality rules  (rule_name, detail_columns, SQL WHERE predicate)
# ---------------------------------------------------------------------------
_PRICE_QUALITY_RULES: list[tuple[str, str, str]] = [
    ("price_positive",      "open",       "open IS NOT NULL AND open <= 0"),
    ("price_positive",      "high",       "high IS NOT NULL AND high <= 0"),
    ("price_positive",      "low",        "low IS NOT NULL AND low <= 0"),
    ("price_positive",      "close",      "close IS NOT NULL AND close <= 0"),
    ("price_positive",      "adj_close",  "adj_close IS NOT NULL AND adj_close <= 0"),
    ("volume_non_negative", "volume",     "volume IS NOT NULL AND volume < 0"),
    ("high_gte_low",        "high,low",   "high IS NOT NULL AND low IS NOT NULL AND high < low"),
    ("high_gte_open",       "high,open",  "high IS NOT NULL AND open IS NOT NULL AND high < open"),
    ("high_gte_close",      "high,close", "high IS NOT NULL AND close IS NOT NULL AND high < close"),
    ("low_lte_open",        "low,open",   "low IS NOT NULL AND open IS NOT NULL AND low > open"),
    ("low_lte_close",       "low,close",  "low IS NOT NULL AND close IS NOT NULL AND low > close"),
]


class ELTPipeline:
    """
    Bronze to Silver ELT pipeline using DuckDB SQL + Python.

    Transforms:
        raw_price_stream      -> output/silver/price/date=YYYY-MM-DD/*.parquet
        raw_fundamental_index -> output/silver/fundamentals/ticker=XXX/data.parquet

    Quality violations are logged to silver_quality_issues but rows are NOT
    dropped from the Silver output.
    """

    def __init__(self, db_path: str = str(DB_PATH)):
        self.db_path = db_path
        self._con: Optional[duckdb.DuckDBPyConnection] = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            self._con = duckdb.connect(self.db_path)
        return self._con

    def close(self):
        if self._con:
            self._con.close()
            self._con = None

    # ------------------------------------------------------------------ #
    #  Silver schema bootstrap                                             #
    # ------------------------------------------------------------------ #

    def ensure_silver_objects(self):
        """Create DuckDB sequences and tables for Silver-layer bookkeeping."""
        con = self._get_connection()
        con.execute("CREATE SEQUENCE IF NOT EXISTS silver_quality_issues_seq")
        con.execute("""
            CREATE TABLE IF NOT EXISTS silver_quality_issues (
                id            BIGINT DEFAULT NEXTVAL('silver_quality_issues_seq') PRIMARY KEY,
                layer         VARCHAR(20),
                dataset       VARCHAR(50),
                ticker        VARCHAR(20),
                business_date DATE,
                rule_name     VARCHAR(100),
                issue_details VARCHAR(500),
                record_payload VARCHAR,
                logged_at     TIMESTAMP DEFAULT NOW()
            )
        """)
        logger.info("Silver DuckDB objects ensured")

    # ------------------------------------------------------------------ #
    #  Price: Bronze -> Silver                                             #
    # ------------------------------------------------------------------ #

    def transform_price(self):
        """raw_price_stream -> Silver price Parquet, partitioned by date.

        1. Deduplicate Bronze rows (latest received_at per ticker+date).
        2. Validate quality rules and log violations.
        3. Export ALL deduped rows to Hive-partitioned Parquet.
        """
        con = self._get_connection()

        raw_count = con.execute("SELECT COUNT(*) FROM raw_price_stream").fetchone()[0]
        if raw_count == 0:
            logger.info("No price data in Bronze — skipping")
            return
        logger.info(f"Bronze raw_price_stream rows: {raw_count:,}")

        # 1. Deduplicate into temp table
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _silver_price AS
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker, date
                           ORDER BY received_at DESC
                       ) AS rn
                FROM raw_price_stream
            )
            SELECT ticker, date, open, high, low, close, adj_close, volume, received_at
            FROM ranked
            WHERE rn = 1
        """)
        deduped_count = con.execute("SELECT COUNT(*) FROM _silver_price").fetchone()[0]
        logger.info(f"Deduped price rows: {deduped_count:,}")

        # 2. Quality validation
        self._validate_price_quality(con)

        # 3. Export to Parquet (Hive-partitioned by date)
        silver_price_dir = SILVER_DIR / "price"
        if silver_price_dir.exists():
            shutil.rmtree(silver_price_dir)
        silver_price_dir.mkdir(parents=True, exist_ok=True)

        con.execute(f"""
            COPY (SELECT * FROM _silver_price)
            TO '{silver_price_dir}'
            (FORMAT PARQUET, PARTITION_BY (date))
        """)

        con.execute("DROP TABLE IF EXISTS _silver_price")
        logger.info(f"Exported {deduped_count:,} price rows -> {silver_price_dir}")

    @staticmethod
    def _validate_price_quality(con: duckdb.DuckDBPyConnection):
        """Check _silver_price against design-doc quality rules; log violations."""
        con.execute("DELETE FROM silver_quality_issues WHERE dataset = 'price'")

        for rule_name, cols, where_clause in _PRICE_QUALITY_RULES:
            detail_expr = " || ' ' || ".join(
                f"'{c}=' || COALESCE(CAST({c} AS VARCHAR), 'NULL')"
                for c in cols.split(",")
            )
            con.execute(f"""
                INSERT INTO silver_quality_issues
                    (layer, dataset, ticker, business_date, rule_name,
                     issue_details, record_payload)
                SELECT 'silver', 'price', ticker, date, '{rule_name}',
                       {detail_expr},
                       ticker || '|' || CAST(date AS VARCHAR)
                FROM _silver_price
                WHERE {where_clause}
            """)

        total = con.execute(
            "SELECT COUNT(*) FROM silver_quality_issues WHERE dataset = 'price'"
        ).fetchone()[0]
        if total:
            logger.warning(f"Logged {total:,} price quality issues")
        else:
            logger.info("No price quality issues found")

    # ------------------------------------------------------------------ #
    #  Fundamentals: Bronze -> Silver                                      #
    # ------------------------------------------------------------------ #

    def transform_fundamentals(self):
        """raw_fundamental_index -> Silver fundamentals Parquet, per ticker.

        1. Deduplicate index (latest received_at per ticker+report_type).
        2. Read each source CSV and unpivot to long format.
        3. Write one Parquet file per ticker.
        """
        con = self._get_connection()

        index_df = con.execute("""
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker, report_type
                           ORDER BY received_at DESC
                       ) AS rn
                FROM raw_fundamental_index
            )
            SELECT ticker, report_type, fiscal_date, file_path, received_at
            FROM ranked
            WHERE rn = 1
        """).fetchdf()

        if index_df.empty:
            logger.info("No fundamental data in Bronze — skipping")
            return
        logger.info(f"Fundamental index entries: {len(index_df):,}")

        frames: list[pd.DataFrame] = []
        processed = errors = 0

        for _, row in index_df.iterrows():
            try:
                long_df = self._unpivot_fundamental(
                    row["file_path"], row["ticker"], row["report_type"],
                )
                if long_df is not None and not long_df.empty:
                    long_df["source_file_path"] = row["file_path"]
                    long_df["received_at"] = row["received_at"]
                    frames.append(long_df)
                    processed += 1
            except Exception as exc:
                logger.warning(f"Failed to process {row['file_path']}: {exc}")
                errors += 1

        if not frames:
            logger.info("No fundamental records produced after unpivot")
            return

        combined = pd.concat(frames, ignore_index=True)
        logger.info(
            f"Unpivoted {len(combined):,} fundamental rows "
            f"from {processed:,} files ({errors} errors)"
        )

        silver_fund_dir = SILVER_DIR / "fundamentals"
        written = 0
        for ticker, grp in combined.groupby("ticker"):
            ticker_dir = silver_fund_dir / f"ticker={ticker}"
            ticker_dir.mkdir(parents=True, exist_ok=True)
            grp.to_parquet(ticker_dir / "data.parquet", index=False)
            written += 1

        logger.info(f"Exported fundamentals for {written:,} tickers -> {silver_fund_dir}")

    @staticmethod
    def _unpivot_fundamental(
        file_path: str, ticker: str, report_type: str,
    ) -> Optional[pd.DataFrame]:
        """Read a wide fundamental CSV and return normalised long-format rows.

        Returns columns: ticker, report_type, metric, period_date, value
        """
        fp = Path(file_path)
        if not fp.exists():
            logger.warning(f"Fundamental file missing: {file_path}")
            return None

        df = pd.read_csv(fp, index_col=0)
        if df.empty:
            return None

        df.index.name = "metric"

        # profile_metadata CSVs are already key-value (metric -> single Value column)
        if "profile" in report_type and len(df.columns) == 1:
            result = df.reset_index()
            result.columns = ["metric", "value"]
            result["value"] = result["value"].apply(
                lambda x: str(x) if pd.notna(x) else None,
            )
            result["period_date"] = None
        else:
            # Financial statements: columns are fiscal-period dates, rows are metrics
            result = (
                df.reset_index()
                .melt(id_vars="metric", var_name="period_date", value_name="value")
            )
            result["value"] = result["value"].apply(
                lambda x: str(x) if pd.notna(x) else None,
            )

        result["ticker"] = ticker
        result["report_type"] = report_type
        return result[["ticker", "report_type", "metric", "period_date", "value"]]

    # ------------------------------------------------------------------ #
    #  Orchestration                                                       #
    # ------------------------------------------------------------------ #

    def run(self, resource: str = "all"):
        """Execute the Phase-3 ELT pipeline."""
        logger.info(f"ELT pipeline starting (resource={resource})")
        try:
            self.ensure_silver_objects()

            if resource in ("all", "price"):
                self.transform_price()

            if resource in ("all", "fundamentals"):
                self.transform_fundamentals()

            logger.info("ELT pipeline completed successfully")
        finally:
            self.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SPX Data Pipeline — ELT (Bronze -> Silver)",
    )
    parser.add_argument(
        "--resource",
        choices=["all", "price", "fundamentals"],
        default="all",
        help="transform target (default: all)",
    )
    args = parser.parse_args()
    ELTPipeline().run(resource=args.resource)
