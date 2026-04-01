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
from textblob import TextBlob

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
        raw_transcript_index  -> output/silver/transcript_text/ticker=XXX/date=YYYY-MM-DD/content.txt
        transcript text       -> output/silver/transcript_sentiment/ticker=XXX/date=YYYY-MM-DD/sentiment.parquet

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

        1. Deduplicate index (latest ingested_at per ticker+report_type).
        2. Read each source CSV and unpivot to long format.
        3. Write one Parquet file per ticker.
        """
        con = self._get_connection()

        index_df = con.execute("""
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker, report_type, period
                           ORDER BY ingested_at DESC
                       ) AS rn
                FROM raw_fundamental_index
            )
            SELECT ticker, report_type, market_date AS fiscal_date, file_path, ingested_at
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
                    long_df["ingested_at"] = row["ingested_at"]
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
        if silver_fund_dir.exists():
            shutil.rmtree(silver_fund_dir)
        silver_fund_dir.mkdir(parents=True, exist_ok=True)

        con.register("_combined_fundamentals_df", combined)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _silver_fundamentals_export AS
            SELECT * FROM _combined_fundamentals_df
        """)
        con.execute(f"""
            COPY _silver_fundamentals_export
            TO '{silver_fund_dir}'
            (FORMAT PARQUET, PARTITION_BY (ticker))
        """)
        con.unregister("_combined_fundamentals_df")
        con.execute("DROP TABLE IF EXISTS _silver_fundamentals_export")

        written = combined["ticker"].nunique()
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
    #  Transcripts: Bronze -> Silver (text extraction)                   #
    # ------------------------------------------------------------------ #

    def transform_transcripts(self):
        """raw_transcript_index -> Silver transcript text files.

        1. Deduplicate index (latest ingested_at per ticker+event_date).
        2. Read each PDF and extract plain text.
        3. Write one .txt file per (ticker, event_date) to transcript_text/.
        """
        con = self._get_connection()

        index_df = con.execute("""
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker, event_date
                           ORDER BY ingested_at DESC
                       ) AS rn
                FROM raw_transcript_index
            )
            SELECT ticker, event_date, file_path, ingested_at
            FROM ranked
            WHERE rn = 1
        """).fetchdf()

        if index_df.empty:
            logger.info("No transcript data in Bronze — skipping")
            return
        logger.info(f"Transcript index entries: {len(index_df):,}")

        silver_text_dir = SILVER_DIR / "transcript_text"
        processed = errors = 0

        for _, row in index_df.iterrows():
            try:
                text = self._extract_pdf_text(row["file_path"])
                if text is None:
                    errors += 1
                    continue

                ticker_dir = silver_text_dir / f"ticker={row['ticker']}"
                ticker_dir.mkdir(parents=True, exist_ok=True)

                # event_date may be a timestamp — normalize to YYYY-MM-DD for the path
                event_date_val = row["event_date"]
                if hasattr(event_date_val, "strftime"):
                    event_date_str = event_date_val.strftime("%Y-%m-%d")
                else:
                    event_date_str = str(event_date_val)[:10]  # "2023-01-26 00:00:00" -> "2023-01-26"

                txt_path = ticker_dir / f"date={event_date_str}" / "content.txt"
                txt_path.parent.mkdir(parents=True, exist_ok=True)
                txt_path.write_text(text, encoding="utf-8")
                processed += 1

            except Exception as exc:
                logger.warning(f"Failed to extract transcript {row['file_path']}: {exc}")
                errors += 1

        logger.info(
            f"Extracted {processed:,} transcript text files ({errors} errors)"
        )

    @staticmethod
    def _extract_pdf_text(pdf_path: str) -> Optional[str]:
        """Extract plain text from a PDF file. Returns None on failure."""
        fp = Path(pdf_path)
        if not fp.exists():
            logger.warning(f"Transcript PDF missing: {pdf_path}")
            return None

        try:
            # Try pypdf first (faster, pure Python)
            try:
                from pypdf import PdfReader
                reader = PdfReader(fp)
                text_parts = []
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                return "\n".join(text_parts) if text_parts else None
            except ImportError:
                pass

            # Fallback to pdfminer
            try:
                from pdfminer.high_level import extract_text
                return extract_text(str(fp))
            except ImportError:
                pass

            logger.warning(f"No PDF library available for {pdf_path}")
            return None

        except Exception as exc:
            logger.warning(f"Failed to extract text from {pdf_path}: {exc}")
            return None

    # ------------------------------------------------------------------ #
    #  Sentiment: Silver transcript_text -> Silver sentiment parquet     #
    # ------------------------------------------------------------------ #

    def transform_sentiment(self):
        """Compute sentiment from silver_transcript_text -> silver_transcript_sentiment.

        Reads all content.txt files under transcript_text/, computes polarity
        and subjectivity via TextBlob, writes sentiment.parquet per (ticker, date).
        """
        silver_text_dir = SILVER_DIR / "transcript_text"
        if not silver_text_dir.exists():
            logger.info("No transcript_text directory — skipping sentiment")
            return

        silver_sentiment_dir = SILVER_DIR / "transcript_sentiment"
        if silver_sentiment_dir.exists():
            shutil.rmtree(silver_sentiment_dir)
        silver_sentiment_dir.mkdir(parents=True, exist_ok=True)
        processed = skipped = 0
        records: list[dict] = []

        for ticker_dir in silver_text_dir.iterdir():
            if not ticker_dir.is_dir() or not ticker_dir.name.startswith("ticker="):
                continue
            ticker = ticker_dir.name.replace("ticker=", "")

            for date_dir in ticker_dir.iterdir():
                if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                    continue
                event_date = date_dir.name.replace("date=", "")
                txt_path = date_dir / "content.txt"

                if not txt_path.exists():
                    skipped += 1
                    continue

                try:
                    text = txt_path.read_text(encoding="utf-8")
                    if not text or not text.strip():
                        # Empty text cannot be analyzed — store NULL per spec
                        logger.warning(f"Empty transcript text for {txt_path}")
                        polarity = None
                        subjectivity = None
                    else:
                        blob = TextBlob(text)
                        polarity = blob.sentiment.polarity
                        subjectivity = blob.sentiment.subjectivity
                except Exception as exc:
                    logger.warning(f"TextBlob failed for {txt_path}: {exc}")
                    polarity = None
                    subjectivity = None

                # Keep NULL sentiment if TextBlob failed per spec.
                records.append({
                    "ticker": ticker,
                    "event_date": event_date,
                    "sentiment_polarity": polarity,
                    "sentiment_subjectivity": subjectivity,
                })
                processed += 1

        if records:
            con = self._get_connection()
            sentiment_df = pd.DataFrame(records)
            con.register("_sentiment_df", sentiment_df)
            con.execute("""
                CREATE OR REPLACE TEMP TABLE _silver_sentiment_export AS
                SELECT * FROM _sentiment_df
            """)
            con.execute(f"""
                COPY _silver_sentiment_export
                TO '{silver_sentiment_dir}'
                (FORMAT PARQUET, PARTITION_BY (ticker, event_date))
            """)
            con.unregister("_sentiment_df")
            con.execute("DROP TABLE IF EXISTS _silver_sentiment_export")

        logger.info(
            f"Computed sentiment for {processed:,} transcripts ({skipped} skipped)"
        )

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

            if resource in ("all", "transcripts"):
                self.transform_transcripts()

            if resource in ("all", "sentiment"):
                self.transform_sentiment()

            logger.info("ELT pipeline completed successfully")
        finally:
            self.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SPX Data Pipeline — ELT (Bronze -> Silver)",
    )
    parser.add_argument(
        "--resource",
        choices=["all", "price", "fundamentals", "transcripts", "sentiment"],
        default="all",
        help="transform target (default: all)",
    )
    args = parser.parse_args()
    ELTPipeline().run(resource=args.resource)
