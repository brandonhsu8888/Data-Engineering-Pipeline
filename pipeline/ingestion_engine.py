"""
Ingestion Engine - Bronze Layer (OLTP)

Watchdog-based monitoring of landing_zone, ingests raw data into Bronze tables.

Design: docs/superpowers/specs/2026-03-20-spx-data-pipeline-design.md
"""

import hashlib
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import duckdb
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer

# Configuration
DATA_DIR = Path(__file__).parent.parent
LANDING_ZONE = DATA_DIR / "output" / "landing_zone"
DB_PATH = DATA_DIR / "duckdb" / "spx_analytics.duckdb"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ingestion_engine")


class IngestionEngine:
    """
    Watchdog-based ingestion engine for Bronze layer.

    Monitors landing_zone directories and ingests new files into Bronze tables:
    - prices/*.csv → raw_price_stream
    - fundamentals/YYYY-MM-DD/*.csv → raw_fundamental_index
    - transcripts/*.pdf → raw_transcript_index
    """

    def __init__(self, db_path: str = str(DB_PATH)):
        self.db_path = db_path
        self._con: Optional[duckdb.DuckDBPyConnection] = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self._con is None:
            self._con = duckdb.connect(self.db_path)
        return self._con

    def close(self):
        """Close database connection."""
        if self._con:
            self._con.close()
            self._con = None

    def _compute_file_hash(self, filepath: Path) -> str:
        """Compute MD5 hash of file."""
        md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def _log_audit(
        self,
        source: str,
        ticker: str,
        market_date: str,
        file_hash: str,
        status: str = "SUCCESS",
        error_message: Optional[str] = None,
    ):
        """Log ingestion to audit table."""
        con = self._get_connection()
        con.execute(
            """
            INSERT INTO ingestion_audit (source, ticker, market_date, file_hash, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [source, ticker, market_date, file_hash, status, error_message],
        )

    def ingest_price_file(self, filepath: Path) -> int:
        """
        Ingest a price CSV file into raw_price_stream.

        File format: price_YYYY-MM-DD.csv
        Expected columns: Date, Ticker, Open, High, Low, Close, Adj Close, Volume
        """
        filename = filepath.stem  # e.g., "price_2024-01-15"
        try:
            market_date = filename.replace("price_", "")

            # Read and validate CSV
            df = pd.read_csv(filepath)

            # Compute file hash
            file_hash = self._compute_file_hash(filepath)

            # Insert rows
            con = self._get_connection()
            rows_inserted = 0
            for _, row in df.iterrows():
                try:
                    # Handle NaN values by converting to None (NULL in SQL)
                    def safe_float(val):
                        import math
                        if val is None or (isinstance(val, float) and math.isnan(val)):
                            return None
                        return val

                    def safe_int(val):
                        import math
                        if val is None or (isinstance(val, float) and math.isnan(val)):
                            return None
                        return int(val) if val is not None else None

                    con.execute(
                        """
                        INSERT INTO raw_price_stream (ticker, date, open, high, low, close, adj_close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            row["Ticker"],
                            market_date,
                            safe_float(row.get("Open")),
                            safe_float(row.get("High")),
                            safe_float(row.get("Low")),
                            safe_float(row.get("Close")),
                            safe_float(row.get("Adj Close")),
                            safe_int(row.get("Volume")),
                        ],
                    )
                    rows_inserted += 1
                except Exception as e:
                    logger.warning(f"Failed to insert row for {row.get('Ticker')}: {e}")

            # Log audit
            self._log_audit("price", df["Ticker"].iloc[0] if len(df) > 0 else "UNKNOWN", market_date, file_hash, "SUCCESS")
            logger.info(f"Ingested {rows_inserted} price records from {filename}")
            return rows_inserted

        except Exception as e:
            logger.error(f"Failed to ingest price file {filepath}: {e}")
            self._log_audit("price", "UNKNOWN", filename, "", "FAILED", str(e))
            return 0

    def ingest_fundamental_file(self, filepath: Path, dated_dir: str) -> int:
        """
        Ingest a fundamental CSV file into raw_fundamental_index.

        File format: TICKER_type_freq.csv (e.g., AAPL_income_quarterly.csv)
        Path format: fundamentals/YYYY-MM-DD/TICKER_type_freq.csv
        """
        try:
            filename = filepath.stem  # e.g., "AAPL_income_quarterly"
            parts = filename.rsplit("_", 2)
            if len(parts) != 3:
                logger.warning(f"Unexpected fundamental filename format: {filename}")
                return 0

            ticker, report_type, freq = parts
            file_hash = self._compute_file_hash(filepath)

            # Extract fiscal date from first row's second column (first date) in the CSV
            # Format: ,2025-12-31,2025-09-30,...
            with open(filepath, "r") as f:
                first_line = f.readline().strip()
                if first_line:
                    cols = first_line.split(",")
                    # First column is empty (metric name), second is first date
                    fiscal_date = cols[1] if len(cols) > 1 else dated_dir
                    if fiscal_date and not fiscal_date.startswith("0"):
                        try:
                            datetime.strptime(fiscal_date, "%Y-%m-%d")
                        except ValueError:
                            fiscal_date = dated_dir  # fallback to directory date
                else:
                    fiscal_date = dated_dir

            con = self._get_connection()
            con.execute(
                """
                INSERT INTO raw_fundamental_index (ticker, report_type, period, market_date, file_path)
                VALUES (?, ?, ?, ?, ?)
                """,
                [ticker, report_type, freq, dated_dir, str(filepath)],
            )

            self._log_audit("fundamental", ticker, dated_dir, file_hash, "SUCCESS")
            logger.info(f"Ingested fundamental index: {filename}")
            return 1

        except Exception as e:
            logger.error(f"Failed to ingest fundamental file {filepath}: {e}")
            self._log_audit("fundamental", filepath.stem, dated_dir, "", "FAILED", str(e))
            return 0

    def ingest_transcript_file(self, filepath: Path) -> int:
        """
        Ingest a transcript PDF into raw_transcript_index.

        File format: TICKER_YYYY-MM-DD.pdf
        """
        try:
            filename = filepath.stem  # e.g., "AAPL_2024-02-01"
            parts = filename.rsplit("_", 1)
            if len(parts) != 2:
                logger.warning(f"Unexpected transcript filename format: {filename}")
                return 0

            ticker, event_date = parts
            file_hash = self._compute_file_hash(filepath)

            con = self._get_connection()
            con.execute(
                """
                INSERT INTO raw_transcript_index (ticker, event_date, file_path)
                VALUES (?, ?, ?)
                """,
                [ticker, event_date, str(filepath)],
            )

            self._log_audit("transcript", ticker, event_date, file_hash, "SUCCESS")
            logger.info(f"Ingested transcript: {filename}")
            return 1

        except Exception as e:
            logger.error(f"Failed to ingest transcript file {filepath}: {e}")
            self._log_audit("transcript", filepath.stem, "", "", "FAILED", str(e))
            return 0

    def scan_and_ingest(self):
        """
        Scan landing_zone and ingest all existing files.
        Used for backfill or catch-up on startup.
        """
        logger.info("Scanning landing_zone for existing files...")

        prices_dir = LANDING_ZONE / "prices"
        fundamentals_dir = LANDING_ZONE / "fundamentals"
        transcripts_dir = LANDING_ZONE / "transcripts"

        total_ingested = 0

        # Ingest price files
        if prices_dir.exists():
            for csv_file in prices_dir.glob("price_*.csv"):
                if csv_file.exists():  # Skip if file was deleted during scan
                    total_ingested += self.ingest_price_file(csv_file)

        # Ingest fundamental files
        if fundamentals_dir.exists():
            for dated_dir in fundamentals_dir.iterdir():
                if dated_dir.is_dir():
                    for csv_file in dated_dir.glob("*.csv"):
                        if csv_file.exists():  # Skip if file was deleted during scan
                            total_ingested += self.ingest_fundamental_file(csv_file, dated_dir.name)

        # Ingest transcript files
        if transcripts_dir.exists():
            for pdf_file in transcripts_dir.glob("*.pdf"):
                if pdf_file.exists():  # Skip if file was deleted during scan
                    total_ingested += self.ingest_transcript_file(pdf_file)

        logger.info(f"Scan complete. Total records ingested: {total_ingested}")
        return total_ingested


class LandingZoneHandler(FileSystemEventHandler):
    """FileSystemEventHandler for landing_zone changes."""

    def __init__(self, engine: IngestionEngine):
        self.engine = engine

    def on_created(self, event: FileSystemEvent):
        """Handle file creation events."""
        if event.is_directory:
            return

        filepath = Path(event.src_path)

        if "prices" in filepath.parts:
            if filepath.suffix == ".csv" and filepath.stem.startswith("price_"):
                self.engine.ingest_price_file(filepath)

        elif "fundamentals" in filepath.parts:
            if filepath.suffix == ".csv":
                # Extract dated directory from path
                parts = filepath.parts
                for i, p in enumerate(parts):
                    if p == "fundamentals" and i + 1 < len(parts):
                        dated_dir = parts[i + 1]
                        self.engine.ingest_fundamental_file(filepath, dated_dir)
                        break

        elif "transcripts" in filepath.parts:
            if filepath.suffix == ".pdf":
                self.engine.ingest_transcript_file(filepath)

    def on_modified(self, event: FileSystemEvent):
        """Handle file modification events (treat as re-ingest)."""
        self.on_created(event)


def run_watchdog(mode: str = "watch", poll_interval: float = 1.0):
    """
    Run the ingestion engine in watchdog mode.

    Args:
        mode: 'watch' (continuous) or 'scan' (one-time backfill)
        poll_interval: Seconds between watchdog polls
    """
    engine = IngestionEngine()

    if mode == "scan":
        engine.scan_and_ingest()
        engine.close()
        return

    # Watch mode
    logger.info(f"Starting ingestion engine in {mode} mode...")
    logger.info(f"Watching: {LANDING_ZONE}")

    # First, do a catch-up scan
    engine.scan_and_ingest()

    # Then start watchdog
    observer = Observer()
    handler = LandingZoneHandler(engine)
    observer.schedule(handler, str(LANDING_ZONE), recursive=True)
    observer.start()

    logger.info("Ingestion engine started. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Shutting down ingestion engine...")
        observer.stop()

    observer.join()
    engine.close()
    logger.info("Ingestion engine stopped.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SPX Data Pipeline - Ingestion Engine")
    parser.add_argument(
        "--mode",
        choices=["watch", "scan"],
        default="watch",
        help="watch: continuous watchdog; scan: one-time backfill",
    )
    parser.add_argument(
        "--poll",
        type=float,
        default=1.0,
        help="Poll interval in seconds (default: 1.0)",
    )
    args = parser.parse_args()

    run_watchdog(mode=args.mode, poll_interval=args.poll)
