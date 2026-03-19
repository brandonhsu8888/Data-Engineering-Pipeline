"""
Comprehensive Simulator - Virtual Clock Data Generator

Simulates virtual clock advancing through trading dates, calling DataProvider
and emitting data to landing_zone for ingestion by IngestionEngine.

Design: docs/superpowers/specs/2026-03-20-spx-data-pipeline-design.md

Performance optimizations:
- Build indexes at init time to avoid repeated file scans
- Use date-based lookups instead of full scans
- Batch processing for fundamentals
"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Set

from ..data_provider import SPXDataProvider

# Configuration
DATA_DIR = Path(__file__).parent.parent.parent
LANDING_ZONE = DATA_DIR / "output" / "landing_zone"
WATERMARK_FILE = DATA_DIR / "output" / ".watermark"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("simulator")


class ComprehensiveSimulator:
    """
    Virtual clock simulator that drives data generation.

    Advances through trading dates, calls DataProvider for each date,
    and emits data files to landing_zone when data exists.

    Performance: Builds indexes at init to avoid repeated file scans.

    Flow:
        Virtual Clock Date -> DataProvider.get_*() -> Landing Zone Files
    """

    def __init__(self, provider: Optional[SPXDataProvider] = None):
        self.provider = provider or SPXDataProvider()
        self._ensure_directories()

        # Performance: Build indexes once at init
        logger.info("Building transcript index...")
        self._transcript_index: Dict[str, List[dict]] = {}  # date -> list of transcripts
        self._build_transcript_index()

        logger.info("Building fundamental index...")
        self._fundamental_index: Dict[str, List[tuple]] = {}  # date -> [(ticker, report_type, freq), ...]
        self._build_fundamental_index()

    def _ensure_directories(self):
        """Ensure landing zone directories exist."""
        (LANDING_ZONE / "prices").mkdir(parents=True, exist_ok=True)
        (LANDING_ZONE / "fundamentals").mkdir(parents=True, exist_ok=True)
        (LANDING_ZONE / "transcripts").mkdir(parents=True, exist_ok=True)

    def _build_transcript_index(self):
        """Build date-indexed transcript lookup for fast access."""
        all_transcripts = self.provider.list_transcripts()
        for transcript in all_transcripts:
            date = transcript["date"]
            if date not in self._transcript_index:
                self._transcript_index[date] = []
            self._transcript_index[date].append(transcript)

    def _build_fundamental_index(self):
        """
        Build date-indexed fundamental lookup for fast access.

        Scans all fundamental files once and indexes by their first (most recent) date.
        This avoids reading file headers every day (the known performance bug).
        """
        import glob
        fundamental_dir = DATA_DIR / "data" / "fundamental" / "SPX_Fundamental_History"
        pattern = str(fundamental_dir / "*.csv")

        for filepath in glob.glob(pattern):
            filename = Path(filepath).stem  # e.g., "AAPL_income_quarterly"
            parts = filename.rsplit("_", 2)
            if len(parts) != 3:
                continue

            ticker, report_type, freq = parts

            # Read only the first row to get the first date column
            try:
                with open(filepath, "r") as f:
                    first_line = f.readline().strip()
                    if first_line:
                        cols = first_line.split(",")
                        if len(cols) > 1:
                            fiscal_date = cols[1]  # First date column
                            if fiscal_date and not fiscal_date.startswith("0"):
                                if fiscal_date not in self._fundamental_index:
                                    self._fundamental_index[fiscal_date] = []
                                self._fundamental_index[fiscal_date].append(
                                    (ticker, report_type, freq, filepath)
                                )
            except Exception:
                pass

        logger.info(f"Fundamental index built: {len(self._fundamental_index)} dates")

    def _read_watermark(self) -> Optional[str]:
        """Read last processed date from watermark file."""
        if WATERMARK_FILE.exists():
            with open(WATERMARK_FILE, "r") as f:
                return f.read().strip()
        return None

    def _write_watermark(self, date: str):
        """Write current date to watermark file."""
        WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(WATERMARK_FILE, "w") as f:
            f.write(date)

    def _emit_price(self, date: str) -> int:
        """
        Emit price data for all tickers on given date.
        Returns number of records emitted.
        """
        output_file = LANDING_ZONE / "prices" / f"price_{date}.csv"

        # Skip if file already exists
        if output_file.exists():
            logger.debug(f"Price file already exists: {output_file}")
            return 0

        try:
            # Load all price data once and filter by date
            import pandas as pd
            df = self.provider._load_price_data()

            # Filter by date
            mask = df["Date"].dt.strftime("%Y-%m-%d") == date
            day_data = df[mask][["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]].copy()

            if len(day_data) > 0:
                day_data.to_csv(output_file, index=False)
                logger.info(f"Emitted price file: {output_file} ({len(day_data)} records)")
                return len(day_data)
            else:
                logger.debug(f"No price data for {date} (weekend/holiday)")
                return 0

        except Exception as e:
            logger.error(f"Failed to emit price for {date}: {e}")
            return 0

    def _emit_fundamentals(self, date: str) -> int:
        """
        Emit fundamental data for given date using pre-built index.
        Returns number of files emitted.

        Performance: Uses index lookup instead of scanning files each day.
        """
        emitted = 0

        # Use pre-built index for fast lookup
        if date not in self._fundamental_index:
            return 0

        for ticker, report_type, freq, filepath in self._fundamental_index[date]:
            output_dir = LANDING_ZONE / "fundamentals" / date
            output_dir.mkdir(parents=True, exist_ok=True)

            output_file = output_dir / f"{ticker}_{report_type}_{freq}.csv"
            if not output_file.exists():
                try:
                    import shutil
                    shutil.copy2(filepath, output_file)
                    logger.info(f"Emitted fundamental: {output_file}")
                    emitted += 1
                except Exception as e:
                    logger.debug(f"Failed to emit fundamentals for {ticker}: {e}")

        return emitted

    def _emit_transcripts(self, date: str) -> int:
        """
        Emit transcript PDF for given date using pre-built index.
        Returns number of transcripts emitted.

        Performance: Uses index lookup instead of scanning year-long list.
        """
        emitted = 0

        # Use pre-built index for fast lookup
        if date not in self._transcript_index:
            return 0

        for transcript in self._transcript_index[date]:
            ticker = transcript["ticker"]
            src_path = Path(transcript["path"])
            dest_file = LANDING_ZONE / "transcripts" / f"{ticker}_{date}.pdf"

            if not dest_file.exists() and src_path.exists():
                try:
                    import shutil
                    shutil.copy2(src_path, dest_file)
                    logger.info(f"Emitted transcript: {dest_file}")
                    emitted += 1
                except Exception as e:
                    logger.debug(f"Failed to emit transcript for {ticker}: {e}")

        return emitted

    def run_backfill(self, start_date: str, end_date: str, delay: float = 0.0):
        """
        Run backfill mode: process all dates from start to end.

        Args:
            start_date: Start date 'YYYY-MM-DD'
            end_date: End date 'YYYY-MM-DD'
            delay: Seconds to sleep between dates (0 = no delay)
        """
        logger.info(f"Starting backfill: {start_date} to {end_date} (delay={delay}s)")

        # Check for watermark (resume support)
        last_date = self._read_watermark()
        if last_date:
            logger.info(f"Resuming from watermark: {last_date}")
            # Start from day after watermark
            start_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            logger.info(f"Adjusted start date: {start_date}")

        # Get all trading dates in range
        trading_dates = self.provider.get_trading_dates(start_date, end_date)
        logger.info(f"Found {len(trading_dates)} trading dates to process")

        total_price_records = 0
        total_fundamental_files = 0
        total_transcript_files = 0

        for i, date in enumerate(trading_dates):
            logger.info(f"[{i+1}/{len(trading_dates)}] Processing {date}")

            # Emit data for this date
            price_count = self._emit_price(date)
            fundamental_count = self._emit_fundamentals(date)
            transcript_count = self._emit_transcripts(date)

            total_price_records += price_count
            total_fundamental_files += fundamental_count
            total_transcript_files += transcript_count

            # Update watermark
            self._write_watermark(date)

            # Delay between dates (for observing pipeline)
            if delay > 0:
                time.sleep(delay)

        logger.info(f"Backfill complete!")
        logger.info(f"  Total price records: {total_price_records}")
        logger.info(f"  Total fundamental files: {total_fundamental_files}")
        logger.info(f"  Total transcript files: {total_transcript_files}")

    def run_realtime(self, start_date: str, delay: float = 1.0):
        """
        Run realtime mode: advance clock from start_date with delay.

        Args:
            start_date: Start date 'YYYY-MM-DD'
            delay: Seconds between each virtual clock tick
        """
        logger.info(f"Starting realtime mode from {start_date} (delay={delay}s)")
        logger.info("Press Ctrl+C to stop")

        current_date = datetime.strptime(start_date, "%Y-%m-%d")

        try:
            while True:
                date_str = current_date.strftime("%Y-%m-%d")
                logger.info(f"Virtual Clock: {date_str}")

                # Only process if this is a trading date
                trading_dates = self.provider.get_trading_dates(date_str, date_str)
                if trading_dates:
                    self._emit_price(date_str)
                    self._emit_fundamentals(date_str)
                    self._emit_transcripts(date_str)
                else:
                    logger.debug(f"  {date_str} is not a trading date, skipping")

                # Advance to next day
                current_date += timedelta(days=1)

                # Delay
                time.sleep(delay)

        except KeyboardInterrupt:
            logger.info("Stopping realtime mode...")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SPX Data Pipeline - Comprehensive Simulator")
    parser.add_argument(
        "--mode",
        choices=["backfill", "realtime"],
        default="backfill",
        help="backfill: batch historical load; realtime: continuous with delay",
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2024-01-02",
        help="Start date YYYY-MM-DD (default: 2024-01-02)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="2024-12-30",
        help="End date YYYY-MM-DD (default: 2024-12-30)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay between dates in seconds (default: 0.0 for backfill)",
    )
    args = parser.parse_args()

    simulator = ComprehensiveSimulator()

    if args.mode == "backfill":
        simulator.run_backfill(args.start, args.end, args.delay)
    else:
        simulator.run_realtime(args.start, args.delay)


if __name__ == "__main__":
    main()
