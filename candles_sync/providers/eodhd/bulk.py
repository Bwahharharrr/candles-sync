"""
EODHD bulk sync for exchange-wide EOD updates.

Fetches bulk EOD data for an entire exchange and writes to partition files.
Uses the bulk endpoint: GET /api/eod-bulk-last-day/{EXCHANGE_CODE}
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from candles_sync.adapters.eodhd import get_api_token, EODHD_API_URL
from candles_sync.adapters.base import Candle


# Default data directory
DATA_DIR = Path.home() / ".corky" / "EODHD" / "candles"

# CSV column order (matches existing candles_sync pattern)
CSV_COLUMNS = ["timestamp", "open", "close", "high", "low", "volume"]


def _canonical_num_str(val: float) -> str:
    """Convert a number to canonical string representation."""
    if val == int(val):
        return str(int(val))
    s = f"{val:.10f}".rstrip("0").rstrip(".")
    return s


def _candle_to_csv_row(candle: Candle) -> str:
    """Convert a Candle to CSV row string."""
    return ",".join([
        str(candle.timestamp),
        _canonical_num_str(candle.open),
        _canonical_num_str(candle.close),
        _canonical_num_str(candle.high),
        _canonical_num_str(candle.low),
        _canonical_num_str(candle.volume),
    ])


def _fetch_bulk_data(
    exchange_code: str,
    date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch bulk EOD data for an exchange.

    Args:
        exchange_code: Exchange code (e.g., 'US', 'LSE')
        date: Optional date in YYYY-MM-DD format (default: latest)

    Returns:
        List of candle dicts from the API
    """
    url = f"{EODHD_API_URL}/eod-bulk-last-day/{exchange_code}"
    params = {
        "api_token": get_api_token(),
        "fmt": "json",
    }
    if date:
        params["date"] = date

    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _parse_bulk_record(record: Dict[str, Any]) -> Optional[tuple]:
    """
    Parse a bulk API record into (symbol, candle) tuple.

    Args:
        record: Single record from bulk API response

    Returns:
        Tuple of (full_symbol, Candle) or None if invalid
    """
    try:
        # Extract symbol and exchange
        code = record.get("code", "").upper()
        exchange = record.get("exchange_short_name", "").upper()
        if not code or not exchange:
            return None

        full_symbol = f"{code}.{exchange}"

        # Parse date to timestamp
        date_str = record.get("date", "")
        if not date_str:
            return None
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        dt = dt.replace(tzinfo=timezone.utc)
        timestamp_ms = int(dt.timestamp() * 1000)

        candle = Candle(
            timestamp=timestamp_ms,
            open=float(record.get("open", 0)),
            high=float(record.get("high", 0)),
            low=float(record.get("low", 0)),
            close=float(record.get("close", 0)),
            volume=float(record.get("volume", 0)),
        )
        return (full_symbol, candle)
    except (KeyError, ValueError, TypeError):
        return None


def _get_partition_file(symbol: str, timestamp_ms: int, data_dir: Path) -> Path:
    """
    Get the partition file path for a candle.

    Daily candles use yearly partitions: {symbol}/1D/{year}.csv

    Args:
        symbol: Full symbol (e.g., 'MCD.US')
        timestamp_ms: Candle timestamp in milliseconds
        data_dir: Base data directory

    Returns:
        Path to the partition file
    """
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    year = dt.year
    return data_dir / symbol / "1D" / f"{year}.csv"


def _ensure_dir(path: Path) -> None:
    """Ensure directory exists."""
    path.parent.mkdir(parents=True, exist_ok=True)


def _read_existing_timestamps(filepath: Path) -> set:
    """Read existing timestamps from a CSV file."""
    timestamps = set()
    if not filepath.exists():
        return timestamps
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("timestamp"):
                try:
                    ts = int(line.split(",")[0])
                    timestamps.add(ts)
                except (ValueError, IndexError):
                    continue
    return timestamps


def _append_to_csv(filepath: Path, candles: List[Candle]) -> int:
    """
    Append candles to a CSV file, skipping duplicates.

    Args:
        filepath: Path to the CSV file
        candles: List of candles to append

    Returns:
        Number of candles actually written
    """
    _ensure_dir(filepath)
    existing_ts = _read_existing_timestamps(filepath)

    # Filter out duplicates and sort by timestamp
    new_candles = [c for c in candles if c.timestamp not in existing_ts]
    new_candles.sort(key=lambda c: c.timestamp)

    if not new_candles:
        return 0

    # Check if file needs header
    needs_header = not filepath.exists() or filepath.stat().st_size == 0

    with open(filepath, "a") as f:
        if needs_header:
            f.write(",".join(CSV_COLUMNS) + "\n")
        for candle in new_candles:
            f.write(_candle_to_csv_row(candle) + "\n")

    return len(new_candles)


def bulk_sync_exchange(
    exchange_code: str,
    date: Optional[str] = None,
    data_dir: Optional[Path] = None,
    verbose: bool = False,
) -> Dict[str, bool]:
    """
    Bulk sync all tickers on an exchange.

    Fetches bulk EOD data and writes to partition files, creating new
    ticker directories as needed.

    Args:
        exchange_code: Exchange code (e.g., 'US', 'LSE')
        date: Optional date in YYYY-MM-DD format (default: latest)
        data_dir: Data directory (default: ~/.corky/EODHD/candles/)
        verbose: Print progress messages

    Returns:
        Dict mapping ticker to success status: {ticker: True/False}
    """
    data_dir = data_dir or DATA_DIR
    exchange_code = exchange_code.upper()

    if verbose:
        print(f"Fetching bulk data for {exchange_code}...")

    # Fetch bulk data
    try:
        raw_data = _fetch_bulk_data(exchange_code, date)
    except Exception as e:
        print(f"Error fetching bulk data: {e}")
        return {}

    if verbose:
        print(f"Received {len(raw_data)} records")

    # Group by symbol
    by_symbol: Dict[str, List[Candle]] = {}
    for record in raw_data:
        parsed = _parse_bulk_record(record)
        if parsed:
            symbol, candle = parsed
            if symbol not in by_symbol:
                by_symbol[symbol] = []
            by_symbol[symbol].append(candle)

    if verbose:
        print(f"Found {len(by_symbol)} unique tickers")

    # Write to partition files
    results: Dict[str, bool] = {}
    written_count = 0
    error_count = 0

    for symbol, candles in by_symbol.items():
        try:
            # Group candles by partition file (by year for 1D)
            by_partition: Dict[Path, List[Candle]] = {}
            for candle in candles:
                partition = _get_partition_file(symbol, candle.timestamp, data_dir)
                if partition not in by_partition:
                    by_partition[partition] = []
                by_partition[partition].append(candle)

            # Write to each partition
            total_written = 0
            for partition_path, partition_candles in by_partition.items():
                written = _append_to_csv(partition_path, partition_candles)
                total_written += written

            results[symbol] = True
            if total_written > 0:
                written_count += 1
                if verbose:
                    print(f"  {symbol}: wrote {total_written} candle(s)")
        except Exception as e:
            results[symbol] = False
            error_count += 1
            if verbose:
                print(f"  {symbol}: ERROR - {e}")

    if verbose:
        print(f"\nBulk sync complete: {written_count} tickers updated, {error_count} errors")

    return results


def bulk_sync_exchanges(
    exchange_codes: List[str],
    date: Optional[str] = None,
    data_dir: Optional[Path] = None,
    verbose: bool = False,
) -> Dict[str, Dict[str, bool]]:
    """
    Bulk sync multiple exchanges.

    Args:
        exchange_codes: List of exchange codes
        date: Optional date in YYYY-MM-DD format
        data_dir: Data directory
        verbose: Print progress messages

    Returns:
        Dict mapping exchange to ticker results
    """
    results: Dict[str, Dict[str, bool]] = {}
    for code in exchange_codes:
        if verbose:
            print(f"\n=== Exchange: {code} ===")
        results[code] = bulk_sync_exchange(
            code,
            date=date,
            data_dir=data_dir,
            verbose=verbose,
        )
    return results
