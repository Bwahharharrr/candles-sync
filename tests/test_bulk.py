"""Unit tests for candles_sync/providers/eodhd/bulk.py."""

import os
from pathlib import Path

# Set token before any EODHD imports that might need it
os.environ["EODHD_API_TOKEN"] = "test-token"

from candles_sync.providers.eodhd.bulk import (
    _parse_bulk_record,
    _candle_to_csv_row,
    _append_to_csv,
)
from candles_sync.adapters.base import Candle


# ============================================================================
# _parse_bulk_record
# ============================================================================


class TestParseBulkRecord:
    """Tests for _parse_bulk_record(record) -> (symbol, Candle) | None."""

    def test_valid_record(self):
        record = {
            "code": "MCD",
            "exchange_short_name": "US",
            "date": "2024-01-15",
            "open": 290.5,
            "high": 295.0,
            "low": 289.0,
            "close": 293.75,
            "volume": 5000000,
        }
        result = _parse_bulk_record(record)
        assert result is not None
        symbol, candle = result
        assert symbol == "MCD.US"
        assert candle.open == 290.5
        assert candle.high == 295.0
        assert candle.low == 289.0
        assert candle.close == 293.75
        assert candle.volume == 5000000.0
        # 2024-01-15 00:00:00 UTC in milliseconds
        assert candle.timestamp == 1705276800000

    def test_missing_code_returns_none(self):
        record = {
            "exchange_short_name": "US",
            "date": "2024-01-15",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 103,
            "volume": 1000,
        }
        assert _parse_bulk_record(record) is None

    def test_empty_code_returns_none(self):
        record = {
            "code": "",
            "exchange_short_name": "US",
            "date": "2024-01-15",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 103,
            "volume": 1000,
        }
        assert _parse_bulk_record(record) is None

    def test_missing_date_returns_none(self):
        record = {
            "code": "AAPL",
            "exchange_short_name": "US",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 103,
            "volume": 1000,
        }
        assert _parse_bulk_record(record) is None

    def test_empty_date_returns_none(self):
        record = {
            "code": "AAPL",
            "exchange_short_name": "US",
            "date": "",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 103,
            "volume": 1000,
        }
        assert _parse_bulk_record(record) is None

    def test_missing_exchange_returns_none(self):
        record = {
            "code": "AAPL",
            "date": "2024-01-15",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 103,
            "volume": 1000,
        }
        assert _parse_bulk_record(record) is None

    def test_code_uppercased(self):
        record = {
            "code": "mcd",
            "exchange_short_name": "us",
            "date": "2024-01-15",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 103,
            "volume": 1000,
        }
        result = _parse_bulk_record(record)
        assert result is not None
        symbol, _ = result
        assert symbol == "MCD.US"

    def test_defaults_to_zero_for_missing_ohlcv(self):
        record = {
            "code": "XYZ",
            "exchange_short_name": "LSE",
            "date": "2024-06-01",
        }
        result = _parse_bulk_record(record)
        assert result is not None
        _, candle = result
        assert candle.open == 0.0
        assert candle.high == 0.0
        assert candle.low == 0.0
        assert candle.close == 0.0
        assert candle.volume == 0.0

    def test_invalid_date_format_returns_none(self):
        record = {
            "code": "AAPL",
            "exchange_short_name": "US",
            "date": "15/01/2024",
            "open": 100,
            "high": 105,
            "low": 99,
            "close": 103,
            "volume": 1000,
        }
        assert _parse_bulk_record(record) is None


# ============================================================================
# _candle_to_csv_row
# ============================================================================


class TestCandleToCsvRow:
    """Tests for _candle_to_csv_row(candle) -> str."""

    def test_produces_correct_csv_string(self):
        candle = Candle(
            timestamp=1705276800000,
            open=290.5,
            high=295.0,
            low=289.0,
            close=293.75,
            volume=5000000.0,
        )
        row = _candle_to_csv_row(candle)
        parts = row.split(",")
        assert len(parts) == 6
        assert parts[0] == "1705276800000"
        # CSV column order: timestamp, open, close, high, low, volume
        assert parts[1] == "290.5"
        assert parts[2] == "293.75"
        assert parts[3] == "295"
        assert parts[4] == "289"
        assert parts[5] == "5000000"

    def test_integer_values_no_trailing_zeros(self):
        candle = Candle(
            timestamp=1000000000000,
            open=100.0,
            high=200.0,
            low=50.0,
            close=150.0,
            volume=1000.0,
        )
        row = _candle_to_csv_row(candle)
        # All values are whole numbers, should have no decimal points
        assert ".0" not in row

    def test_decimal_precision_preserved(self):
        candle = Candle(
            timestamp=1000000000000,
            open=1.12345678,
            high=2.12345678,
            low=0.12345678,
            close=1.5,
            volume=999.99,
        )
        row = _candle_to_csv_row(candle)
        parts = row.split(",")
        # open is parts[1], close is parts[2] (CSV_COLUMNS order)
        assert parts[1] == "1.12345678"
        assert parts[2] == "1.5"


# ============================================================================
# _append_to_csv
# ============================================================================


class TestAppendToCsv:
    """Tests for _append_to_csv(filepath, candles) -> int."""

    def test_creates_new_file_with_header(self, tmp_path):
        filepath = tmp_path / "sym" / "1D" / "2024.csv"
        candles = [
            Candle(timestamp=1705276800000, open=100, high=105, low=99, close=103, volume=1000),
        ]
        written = _append_to_csv(filepath, candles)
        assert written == 1
        assert filepath.exists()
        content = filepath.read_text()
        lines = content.strip().split("\n")
        assert lines[0] == "timestamp,open,close,high,low,volume"
        assert len(lines) == 2

    def test_appends_and_deduplicates(self, tmp_path):
        filepath = tmp_path / "test.csv"
        candle_a = Candle(timestamp=1000000, open=10, high=11, low=9, close=10.5, volume=100)
        candle_b = Candle(timestamp=2000000, open=20, high=21, low=19, close=20.5, volume=200)

        # Write first candle
        _append_to_csv(filepath, [candle_a])

        # Append with a duplicate and a new candle
        written = _append_to_csv(filepath, [candle_a, candle_b])
        assert written == 1  # only candle_b is new

        content = filepath.read_text()
        lines = content.strip().split("\n")
        # header + 2 data rows
        assert len(lines) == 3

    def test_output_is_sorted_by_timestamp(self, tmp_path):
        filepath = tmp_path / "sorted.csv"
        # Write candles in reverse order
        candles = [
            Candle(timestamp=3000000, open=30, high=31, low=29, close=30.5, volume=300),
            Candle(timestamp=1000000, open=10, high=11, low=9, close=10.5, volume=100),
            Candle(timestamp=2000000, open=20, high=21, low=19, close=20.5, volume=200),
        ]
        _append_to_csv(filepath, candles)

        content = filepath.read_text()
        lines = content.strip().split("\n")
        # Skip header, extract timestamps
        timestamps = [int(line.split(",")[0]) for line in lines[1:]]
        assert timestamps == sorted(timestamps)

    def test_atomic_write_no_tmp_left(self, tmp_path):
        filepath = tmp_path / "atomic.csv"
        candles = [
            Candle(timestamp=1000000, open=10, high=11, low=9, close=10.5, volume=100),
        ]
        _append_to_csv(filepath, candles)

        tmp_path_file = filepath.with_suffix(".tmp")
        assert not tmp_path_file.exists()
        assert filepath.exists()

    def test_returns_zero_for_all_duplicates(self, tmp_path):
        filepath = tmp_path / "dup.csv"
        candle = Candle(timestamp=1000000, open=10, high=11, low=9, close=10.5, volume=100)
        _append_to_csv(filepath, [candle])

        written = _append_to_csv(filepath, [candle])
        assert written == 0

    def test_appending_preserves_existing_data(self, tmp_path):
        filepath = tmp_path / "preserve.csv"
        candle_a = Candle(timestamp=1000000, open=10, high=11, low=9, close=10.5, volume=100)
        candle_b = Candle(timestamp=2000000, open=20, high=21, low=19, close=20.5, volume=200)

        _append_to_csv(filepath, [candle_a])
        _append_to_csv(filepath, [candle_b])

        content = filepath.read_text()
        lines = content.strip().split("\n")
        # header + 2 data rows
        assert len(lines) == 3
        # First data row should be candle_a (lower timestamp)
        assert lines[1].startswith("1000000,")
        assert lines[2].startswith("2000000,")


