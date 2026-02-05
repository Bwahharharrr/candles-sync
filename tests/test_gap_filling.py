"""Unit tests for gap-filling logic in candles_sync.candles_sync."""

import pandas as pd
import pytest

from candles_sync.candles_sync import (
    CSV_COLUMNS,
    _audit_api_chunk_fill_internal_gaps,
    _canonical_num_str,
    _synthesize_gap_rows,
)
from candles_sync.adapters.base import Candle

# Interval constants (milliseconds) mirroring the production INTERVAL_MS dict.
INTERVAL_MS = {"1m": 60_000, "1h": 3_600_000, "1D": 86_400_000}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_candle(ts: int, price: float = 100.0, volume: float = 10.0) -> Candle:
    """Build a Candle with uniform OHLC equal to *price* for easy assertions."""
    return Candle(
        timestamp=ts,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=volume,
    )


# ---------------------------------------------------------------------------
# _synthesize_gap_rows
# ---------------------------------------------------------------------------

class TestSynthesizeGapRows:
    """Tests for _synthesize_gap_rows."""

    def test_single_missing_timestamp(self):
        """A single missing timestamp produces one synthetic row."""
        ts = 1_000_000
        prev_close = 42.5
        df = _synthesize_gap_rows([ts], prev_close)

        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == CSV_COLUMNS
        assert len(df) == 1

        row = df.iloc[0]
        assert int(row["timestamp"]) == ts
        expected_price = _canonical_num_str(prev_close)
        assert row["open"] == expected_price
        assert row["close"] == expected_price
        assert row["high"] == expected_price
        assert row["low"] == expected_price
        assert row["volume"] == "0"

    def test_multiple_missing_timestamps(self):
        """Multiple missing timestamps produce one row per gap slot."""
        timestamps = [2_000_000, 2_060_000, 2_120_000]
        prev_close = 99.0
        df = _synthesize_gap_rows(timestamps, prev_close)

        assert len(df) == 3
        assert list(df.columns) == CSV_COLUMNS

        for i, ts in enumerate(timestamps):
            row = df.iloc[i]
            assert int(row["timestamp"]) == ts
            assert row["volume"] == "0"

    def test_canonical_number_format(self):
        """prev_close is run through _canonical_num_str (no trailing zeros)."""
        # 100.50000 should become "100.5" after canonical formatting
        df = _synthesize_gap_rows([5_000_000], 100.50000)
        row = df.iloc[0]
        assert row["open"] == "100.5"
        assert row["close"] == "100.5"
        assert row["high"] == "100.5"
        assert row["low"] == "100.5"

    def test_empty_expected_ts_returns_empty_df(self):
        """An empty list of timestamps returns an empty DataFrame with correct columns."""
        df = _synthesize_gap_rows([], 50.0)
        assert list(df.columns) == CSV_COLUMNS
        assert len(df) == 0


# ---------------------------------------------------------------------------
# _audit_api_chunk_fill_internal_gaps
# ---------------------------------------------------------------------------

class TestAuditApiChunkFillInternalGaps:
    """Tests for _audit_api_chunk_fill_internal_gaps."""

    def test_no_gaps_returns_canonical_rows(self):
        """Consecutive candles with no gap return the same data, canonicalized."""
        base = 1_600_000_000_000
        interval = INTERVAL_MS["1m"]
        candles = [
            _make_candle(base, price=100.0, volume=5.0),
            _make_candle(base + interval, price=101.0, volume=6.0),
            _make_candle(base + 2 * interval, price=102.0, volume=7.0),
        ]

        result = _audit_api_chunk_fill_internal_gaps("1m", candles)

        assert len(result) == 3
        # Each sub-list matches CSV_COLUMNS order
        assert result[0][0] == str(base)
        assert result[0][1] == _canonical_num_str(100.0)  # open
        assert result[0][2] == _canonical_num_str(100.0)  # close
        assert result[0][5] == _canonical_num_str(5.0)    # volume

    def test_1m_gap_fills_two_missing_candles(self):
        """A 3-minute jump (skipping 2 intervals) inserts 2 synthetic rows."""
        base = 1_600_000_000_000
        interval = INTERVAL_MS["1m"]

        candle_a = Candle(
            timestamp=base,
            open=100.0, high=105.0, low=95.0, close=102.0, volume=10.0,
        )
        # Jump from base to base + 3*interval — 2 intervals are missing
        candle_b = Candle(
            timestamp=base + 3 * interval,
            open=103.0, high=108.0, low=98.0, close=106.0, volume=20.0,
        )

        result = _audit_api_chunk_fill_internal_gaps("1m", [candle_a, candle_b])

        # Original 2 + 2 filled = 4
        assert len(result) == 4

        # Verify timestamps are continuous
        timestamps = [int(r[0]) for r in result]
        assert timestamps == [
            base,
            base + interval,
            base + 2 * interval,
            base + 3 * interval,
        ]

        # The two filled rows should carry candle_a's close as OHLC and volume=0
        for filled_row in result[1:3]:
            assert filled_row[1] == _canonical_num_str(102.0)  # open
            assert filled_row[2] == _canonical_num_str(102.0)  # close
            assert filled_row[3] == _canonical_num_str(102.0)  # high
            assert filled_row[4] == _canonical_num_str(102.0)  # low
            assert filled_row[5] == _canonical_num_str(0)       # volume

    def test_empty_input_returns_empty_list(self):
        """An empty candles list returns []."""
        result = _audit_api_chunk_fill_internal_gaps("1h", [])
        assert result == []

    def test_single_candle_returns_canonical_single_row(self):
        """A single candle is returned canonicalized without error."""
        ts = 1_600_000_000_000
        candle = Candle(
            timestamp=ts,
            open=200.123456789, high=201.0, low=199.0, close=200.5, volume=50.0,
        )

        result = _audit_api_chunk_fill_internal_gaps("1D", [candle])

        assert len(result) == 1
        row = result[0]
        assert row[0] == str(ts)
        assert row[1] == _canonical_num_str(200.123456789)
        assert row[2] == _canonical_num_str(200.5)
        assert row[3] == _canonical_num_str(201.0)
        assert row[4] == _canonical_num_str(199.0)
        assert row[5] == _canonical_num_str(50.0)

    def test_deduplicates_timestamps(self):
        """Duplicate timestamps are collapsed (keep last)."""
        ts = 1_600_000_000_000
        candle_first = Candle(
            timestamp=ts, open=100.0, high=100.0, low=100.0, close=100.0, volume=1.0,
        )
        candle_dup = Candle(
            timestamp=ts, open=200.0, high=200.0, low=200.0, close=200.0, volume=2.0,
        )

        result = _audit_api_chunk_fill_internal_gaps("1h", [candle_first, candle_dup])

        assert len(result) == 1
        # "keep last" means the second candle wins
        assert result[0][1] == _canonical_num_str(200.0)
        assert result[0][5] == _canonical_num_str(2.0)

    def test_preserves_ascending_sort_order(self):
        """Output timestamps are always in ascending order even if input is jumbled."""
        base = 1_600_000_000_000
        interval = INTERVAL_MS["1h"]

        candles = [
            _make_candle(base + 2 * interval, price=300.0),
            _make_candle(base, price=100.0),
            _make_candle(base + interval, price=200.0),
        ]

        result = _audit_api_chunk_fill_internal_gaps("1h", candles)

        timestamps = [int(r[0]) for r in result]
        assert timestamps == sorted(timestamps)
        assert len(result) == 3

    def test_1h_gap_fills_correctly(self):
        """Verify gap filling works for 1h timeframe too."""
        base = 1_600_000_000_000
        interval = INTERVAL_MS["1h"]

        candle_a = Candle(
            timestamp=base,
            open=50.0, high=55.0, low=45.0, close=52.0, volume=100.0,
        )
        # Skip 1 hour
        candle_b = Candle(
            timestamp=base + 2 * interval,
            open=53.0, high=58.0, low=48.0, close=56.0, volume=120.0,
        )

        result = _audit_api_chunk_fill_internal_gaps("1h", [candle_a, candle_b])

        assert len(result) == 3
        timestamps = [int(r[0]) for r in result]
        assert timestamps == [base, base + interval, base + 2 * interval]

        # Filled row carries candle_a close
        filled = result[1]
        assert filled[1] == _canonical_num_str(52.0)
        assert filled[5] == _canonical_num_str(0)

    def test_all_columns_are_strings(self):
        """Every element in the returned list-of-lists is a string."""
        candles = [
            _make_candle(1_600_000_000_000, price=123.456, volume=789.0),
        ]

        result = _audit_api_chunk_fill_internal_gaps("1D", candles)

        for row in result:
            assert len(row) == len(CSV_COLUMNS)
            for cell in row:
                assert isinstance(cell, str), f"Expected str, got {type(cell)}: {cell!r}"
