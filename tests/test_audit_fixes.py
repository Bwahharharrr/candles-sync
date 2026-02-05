"""
Tests for audit fixes (C1-C5, H1-H6, L2-L3).

Verifies the specific behaviors corrected by the codebase audit.
"""

import os
import decimal

import pytest

# Set token before any EODHD imports
os.environ["EODHD_API_TOKEN"] = "test-token"

from candles_sync.adapters.base import Candle
from candles_sync.adapters.eodhd import EODHDAdapter, EODHD_EARLIEST_EOD_DATE
from candles_sync.adapters.yahoo import YahooAdapter
from candles_sync.candles_sync import (
    _audit_api_chunk_fill_internal_gaps,
    _group_consecutive,
    _to_decimal,
    _read_last_timestamp_from_file,
    _canonical_num_str,
    last_timestamp_in_dir,
    MAX_GAP_SYNTHESIS_ROWS,
    CANONICAL_DECIMAL_PLACES,
)


# ============================================================================
# C2 — Gap synthesis ceiling
# ============================================================================


class TestGapSynthesisCeiling:
    """Verify that unbounded gaps are capped at MAX_GAP_SYNTHESIS_ROWS."""

    def test_small_gap_is_filled(self):
        """A normal gap of a few missing candles should be filled."""
        candles = [
            Candle(timestamp=0, open=1, high=1, low=1, close=1, volume=0),
            # 3 missing 1-minute candles
            Candle(timestamp=240_000, open=1, high=1, low=1, close=1, volume=0),
        ]
        result = _audit_api_chunk_fill_internal_gaps("1m", candles)
        # Original 2 + 3 synthesized = 5
        assert len(result) == 5

    def test_huge_gap_is_skipped(self):
        """A gap exceeding MAX_GAP_SYNTHESIS_ROWS should NOT be synthesized."""
        # Create a gap larger than MAX_GAP_SYNTHESIS_ROWS minutes
        gap_ms = (MAX_GAP_SYNTHESIS_ROWS + 100) * 60_000
        candles = [
            Candle(timestamp=0, open=1, high=1, low=1, close=1, volume=0),
            Candle(timestamp=gap_ms, open=1, high=1, low=1, close=1, volume=0),
        ]
        result = _audit_api_chunk_fill_internal_gaps("1m", candles)
        # Only the 2 original candles, no synthesis
        assert len(result) == 2


# ============================================================================
# C3 — last_timestamp_in_dir full-scan uses running max
# ============================================================================


class TestLastTimestampInDirFullScan:
    """Verify full_scan=True uses running max, not list + max()."""

    def test_full_scan_returns_correct_max(self, tmp_path):
        """Full scan finds the maximum timestamp across all files."""
        for name, ts in [("2020-01-01.csv", 1577836800000), ("2020-01-03.csv", 1578009600000), ("2020-01-02.csv", 1577923200000)]:
            f = tmp_path / name
            f.write_text(f"timestamp,open,close,high,low,volume\n{ts},1,1,1,1,0\n")

        result = last_timestamp_in_dir(str(tmp_path), full_scan=True)
        assert result == 1578009600000

    def test_full_scan_with_empty_files(self, tmp_path):
        """Full scan ignores empty files gracefully."""
        (tmp_path / "2020-01-01.csv").write_text("timestamp,open,close,high,low,volume\n")
        (tmp_path / "2020-01-02.csv").write_text("timestamp,open,close,high,low,volume\n100,1,1,1,1,0\n")

        result = last_timestamp_in_dir(str(tmp_path), full_scan=True)
        assert result == 100


# ============================================================================
# H1 — EODHD symbol format validation
# ============================================================================


class TestEODHDSymbolValidation:
    """Verify EODHD adapter rejects bare symbols."""

    def test_valid_symbol_accepted(self):
        adapter = EODHDAdapter()
        assert adapter.format_symbol("AAPL.US") == "AAPL.US"
        assert adapter.format_symbol("mcd.us") == "MCD.US"

    def test_bare_symbol_raises(self):
        adapter = EODHDAdapter()
        with pytest.raises(ValueError, match="SYMBOL.EXCHANGE"):
            adapter.format_symbol("AAPL")

    def test_empty_symbol_raises(self):
        adapter = EODHDAdapter()
        with pytest.raises(ValueError, match="SYMBOL.EXCHANGE"):
            adapter.format_symbol("")


# ============================================================================
# H2 — EODHD genesis fallback returns earliest_possible, not now_ms
# ============================================================================


class TestEODHDGenesisFallback:
    """Verify genesis returns start-of-window when no candles found."""

    def test_intraday_no_data_returns_earliest_possible(self):
        adapter = EODHDAdapter()
        # fetch_fn returns empty
        result = adapter.find_genesis_timestamp(
            "AAPL.US", "1m", fetch_fn=lambda start, end: []
        )
        # Should NOT be close to now — should be ~120 days ago
        import time
        now_ms = int(time.time() * 1000)
        max_history_ms = 120 * 24 * 60 * 60 * 1000
        expected_approx = now_ms - max_history_ms
        # Allow 5s tolerance
        assert abs(result - expected_approx) < 5000

    def test_eod_no_data_returns_year_2000(self):
        adapter = EODHDAdapter()
        result = adapter.find_genesis_timestamp(
            "AAPL.US", "1D", fetch_fn=lambda start, end: []
        )
        expected = int(EODHD_EARLIEST_EOD_DATE.timestamp() * 1000)
        assert result == expected


# ============================================================================
# H3 — Yahoo null volume handling
# ============================================================================


class TestYahooNullVolume:
    """Verify Yahoo adapter skips candles with null volume."""

    def test_null_volume_skipped(self):
        adapter = YahooAdapter()
        data = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1000, 2000],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [1.0, 2.0],
                                    "high": [1.5, 2.5],
                                    "low": [0.5, 1.5],
                                    "close": [1.2, 2.2],
                                    "volume": [None, 100],
                                }
                            ]
                        },
                    }
                ]
            }
        }
        candles = adapter.parse_response(data)
        assert len(candles) == 1
        assert candles[0].timestamp == 2000 * 1000

    def test_valid_volume_kept(self):
        adapter = YahooAdapter()
        data = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1000],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [1.0],
                                    "high": [1.5],
                                    "low": [0.5],
                                    "close": [1.2],
                                    "volume": [500],
                                }
                            ]
                        },
                    }
                ]
            }
        }
        candles = adapter.parse_response(data)
        assert len(candles) == 1
        assert candles[0].volume == 500.0


# ============================================================================
# H4 — _to_decimal narrow exception handling
# ============================================================================


class TestToDecimalNarrow:
    """Verify _to_decimal catches only conversion errors."""

    def test_valid_values(self):
        assert _to_decimal(42) == decimal.Decimal("42")
        assert _to_decimal("3.14") == decimal.Decimal("3.14")
        assert _to_decimal(0) == decimal.Decimal("0")

    def test_invalid_value_returns_zero(self):
        assert _to_decimal("not_a_number") == decimal.Decimal(0)
        assert _to_decimal(None) == decimal.Decimal(0)

    def test_nan_converts_to_decimal_nan(self):
        result = _to_decimal(float("nan"))
        # float NaN converts to Decimal NaN without error
        assert result.is_nan()


# ============================================================================
# H5 — Corrupted CSV logging
# ============================================================================


class TestCorruptedCSVLogging:
    """Verify _read_last_timestamp_from_file logs warnings for errors."""

    def test_corrupted_file_returns_none(self, tmp_path):
        f = tmp_path / "corrupted.csv"
        f.write_bytes(b"\xff\xfe" + b"\x00" * 100)  # binary garbage
        result = _read_last_timestamp_from_file(str(f))
        assert result is None

    def test_valid_file_returns_timestamp(self, tmp_path):
        f = tmp_path / "valid.csv"
        f.write_text("timestamp,open,close,high,low,volume\n12345,1,1,1,1,0\n")
        result = _read_last_timestamp_from_file(str(f))
        assert result == 12345


# ============================================================================
# H6 — _group_consecutive guards against malformed names
# ============================================================================


class TestGroupConsecutiveMalformed:
    """Verify _group_consecutive skips invalid partition names."""

    def test_valid_names_grouped(self):
        result = _group_consecutive("1m", ["2024-01-01", "2024-01-02", "2024-01-03"])
        assert result == [["2024-01-01", "2024-01-02", "2024-01-03"]]

    def test_malformed_name_skipped(self):
        result = _group_consecutive("1m", ["2024-01-01", "not-a-date", "2024-01-02"])
        # "not-a-date" is skipped; "2024-01-01" and "2024-01-02" form one group
        assert result == [["2024-01-01", "2024-01-02"]]

    def test_all_malformed_returns_empty(self):
        result = _group_consecutive("1m", ["bad1", "bad2"])
        assert result == []

    def test_empty_list_returns_empty(self):
        result = _group_consecutive("1m", [])
        assert result == []


# ============================================================================
# L2/L3 — Named constants exist
# ============================================================================


class TestNamedConstants:
    """Verify extracted constants are accessible and reasonable."""

    def test_eodhd_earliest_date_is_year_2000(self):
        assert EODHD_EARLIEST_EOD_DATE.year == 2000
        assert EODHD_EARLIEST_EOD_DATE.month == 1
        assert EODHD_EARLIEST_EOD_DATE.day == 1

    def test_canonical_decimal_places(self):
        assert CANONICAL_DECIMAL_PLACES == 10

    def test_canonical_num_str_uses_constant(self):
        # A value that would differ if precision were wrong
        result = _canonical_num_str("1.123456789012345")
        # Should be truncated to 10 decimal places, trailing zeros stripped
        assert result == "1.123456789"
        # Verify a value that keeps trailing digits
        result2 = _canonical_num_str("1.1234567891")
        assert result2 == "1.1234567891"

    def test_gap_synthesis_ceiling_exists(self):
        assert MAX_GAP_SYNTHESIS_ROWS == 50_000
