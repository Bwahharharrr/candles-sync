"""
Comprehensive unit tests for write_partition and related file helpers
in candles_sync/candles_sync.py.
"""

import os

import pandas as pd
import pytest

from candles_sync.candles_sync import (
    CSV_COLUMNS,
    _read_last_timestamp_from_file,
    last_timestamp_in_dir,
    write_partition,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(rows: list[list]) -> pd.DataFrame:
    """Build a DataFrame from raw row values matching CSV_COLUMNS order."""
    return pd.DataFrame(rows, columns=CSV_COLUMNS, dtype=str)


def _read_csv(path: str) -> pd.DataFrame:
    """Read a partition CSV back exactly as write_partition would read it."""
    return pd.read_csv(path, dtype=str)


def _write_raw_csv(path: str, lines: list[str]) -> None:
    """Write raw CSV content (header + data lines) for test fixtures."""
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# write_partition — new file creation
# ---------------------------------------------------------------------------

class TestWritePartitionNewFile:
    """write_partition when no file exists yet."""

    def test_creates_file_with_correct_header_and_rows(self, tmp_path):
        path = str(tmp_path / "2024-01-01.csv")
        df = _make_df([
            ["1704067200000", "42000.5", "42100", "42200", "41900", "1.25"],
        ])

        delta = write_partition(df, path)

        assert os.path.exists(path)
        result = _read_csv(path)
        assert list(result.columns) == CSV_COLUMNS
        assert len(result) == 1
        assert delta == 1

    def test_multiple_rows_sorted_by_timestamp(self, tmp_path):
        path = str(tmp_path / "2024-01-01.csv")
        df = _make_df([
            ["1704067260000", "42100", "42200", "42300", "42000", "0.5"],
            ["1704067200000", "42000", "42100", "42200", "41900", "1.0"],
        ])

        write_partition(df, path)

        result = _read_csv(path)
        timestamps = result["timestamp"].astype(int).tolist()
        assert timestamps == sorted(timestamps)
        assert timestamps == [1704067200000, 1704067260000]


# ---------------------------------------------------------------------------
# write_partition — merge with existing file
# ---------------------------------------------------------------------------

class TestWritePartitionMerge:
    """write_partition when a file already exists."""

    def test_merges_new_rows_into_existing(self, tmp_path):
        path = str(tmp_path / "2024-01-01.csv")

        # Write initial data
        old_df = _make_df([
            ["1704067200000", "42000", "42100", "42200", "41900", "1.0"],
        ])
        write_partition(old_df, path)

        # Merge new data
        new_df = _make_df([
            ["1704067260000", "42100", "42200", "42300", "42000", "0.5"],
        ])
        delta = write_partition(new_df, path)

        result = _read_csv(path)
        assert len(result) == 2
        assert delta == 1

    def test_deduplicates_by_timestamp_keeps_last(self, tmp_path):
        """When new data has same timestamp as existing, 'last' (new) wins."""
        path = str(tmp_path / "2024-01-01.csv")

        old_df = _make_df([
            ["1704067200000", "42000", "42100", "42200", "41900", "1.0"],
        ])
        write_partition(old_df, path)

        # Same timestamp, different OHLCV
        new_df = _make_df([
            ["1704067200000", "99999", "99999", "99999", "99999", "99999"],
        ])
        delta = write_partition(new_df, path)

        result = _read_csv(path)
        assert len(result) == 1
        assert delta == 0  # merged count same as old count
        assert result.iloc[0]["open"] == "99999"

    def test_partial_overlap_keeps_correct_values(self, tmp_path):
        """Mix of overlapping and new timestamps."""
        path = str(tmp_path / "2024-01-01.csv")

        old_df = _make_df([
            ["1704067200000", "100", "101", "102", "99", "10"],
            ["1704067260000", "101", "102", "103", "100", "20"],
        ])
        write_partition(old_df, path)

        # Overlaps ts 1704067260000, adds ts 1704067320000
        new_df = _make_df([
            ["1704067260000", "200", "201", "202", "199", "30"],
            ["1704067320000", "300", "301", "302", "299", "40"],
        ])
        delta = write_partition(new_df, path)

        result = _read_csv(path)
        assert len(result) == 3
        assert delta == 1  # net +1 row

        # The overlapping row should use the new values (keep="last")
        row_260 = result[result["timestamp"] == "1704067260000"].iloc[0]
        assert row_260["open"] == "200"


# ---------------------------------------------------------------------------
# write_partition — sorting
# ---------------------------------------------------------------------------

class TestWritePartitionSorting:
    """Output is always sorted by timestamp ascending."""

    def test_unsorted_input_produces_sorted_output(self, tmp_path):
        path = str(tmp_path / "test.csv")
        df = _make_df([
            ["1704067320000", "3", "3", "3", "3", "3"],
            ["1704067200000", "1", "1", "1", "1", "1"],
            ["1704067260000", "2", "2", "2", "2", "2"],
        ])

        write_partition(df, path)

        result = _read_csv(path)
        timestamps = result["timestamp"].astype(int).tolist()
        assert timestamps == [1704067200000, 1704067260000, 1704067320000]

    def test_merge_of_interleaved_timestamps_sorted(self, tmp_path):
        path = str(tmp_path / "test.csv")

        old_df = _make_df([
            ["1704067200000", "1", "1", "1", "1", "1"],
            ["1704067320000", "3", "3", "3", "3", "3"],
        ])
        write_partition(old_df, path)

        new_df = _make_df([
            ["1704067260000", "2", "2", "2", "2", "2"],
        ])
        write_partition(new_df, path)

        result = _read_csv(path)
        timestamps = result["timestamp"].astype(int).tolist()
        assert timestamps == [1704067200000, 1704067260000, 1704067320000]


# ---------------------------------------------------------------------------
# write_partition — canonical number formatting
# ---------------------------------------------------------------------------

class TestWritePartitionCanonicalNumbers:
    """_canonical_num_str is applied to OHLCV columns."""

    def test_trailing_zeros_stripped(self, tmp_path):
        path = str(tmp_path / "test.csv")
        df = _make_df([
            ["1000", "42000.50000", "42100.00000", "42200.10000", "41900.00100", "1.25000"],
        ])

        write_partition(df, path)

        result = _read_csv(path)
        row = result.iloc[0]
        assert row["open"] == "42000.5"
        assert row["close"] == "42100"
        assert row["high"] == "42200.1"
        assert row["low"] == "41900.001"
        assert row["volume"] == "1.25"

    def test_integer_values_have_no_decimal(self, tmp_path):
        path = str(tmp_path / "test.csv")
        df = _make_df([
            ["1000", "100", "200", "300", "50", "10"],
        ])

        write_partition(df, path)

        result = _read_csv(path)
        row = result.iloc[0]
        assert row["open"] == "100"
        assert row["close"] == "200"
        assert row["volume"] == "10"

    def test_very_small_values_preserved(self, tmp_path):
        path = str(tmp_path / "test.csv")
        df = _make_df([
            ["1000", "0.0000001234", "0.00000056", "0.0000009", "0.0000000001", "0.001"],
        ])

        write_partition(df, path)

        result = _read_csv(path)
        row = result.iloc[0]
        # _canonical_num_str uses 10 decimal places then strips trailing zeros
        assert row["open"] == "0.0000001234"
        assert row["close"] == "0.00000056"
        assert row["volume"] == "0.001"

    def test_zero_value(self, tmp_path):
        path = str(tmp_path / "test.csv")
        df = _make_df([
            ["1000", "0", "0.0", "0.00", "0.000", "0"],
        ])

        write_partition(df, path)

        result = _read_csv(path)
        row = result.iloc[0]
        assert row["open"] == "0"
        assert row["close"] == "0"
        assert row["high"] == "0"
        assert row["low"] == "0"
        assert row["volume"] == "0"


# ---------------------------------------------------------------------------
# write_partition — atomic write (.tmp file)
# ---------------------------------------------------------------------------

class TestWritePartitionAtomicWrite:
    """Verify the atomic write leaves no .tmp debris."""

    def test_no_tmp_file_after_write(self, tmp_path):
        path = str(tmp_path / "test.csv")
        df = _make_df([
            ["1000", "1", "1", "1", "1", "1"],
        ])

        write_partition(df, path)

        assert not os.path.exists(path + ".tmp")
        assert os.path.exists(path)

    def test_no_tmp_file_after_merge(self, tmp_path):
        path = str(tmp_path / "test.csv")

        old_df = _make_df([["1000", "1", "1", "1", "1", "1"]])
        write_partition(old_df, path)

        new_df = _make_df([["2000", "2", "2", "2", "2", "2"]])
        write_partition(new_df, path)

        tmp_files = [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]
        assert tmp_files == []


# ---------------------------------------------------------------------------
# write_partition — delta return value
# ---------------------------------------------------------------------------

class TestWritePartitionDelta:
    """Verify the delta (new row count) return value."""

    def test_delta_new_file(self, tmp_path):
        path = str(tmp_path / "test.csv")
        df = _make_df([
            ["1000", "1", "1", "1", "1", "1"],
            ["2000", "2", "2", "2", "2", "2"],
        ])
        assert write_partition(df, path) == 2

    def test_delta_no_new_rows(self, tmp_path):
        path = str(tmp_path / "test.csv")
        df = _make_df([["1000", "1", "1", "1", "1", "1"]])
        write_partition(df, path)
        # Re-write the same data
        assert write_partition(df, path) == 0

    def test_delta_with_duplicates_and_new(self, tmp_path):
        path = str(tmp_path / "test.csv")
        old_df = _make_df([
            ["1000", "1", "1", "1", "1", "1"],
            ["2000", "2", "2", "2", "2", "2"],
        ])
        write_partition(old_df, path)

        new_df = _make_df([
            ["2000", "9", "9", "9", "9", "9"],  # duplicate
            ["3000", "3", "3", "3", "3", "3"],  # new
        ])
        assert write_partition(new_df, path) == 1


# ---------------------------------------------------------------------------
# write_partition — edge cases
# ---------------------------------------------------------------------------

class TestWritePartitionEdgeCases:
    """Edge cases for write_partition."""

    def test_empty_dataframe_new_file(self, tmp_path):
        path = str(tmp_path / "test.csv")
        df = pd.DataFrame(columns=CSV_COLUMNS)

        delta = write_partition(df, path)

        assert os.path.exists(path)
        result = _read_csv(path)
        assert len(result) == 0
        assert delta == 0

    def test_nan_timestamps_dropped(self, tmp_path):
        path = str(tmp_path / "test.csv")
        df = _make_df([
            ["1000", "1", "1", "1", "1", "1"],
            ["not_a_number", "2", "2", "2", "2", "2"],
        ])

        write_partition(df, path)

        result = _read_csv(path)
        assert len(result) == 1

    def test_timestamp_stored_as_int(self, tmp_path):
        """Timestamps must be integers, not floats (no '.0' suffix)."""
        path = str(tmp_path / "test.csv")
        df = _make_df([["1704067200000", "1", "1", "1", "1", "1"]])

        write_partition(df, path)

        result = _read_csv(path)
        ts_str = result.iloc[0]["timestamp"]
        assert "." not in ts_str
        assert ts_str == "1704067200000"


# ---------------------------------------------------------------------------
# _read_last_timestamp_from_file
# ---------------------------------------------------------------------------

class TestReadLastTimestampFromFile:
    """Tests for the O(1) seek-from-end timestamp reader."""

    def test_valid_csv_returns_last_timestamp(self, tmp_path):
        path = str(tmp_path / "test.csv")
        _write_raw_csv(path, [
            "timestamp,open,close,high,low,volume",
            "1000,1,1,1,1,1",
            "2000,2,2,2,2,2",
            "3000,3,3,3,3,3",
        ])

        assert _read_last_timestamp_from_file(path) == 3000

    def test_single_data_row(self, tmp_path):
        path = str(tmp_path / "test.csv")
        _write_raw_csv(path, [
            "timestamp,open,close,high,low,volume",
            "1704067200000,42000,42100,42200,41900,1",
        ])

        assert _read_last_timestamp_from_file(path) == 1704067200000

    def test_header_only_csv_returns_none(self, tmp_path):
        path = str(tmp_path / "test.csv")
        _write_raw_csv(path, [
            "timestamp,open,close,high,low,volume",
        ])

        assert _read_last_timestamp_from_file(path) is None

    def test_empty_file_returns_none(self, tmp_path):
        path = str(tmp_path / "test.csv")
        with open(path, "w") as f:
            f.write("")

        assert _read_last_timestamp_from_file(path) is None

    def test_nonexistent_file_returns_none(self, tmp_path):
        path = str(tmp_path / "does_not_exist.csv")

        assert _read_last_timestamp_from_file(path) is None

    def test_float_timestamp_converted_to_int(self, tmp_path):
        """The implementation uses int(float(ts_str)), so '3000.0' -> 3000."""
        path = str(tmp_path / "test.csv")
        _write_raw_csv(path, [
            "timestamp,open,close,high,low,volume",
            "3000.0,3,3,3,3,3",
        ])

        assert _read_last_timestamp_from_file(path) == 3000

    def test_trailing_newlines_handled(self, tmp_path):
        """Files may end with trailing newlines; should still find last data line."""
        path = str(tmp_path / "test.csv")
        with open(path, "w") as f:
            f.write("timestamp,open,close,high,low,volume\n")
            f.write("1000,1,1,1,1,1\n")
            f.write("2000,2,2,2,2,2\n")
            f.write("\n")  # trailing blank line

        assert _read_last_timestamp_from_file(path) == 2000

    def test_large_file_reads_only_tail(self, tmp_path):
        """Even with many rows, the function should return the last timestamp correctly."""
        path = str(tmp_path / "test.csv")
        lines = ["timestamp,open,close,high,low,volume"]
        for i in range(500):
            ts = 1000 + i * 60000
            lines.append(f"{ts},{i},{i},{i},{i},{i}")
        _write_raw_csv(path, lines)

        expected = 1000 + 499 * 60000
        assert _read_last_timestamp_from_file(path) == expected


# ---------------------------------------------------------------------------
# last_timestamp_in_dir
# ---------------------------------------------------------------------------

class TestLastTimestampInDir:
    """Tests for scanning a directory for the latest timestamp."""

    def test_multiple_files_returns_max(self, tmp_path):
        _write_raw_csv(str(tmp_path / "2024-01-01.csv"), [
            "timestamp,open,close,high,low,volume",
            "1704067200000,1,1,1,1,1",
        ])
        _write_raw_csv(str(tmp_path / "2024-01-02.csv"), [
            "timestamp,open,close,high,low,volume",
            "1704153600000,2,2,2,2,2",
        ])
        _write_raw_csv(str(tmp_path / "2024-01-03.csv"), [
            "timestamp,open,close,high,low,volume",
            "1704240000000,3,3,3,3,3",
        ])

        result = last_timestamp_in_dir(str(tmp_path))
        assert result == 1704240000000

    def test_empty_directory_returns_none(self, tmp_path):
        assert last_timestamp_in_dir(str(tmp_path)) is None

    def test_header_only_files_returns_none(self, tmp_path):
        _write_raw_csv(str(tmp_path / "2024-01-01.csv"), [
            "timestamp,open,close,high,low,volume",
        ])
        _write_raw_csv(str(tmp_path / "2024-01-02.csv"), [
            "timestamp,open,close,high,low,volume",
        ])

        assert last_timestamp_in_dir(str(tmp_path)) is None

    def test_ignores_non_csv_files(self, tmp_path):
        # Non-CSV file with a timestamp-like name
        (tmp_path / ".gotstart").touch()
        (tmp_path / "notes.txt").write_text("not a csv")

        _write_raw_csv(str(tmp_path / "2024-01-01.csv"), [
            "timestamp,open,close,high,low,volume",
            "1704067200000,1,1,1,1,1",
        ])

        assert last_timestamp_in_dir(str(tmp_path)) == 1704067200000

    def test_skips_empty_csv_files(self, tmp_path):
        # Empty CSV (0 bytes)
        (tmp_path / "2024-01-01.csv").write_text("")
        _write_raw_csv(str(tmp_path / "2024-01-02.csv"), [
            "timestamp,open,close,high,low,volume",
            "5000,1,1,1,1,1",
        ])

        assert last_timestamp_in_dir(str(tmp_path)) == 5000

    def test_optimized_reverse_scan_finds_latest(self, tmp_path):
        """The optimized (non-full_scan) path scans files in reverse name order
        and returns the first valid timestamp found."""
        _write_raw_csv(str(tmp_path / "2024-01.csv"), [
            "timestamp,open,close,high,low,volume",
            "1000,1,1,1,1,1",
        ])
        _write_raw_csv(str(tmp_path / "2024-12.csv"), [
            "timestamp,open,close,high,low,volume",
            "9000,9,9,9,9,9",
        ])

        # Non-full_scan (default): scans from 2024-12.csv first
        result = last_timestamp_in_dir(str(tmp_path))
        assert result == 9000

    def test_full_scan_returns_max_across_all_files(self, tmp_path):
        """full_scan=True scans every file and returns the global max."""
        _write_raw_csv(str(tmp_path / "2024-01.csv"), [
            "timestamp,open,close,high,low,volume",
            "99999,1,1,1,1,1",  # Largest ts in an earlier file
        ])
        _write_raw_csv(str(tmp_path / "2024-12.csv"), [
            "timestamp,open,close,high,low,volume",
            "5000,9,9,9,9,9",
        ])

        result = last_timestamp_in_dir(str(tmp_path), full_scan=True)
        assert result == 99999

    def test_mixed_valid_and_empty_files(self, tmp_path):
        """Directory with a mix of header-only, empty, and valid CSV files."""
        (tmp_path / "2024-01-01.csv").write_text("")
        _write_raw_csv(str(tmp_path / "2024-01-02.csv"), [
            "timestamp,open,close,high,low,volume",
        ])
        _write_raw_csv(str(tmp_path / "2024-01-03.csv"), [
            "timestamp,open,close,high,low,volume",
            "7777,1,1,1,1,1",
        ])

        assert last_timestamp_in_dir(str(tmp_path)) == 7777


# ---------------------------------------------------------------------------
# Integration: write_partition -> _read_last_timestamp_from_file round-trip
# ---------------------------------------------------------------------------

class TestRoundTrip:
    """Verify that files written by write_partition are correctly read back."""

    def test_write_then_read_last_timestamp(self, tmp_path):
        path = str(tmp_path / "test.csv")
        df = _make_df([
            ["1000", "1", "1", "1", "1", "1"],
            ["2000", "2", "2", "2", "2", "2"],
            ["3000", "3", "3", "3", "3", "3"],
        ])

        write_partition(df, path)
        assert _read_last_timestamp_from_file(path) == 3000

    def test_write_then_last_timestamp_in_dir(self, tmp_path):
        for name, ts in [("a.csv", "1000"), ("b.csv", "2000"), ("c.csv", "5000")]:
            path = str(tmp_path / name)
            df = _make_df([[ts, "1", "1", "1", "1", "1"]])
            write_partition(df, path)

        assert last_timestamp_in_dir(str(tmp_path)) == 5000

    def test_merge_then_read_last_timestamp(self, tmp_path):
        path = str(tmp_path / "test.csv")

        old = _make_df([["1000", "1", "1", "1", "1", "1"]])
        write_partition(old, path)

        new = _make_df([["5000", "5", "5", "5", "5", "5"]])
        write_partition(new, path)

        assert _read_last_timestamp_from_file(path) == 5000
