"""Comprehensive unit tests for the Partition class in candles_sync.candles_sync."""

from datetime import datetime, timedelta, timezone

import pytest

from candles_sync.candles_sync import Partition


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc(*args, **kwargs) -> datetime:
    """Shorthand for creating a timezone-aware UTC datetime."""
    return datetime(*args, **kwargs, tzinfo=timezone.utc)


def _to_ms(dt: datetime) -> int:
    """Convert a datetime to milliseconds since epoch."""
    return int(dt.timestamp() * 1000)


# ---------------------------------------------------------------------------
# 1. from_timestamp — one test per timeframe
# ---------------------------------------------------------------------------

class TestFromTimestamp:
    """Partition.from_timestamp for each timeframe."""

    def test_1m_midday(self):
        """1m partitions are daily; a midday timestamp yields that day's partition."""
        dt = _utc(2024, 3, 15, 13, 45, 30)
        p = Partition.from_timestamp("1m", _to_ms(dt))

        assert p.timeframe == "1m"
        assert p.name == "2024-03-15"
        assert p.start == _utc(2024, 3, 15)
        assert p.end == _utc(2024, 3, 16) - timedelta(milliseconds=1)

    def test_1m_midnight_exact(self):
        """Exact midnight belongs to that day, not the previous one."""
        dt = _utc(2024, 1, 1, 0, 0, 0)
        p = Partition.from_timestamp("1m", _to_ms(dt))

        assert p.name == "2024-01-01"
        assert p.start == _utc(2024, 1, 1)

    def test_1m_last_millisecond_of_day(self):
        """Last ms of the day still belongs to that day's partition."""
        dt = _utc(2024, 6, 30, 23, 59, 59, 999000)
        p = Partition.from_timestamp("1m", _to_ms(dt))

        assert p.name == "2024-06-30"

    def test_1h_middle_of_month(self):
        """1h partitions are monthly; mid-month timestamp yields that month."""
        dt = _utc(2023, 7, 18, 9, 0, 0)
        p = Partition.from_timestamp("1h", _to_ms(dt))

        assert p.timeframe == "1h"
        assert p.name == "2023-07"
        assert p.start == _utc(2023, 7, 1)
        assert p.end == _utc(2023, 8, 1) - timedelta(milliseconds=1)

    def test_1h_first_moment_of_month(self):
        dt = _utc(2025, 1, 1, 0, 0, 0)
        p = Partition.from_timestamp("1h", _to_ms(dt))

        assert p.name == "2025-01"
        assert p.start == _utc(2025, 1, 1)

    def test_1D_midyear(self):
        """1D partitions are yearly; a midyear timestamp yields that year."""
        dt = _utc(2022, 6, 15, 12, 0, 0)
        p = Partition.from_timestamp("1D", _to_ms(dt))

        assert p.timeframe == "1D"
        assert p.name == "2022"
        assert p.start == _utc(2022, 1, 1)
        assert p.end == _utc(2023, 1, 1) - timedelta(milliseconds=1)

    def test_1D_january_first(self):
        dt = _utc(2020, 1, 1)
        p = Partition.from_timestamp("1D", _to_ms(dt))

        assert p.name == "2020"
        assert p.start == _utc(2020, 1, 1)

    def test_1D_december_31(self):
        dt = _utc(2020, 12, 31, 23, 59, 59)
        p = Partition.from_timestamp("1D", _to_ms(dt))

        assert p.name == "2020"


# ---------------------------------------------------------------------------
# 2. from_name — one test per timeframe
# ---------------------------------------------------------------------------

class TestFromName:
    """Partition.from_name for each timeframe."""

    def test_1m_day_name(self):
        p = Partition.from_name("1m", "2024-03-15")

        assert p.timeframe == "1m"
        assert p.name == "2024-03-15"
        assert p.start == _utc(2024, 3, 15)
        assert p.end == _utc(2024, 3, 16) - timedelta(milliseconds=1)

    def test_1h_month_name(self):
        p = Partition.from_name("1h", "2023-07")

        assert p.timeframe == "1h"
        assert p.name == "2023-07"
        assert p.start == _utc(2023, 7, 1)
        assert p.end == _utc(2023, 8, 1) - timedelta(milliseconds=1)

    def test_1D_year_name(self):
        p = Partition.from_name("1D", "2022")

        assert p.timeframe == "1D"
        assert p.name == "2022"
        assert p.start == _utc(2022, 1, 1)
        assert p.end == _utc(2023, 1, 1) - timedelta(milliseconds=1)

    def test_from_name_invalid_timeframe_raises(self):
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            Partition.from_name("5m", "2024-01-01")


# ---------------------------------------------------------------------------
# 3. Round-trip: from_timestamp -> name -> from_name
# ---------------------------------------------------------------------------

class TestRoundTrip:
    """from_timestamp produces a name that from_name converts back identically."""

    @pytest.mark.parametrize("tf, ts_dt", [
        ("1m", _utc(2024, 2, 29, 15, 30)),    # leap day
        ("1m", _utc(2023, 12, 31, 23, 59)),
        ("1h", _utc(2024, 2, 15, 8, 0)),
        ("1h", _utc(2023, 12, 31, 23, 59)),
        ("1D", _utc(2024, 7, 4, 12, 0)),
        ("1D", _utc(2000, 1, 1, 0, 0)),
    ])
    def test_round_trip(self, tf, ts_dt):
        p1 = Partition.from_timestamp(tf, _to_ms(ts_dt))
        p2 = Partition.from_name(tf, p1.name)

        assert p1 == p2
        assert p1.start == p2.start
        assert p1.end == p2.end
        assert p1.name == p2.name
        assert p1.timeframe == p2.timeframe


# ---------------------------------------------------------------------------
# 4. all_between — multiple partitions
# ---------------------------------------------------------------------------

class TestAllBetween:
    """Partition.all_between covering a range of partitions."""

    def test_1m_three_days(self):
        parts = Partition.all_between("1m", _utc(2024, 1, 10), _utc(2024, 1, 12, 23, 59))

        names = [p.name for p in parts]
        assert names == ["2024-01-10", "2024-01-11", "2024-01-12"]

    def test_1h_three_months(self):
        parts = Partition.all_between("1h", _utc(2024, 1, 1), _utc(2024, 3, 31))

        names = [p.name for p in parts]
        assert names == ["2024-01", "2024-02", "2024-03"]

    def test_1D_three_years(self):
        parts = Partition.all_between("1D", _utc(2020, 1, 1), _utc(2022, 12, 31))

        names = [p.name for p in parts]
        assert names == ["2020", "2021", "2022"]

    def test_1m_across_month_boundary(self):
        """Verify daily partitions span across a month boundary."""
        parts = Partition.all_between("1m", _utc(2024, 1, 30), _utc(2024, 2, 2))

        names = [p.name for p in parts]
        assert names == ["2024-01-30", "2024-01-31", "2024-02-01", "2024-02-02"]

    def test_1h_across_year_boundary(self):
        parts = Partition.all_between("1h", _utc(2023, 11, 1), _utc(2024, 2, 15))

        names = [p.name for p in parts]
        assert names == ["2023-11", "2023-12", "2024-01", "2024-02"]

    def test_1D_across_decade_boundary(self):
        parts = Partition.all_between("1D", _utc(2019, 6, 1), _utc(2021, 3, 1))

        names = [p.name for p in parts]
        assert names == ["2019", "2020", "2021"]

    def test_start_mid_partition(self):
        """Starting in the middle of a partition still includes that partition."""
        parts = Partition.all_between("1m", _utc(2024, 1, 15, 18, 30), _utc(2024, 1, 17, 6, 0))

        names = [p.name for p in parts]
        assert names == ["2024-01-15", "2024-01-16", "2024-01-17"]


# ---------------------------------------------------------------------------
# 5. all_between — single partition
# ---------------------------------------------------------------------------

class TestAllBetweenSingle:
    """all_between returns exactly one partition when the range fits within one."""

    def test_1m_same_day(self):
        parts = Partition.all_between("1m", _utc(2024, 5, 20, 1, 0), _utc(2024, 5, 20, 23, 0))

        assert len(parts) == 1
        assert parts[0].name == "2024-05-20"

    def test_1h_same_month(self):
        parts = Partition.all_between("1h", _utc(2024, 3, 5), _utc(2024, 3, 28))

        assert len(parts) == 1
        assert parts[0].name == "2024-03"

    def test_1D_same_year(self):
        parts = Partition.all_between("1D", _utc(2024, 2, 1), _utc(2024, 11, 30))

        assert len(parts) == 1
        assert parts[0].name == "2024"

    def test_empty_when_start_after_end(self):
        """If start > end, no partitions are returned."""
        parts = Partition.all_between("1m", _utc(2024, 5, 20), _utc(2024, 5, 19))

        assert parts == []


# ---------------------------------------------------------------------------
# 6. next() — chaining across partition boundaries
# ---------------------------------------------------------------------------

class TestNext:
    """Partition.next() returns the immediately following partition."""

    def test_1m_next_day(self):
        p = Partition.from_name("1m", "2024-03-15")
        n = p.next()

        assert n.name == "2024-03-16"
        assert n.start == _utc(2024, 3, 16)

    def test_1m_next_chains_three(self):
        p = Partition.from_name("1m", "2024-03-15")
        chain = [p.name]
        for _ in range(3):
            p = p.next()
            chain.append(p.name)

        assert chain == ["2024-03-15", "2024-03-16", "2024-03-17", "2024-03-18"]

    def test_1h_next_month(self):
        p = Partition.from_name("1h", "2024-03")
        n = p.next()

        assert n.name == "2024-04"
        assert n.start == _utc(2024, 4, 1)

    def test_1D_next_year(self):
        p = Partition.from_name("1D", "2024")
        n = p.next()

        assert n.name == "2025"
        assert n.start == _utc(2025, 1, 1)

    def test_next_start_equals_previous_end_plus_1ms(self):
        """The next partition's start is exactly 1ms after the current partition's end."""
        for tf, name in [("1m", "2024-06-30"), ("1h", "2024-06"), ("1D", "2024")]:
            p = Partition.from_name(tf, name)
            n = p.next()
            assert n.start == p.end + timedelta(milliseconds=1)

    def test_next_across_month_boundary_1m(self):
        """Last day of month -> first day of next month."""
        p = Partition.from_name("1m", "2024-01-31")
        n = p.next()

        assert n.name == "2024-02-01"

    def test_next_across_year_boundary_1m(self):
        """Dec 31 -> Jan 1 of the following year."""
        p = Partition.from_name("1m", "2024-12-31")
        n = p.next()

        assert n.name == "2025-01-01"
        assert n.start == _utc(2025, 1, 1)

    def test_next_across_year_boundary_1h(self):
        """December -> January of the following year."""
        p = Partition.from_name("1h", "2024-12")
        n = p.next()

        assert n.name == "2025-01"
        assert n.start == _utc(2025, 1, 1)


# ---------------------------------------------------------------------------
# 7. Edge cases: year boundaries, month boundaries, leap years
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Edge cases around boundaries and calendar quirks."""

    def test_leap_year_feb_29(self):
        """Feb 29 is a valid daily partition in a leap year."""
        p = Partition.from_name("1m", "2024-02-29")

        assert p.start == _utc(2024, 2, 29)
        assert p.end == _utc(2024, 3, 1) - timedelta(milliseconds=1)

    def test_leap_year_feb_28_next_is_feb_29(self):
        p = Partition.from_name("1m", "2024-02-28")
        n = p.next()

        assert n.name == "2024-02-29"

    def test_non_leap_year_feb_28_next_is_mar_01(self):
        p = Partition.from_name("1m", "2023-02-28")
        n = p.next()

        assert n.name == "2023-03-01"

    def test_leap_year_february_monthly(self):
        """Monthly partition for February in a leap year ends correctly."""
        p = Partition.from_name("1h", "2024-02")

        assert p.start == _utc(2024, 2, 1)
        assert p.end == _utc(2024, 3, 1) - timedelta(milliseconds=1)

    def test_non_leap_year_february_monthly(self):
        p = Partition.from_name("1h", "2023-02")

        assert p.start == _utc(2023, 2, 1)
        assert p.end == _utc(2023, 3, 1) - timedelta(milliseconds=1)

    def test_end_is_inclusive_last_millisecond(self):
        """End time should be the last millisecond before the next partition starts."""
        p = Partition.from_name("1m", "2024-06-15")
        expected_end = _utc(2024, 6, 16) - timedelta(milliseconds=1)

        assert p.end == expected_end
        # The end microsecond value should be 999000 (999 ms)
        assert p.end.microsecond == 999000

    def test_all_between_leap_year_feb(self):
        """All days in Feb 2024 (leap year) should produce 29 partitions."""
        parts = Partition.all_between("1m", _utc(2024, 2, 1), _utc(2024, 2, 29, 23, 59))

        assert len(parts) == 29
        assert parts[0].name == "2024-02-01"
        assert parts[-1].name == "2024-02-29"

    def test_all_between_non_leap_year_feb(self):
        """All days in Feb 2023 (non-leap) should produce 28 partitions."""
        parts = Partition.all_between("1m", _utc(2023, 2, 1), _utc(2023, 2, 28, 23, 59))

        assert len(parts) == 28
        assert parts[-1].name == "2023-02-28"

    def test_epoch_timestamp(self):
        """Timestamp 0 (Unix epoch) should produce the 1970-01-01 partition."""
        p = Partition.from_timestamp("1m", 0)

        assert p.name == "1970-01-01"
        assert p.start == _utc(1970, 1, 1)

    def test_epoch_monthly(self):
        p = Partition.from_timestamp("1h", 0)

        assert p.name == "1970-01"

    def test_epoch_yearly(self):
        p = Partition.from_timestamp("1D", 0)

        assert p.name == "1970"

    def test_year_2000_boundary(self):
        """Y2K boundary should work correctly."""
        p = Partition.from_name("1m", "1999-12-31")
        n = p.next()

        assert n.name == "2000-01-01"


# ---------------------------------------------------------------------------
# 8. Ordering (frozen dataclass with order=True)
# ---------------------------------------------------------------------------

class TestOrdering:
    """Partition supports comparison operators due to order=True on the dataclass."""

    def test_same_timeframe_sorted_by_fields(self):
        """Partitions of the same timeframe are ordered by their fields (timeframe, name, start, end)."""
        a = Partition.from_name("1m", "2024-01-01")
        b = Partition.from_name("1m", "2024-01-02")
        c = Partition.from_name("1m", "2024-01-03")

        assert a < b < c
        assert c > b > a

    def test_equality(self):
        a = Partition.from_name("1m", "2024-01-01")
        b = Partition.from_name("1m", "2024-01-01")

        assert a == b
        assert not (a != b)

    def test_inequality_different_names(self):
        a = Partition.from_name("1m", "2024-01-01")
        b = Partition.from_name("1m", "2024-01-02")

        assert a != b

    def test_sorted_list(self):
        """A shuffled list of partitions sorts correctly."""
        names = ["2024-01-05", "2024-01-01", "2024-01-03", "2024-01-02", "2024-01-04"]
        parts = [Partition.from_name("1m", n) for n in names]

        sorted_parts = sorted(parts)
        sorted_names = [p.name for p in sorted_parts]

        assert sorted_names == ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]

    def test_le_ge(self):
        a = Partition.from_name("1h", "2024-01")
        b = Partition.from_name("1h", "2024-02")

        assert a <= b
        assert a <= a
        assert b >= a
        assert b >= b

    def test_monthly_ordering(self):
        parts = [
            Partition.from_name("1h", "2024-12"),
            Partition.from_name("1h", "2024-01"),
            Partition.from_name("1h", "2024-06"),
        ]
        sorted_parts = sorted(parts)

        assert [p.name for p in sorted_parts] == ["2024-01", "2024-06", "2024-12"]

    def test_yearly_ordering(self):
        parts = [
            Partition.from_name("1D", "2025"),
            Partition.from_name("1D", "2020"),
            Partition.from_name("1D", "2023"),
        ]
        sorted_parts = sorted(parts)

        assert [p.name for p in sorted_parts] == ["2020", "2023", "2025"]


# ---------------------------------------------------------------------------
# 9. Frozen dataclass behavior
# ---------------------------------------------------------------------------

class TestFrozen:
    """Partition instances are immutable (frozen=True)."""

    def test_cannot_set_attribute(self):
        p = Partition.from_name("1m", "2024-01-01")
        with pytest.raises(AttributeError):
            p.name = "2024-01-02"

    def test_cannot_set_start(self):
        p = Partition.from_name("1m", "2024-01-01")
        with pytest.raises(AttributeError):
            p.start = _utc(2024, 2, 1)

    def test_hashable(self):
        """Frozen dataclasses are hashable and can be used in sets."""
        a = Partition.from_name("1m", "2024-01-01")
        b = Partition.from_name("1m", "2024-01-01")
        c = Partition.from_name("1m", "2024-01-02")

        s = {a, b, c}
        assert len(s) == 2


# ---------------------------------------------------------------------------
# 10. Invalid input handling
# ---------------------------------------------------------------------------

class TestInvalidInput:
    """Error cases for invalid timeframes and names."""

    def test_invalid_timeframe_from_timestamp(self):
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            Partition.from_timestamp("5m", 1000000)

    def test_invalid_timeframe_from_name(self):
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            Partition.from_name("2h", "2024-01")

    def test_invalid_timeframe_all_between(self):
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            Partition.all_between("3D", _utc(2024, 1, 1), _utc(2024, 1, 2))

    def test_malformed_name_1m(self):
        with pytest.raises((ValueError, KeyError)):
            Partition.from_name("1m", "not-a-date")

    def test_malformed_name_1h(self):
        with pytest.raises((ValueError, KeyError)):
            Partition.from_name("1h", "bad")

    def test_malformed_name_1D(self):
        with pytest.raises((ValueError, KeyError)):
            Partition.from_name("1D", "nope")
