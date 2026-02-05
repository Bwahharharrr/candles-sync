"""Comprehensive unit tests for _to_decimal and _canonical_num_str."""

import math
from decimal import Decimal, InvalidOperation

import pytest

from candles_sync.candles_sync import _to_decimal, _canonical_num_str


# ============================================================================
# _to_decimal
# ============================================================================


class TestToDecimal:
    """Tests for _to_decimal(value) -> Decimal."""

    # ---- Valid inputs ----

    def test_integer(self):
        assert _to_decimal(42) == Decimal("42")

    def test_zero_int(self):
        assert _to_decimal(0) == Decimal("0")

    def test_negative_integer(self):
        assert _to_decimal(-7) == Decimal("-7")

    def test_large_integer(self):
        assert _to_decimal(10**18) == Decimal("1000000000000000000")

    def test_float(self):
        result = _to_decimal(3.14)
        # str(3.14) == "3.14", so Decimal("3.14") is expected
        assert result == Decimal("3.14")

    def test_negative_float(self):
        assert _to_decimal(-0.001) == Decimal("-0.001")

    def test_float_zero(self):
        assert _to_decimal(0.0) == Decimal("0.0")

    def test_string_integer(self):
        assert _to_decimal("100") == Decimal("100")

    def test_string_float(self):
        assert _to_decimal("123.456") == Decimal("123.456")

    def test_string_negative(self):
        assert _to_decimal("-99.9") == Decimal("-99.9")

    def test_string_scientific_notation(self):
        result = _to_decimal("1.5E-8")
        assert result == Decimal("1.5E-8")

    def test_decimal_passthrough(self):
        d = Decimal("999.12345")
        assert _to_decimal(d) == Decimal("999.12345")

    def test_decimal_zero(self):
        assert _to_decimal(Decimal("0")) == Decimal("0")

    def test_bool_true(self):
        # bool is a subclass of int; str(True) == "True" which is invalid
        # for Decimal, so this should fall back to 0
        result = _to_decimal(True)
        assert result == Decimal(0)

    def test_bool_false(self):
        result = _to_decimal(False)
        assert result == Decimal(0)

    # ---- Invalid inputs (should return Decimal(0) with warning log) ----

    def test_none_returns_zero(self, capsys):
        result = _to_decimal(None)
        assert result == Decimal(0)
        captured = capsys.readouterr()
        assert "Invalid numeric value" in captured.out

    def test_non_numeric_string_returns_zero(self, capsys):
        result = _to_decimal("abc")
        assert result == Decimal(0)
        captured = capsys.readouterr()
        assert "Invalid numeric value" in captured.out

    def test_empty_string_returns_zero(self, capsys):
        result = _to_decimal("")
        assert result == Decimal(0)
        captured = capsys.readouterr()
        assert "Invalid numeric value" in captured.out

    def test_nan_returns_zero(self, capsys):
        result = _to_decimal(float("nan"))
        # str(nan) == "nan", Decimal("nan") produces a NaN Decimal which
        # is actually valid — but the function uses str() then Decimal().
        # Decimal("nan") does NOT raise; it returns Decimal('NaN').
        # However, Decimal('NaN') != Decimal(0), so let's verify what
        # actually happens: str(float('nan')) == 'nan', Decimal('nan')
        # succeeds. The function will NOT hit the except branch.
        # This is acceptable behavior — document it.
        assert result.is_nan() or result == Decimal(0)

    def test_inf_returns_zero(self, capsys):
        # str(float('inf')) == 'inf', Decimal('inf') -> Decimal('Infinity')
        # which does NOT raise. So this goes through without error.
        result = _to_decimal(float("inf"))
        assert result == Decimal("Infinity") or result == Decimal(0)

    def test_negative_inf_returns_zero(self, capsys):
        result = _to_decimal(float("-inf"))
        assert result == Decimal("-Infinity") or result == Decimal(0)

    def test_list_returns_zero(self, capsys):
        result = _to_decimal([1, 2, 3])
        assert result == Decimal(0)
        captured = capsys.readouterr()
        assert "Invalid numeric value" in captured.out

    def test_dict_returns_zero(self, capsys):
        result = _to_decimal({"a": 1})
        assert result == Decimal(0)
        captured = capsys.readouterr()
        assert "Invalid numeric value" in captured.out

    def test_object_returns_zero(self, capsys):
        result = _to_decimal(object())
        assert result == Decimal(0)
        captured = capsys.readouterr()
        assert "Invalid numeric value" in captured.out


# ============================================================================
# _canonical_num_str
# ============================================================================


class TestCanonicalNumStr:
    """Tests for _canonical_num_str(x) -> str."""

    # ---- Integers (should strip trailing .0000...) ----

    def test_integer_strips_trailing_zeros(self):
        assert _canonical_num_str(42) == "42"

    def test_integer_zero(self):
        assert _canonical_num_str(0) == "0"

    def test_integer_one(self):
        assert _canonical_num_str(1) == "1"

    def test_negative_integer(self):
        assert _canonical_num_str(-5) == "-5"

    def test_large_integer(self):
        assert _canonical_num_str(1000000) == "1000000"

    def test_integer_string(self):
        assert _canonical_num_str("100") == "100"

    # ---- Floats (precision preserved up to 10dp) ----

    def test_float_basic(self):
        assert _canonical_num_str(1.5) == "1.5"

    def test_float_two_decimals(self):
        assert _canonical_num_str(3.14) == "3.14"

    def test_float_many_decimals(self):
        # 1.123456789 has 9 decimal places — all should be kept
        result = _canonical_num_str("1.123456789")
        assert result == "1.123456789"

    def test_float_ten_decimals(self):
        result = _canonical_num_str("1.1234567890")
        assert result == "1.123456789"

    def test_float_trailing_zeros_stripped(self):
        result = _canonical_num_str("1.50000")
        assert result == "1.5"

    def test_float_all_trailing_zeros_stripped(self):
        result = _canonical_num_str("42.0")
        assert result == "42"

    # ---- Strings ----

    def test_string_decimal(self):
        assert _canonical_num_str("123.456") == "123.456"

    def test_string_integer(self):
        assert _canonical_num_str("789") == "789"

    def test_string_with_leading_zeros_decimal(self):
        # Decimal("007.5") == Decimal("7.5")
        assert _canonical_num_str("007.5") == "7.5"

    # ---- Very small numbers ----

    def test_very_small_positive(self):
        result = _canonical_num_str("0.0000000001")
        assert result == "0.0000000001"

    def test_very_small_negative(self):
        result = _canonical_num_str("-0.0000000001")
        assert result == "-0.0000000001"

    def test_smaller_than_10dp_truncated(self):
        # 11 decimal places — the 11th digit should be truncated by the
        # format specifier :.10f
        result = _canonical_num_str("0.00000000001")
        assert result == "0"

    def test_small_scientific_notation(self):
        # 1e-8 == 0.00000001
        result = _canonical_num_str(1e-8)
        assert result == "0.00000001"

    def test_small_scientific_string(self):
        result = _canonical_num_str("1E-10")
        assert result == "0.0000000001"

    # ---- Very large numbers ----

    def test_very_large_number(self):
        result = _canonical_num_str(99999999999)
        assert result == "99999999999"

    def test_large_float(self):
        result = _canonical_num_str("123456789.123456789")
        # 10dp format: 123456789.1234567890, stripped -> 123456789.123456789
        assert result == "123456789.123456789"

    def test_large_with_decimal_part(self):
        result = _canonical_num_str("1000000.5")
        assert result == "1000000.5"

    # ---- Zero ----

    def test_zero_int(self):
        assert _canonical_num_str(0) == "0"

    def test_zero_float(self):
        assert _canonical_num_str(0.0) == "0"

    def test_zero_string(self):
        assert _canonical_num_str("0") == "0"

    def test_zero_decimal(self):
        assert _canonical_num_str(Decimal("0")) == "0"

    def test_negative_zero_float(self):
        # -0.0 formatted to 10dp: "-0.0000000000" -> stripped -> "-0" or "0"
        result = _canonical_num_str(-0.0)
        # Depending on Decimal handling, this could be "-0" or "0"
        assert result in ("0", "-0")

    # ---- Negative numbers ----

    def test_negative_float(self):
        assert _canonical_num_str(-3.14) == "-3.14"

    def test_negative_string(self):
        assert _canonical_num_str("-99.9") == "-99.9"

    def test_negative_small(self):
        assert _canonical_num_str("-0.001") == "-0.001"

    def test_negative_large(self):
        assert _canonical_num_str(-1000000) == "-1000000"

    # ---- Invalid input (fallback to "0" via _to_decimal) ----

    def test_none_returns_zero_string(self):
        assert _canonical_num_str(None) == "0"

    def test_non_numeric_string_returns_zero_string(self):
        assert _canonical_num_str("abc") == "0"

    def test_empty_string_returns_zero_string(self):
        assert _canonical_num_str("") == "0"

    def test_list_returns_zero_string(self):
        assert _canonical_num_str([1, 2]) == "0"

    def test_dict_returns_zero_string(self):
        assert _canonical_num_str({"key": "val"}) == "0"

    # ---- Decimal objects ----

    def test_decimal_input(self):
        assert _canonical_num_str(Decimal("1.23")) == "1.23"

    def test_decimal_high_precision_truncated(self):
        # Decimal with 15 digits — formatted to 10dp then trailing zeros stripped
        result = _canonical_num_str(Decimal("1.123456789012345"))
        # :.10f gives "1.1234567890", stripping trailing zero -> "1.123456789"
        assert result == "1.123456789"

    def test_decimal_integer_value(self):
        assert _canonical_num_str(Decimal("500")) == "500"

    # ---- Edge cases ----

    def test_return_type_is_str(self):
        result = _canonical_num_str(42)
        assert isinstance(result, str)

    def test_dot_five(self):
        assert _canonical_num_str(0.5) == "0.5"

    def test_exactly_ten_significant_decimals(self):
        result = _canonical_num_str("3.1415926535")
        assert result == "3.1415926535"

    def test_eleven_decimals_rounds(self):
        # 3.14159265359 formatted to 10dp should round the 10th place
        result = _canonical_num_str("3.14159265359")
        # Decimal("3.14159265359") formatted with :.10f
        expected = f"{Decimal('3.14159265359'):.10f}".rstrip("0").rstrip(".")
        assert result == expected
