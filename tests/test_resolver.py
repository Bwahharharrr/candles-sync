"""Unit tests for candles_sync/providers/eodhd/resolver.py."""

import pytest
from unittest.mock import MagicMock, patch

from candles_sync.providers.eodhd.resolver import (
    EODHDResolver,
    SymbolNotFoundError,
    AmbiguousSymbolError,
)


@pytest.fixture
def mock_cache():
    """Create a mock MetadataCache for all resolver tests."""
    return MagicMock()


@pytest.fixture
def resolver(mock_cache):
    """Create an EODHDResolver with a mocked MetadataCache."""
    return EODHDResolver(cache=mock_cache)


# ============================================================================
# _parse_symbol
# ============================================================================


class TestParseSymbol:
    """Tests for EODHDResolver._parse_symbol(symbol)."""

    def test_full_symbol_returns_code_and_exchange(self, resolver):
        code, exchange = resolver._parse_symbol("MCD.US")
        assert code == "MCD"
        assert exchange == "US"

    def test_bare_symbol_returns_code_and_none(self, resolver):
        code, exchange = resolver._parse_symbol("MCD")
        assert code == "MCD"
        assert exchange is None

    def test_full_symbol_uppercased(self, resolver):
        code, exchange = resolver._parse_symbol("mcd.us")
        assert code == "MCD"
        assert exchange == "US"

    def test_bare_symbol_uppercased(self, resolver):
        code, exchange = resolver._parse_symbol("aapl")
        assert code == "AAPL"
        assert exchange is None

    def test_symbol_with_whitespace_stripped(self, resolver):
        code, exchange = resolver._parse_symbol("  MCD.US  ")
        assert code == "MCD"
        assert exchange == "US"

    def test_symbol_with_multiple_dots_splits_on_last(self, resolver):
        # e.g., "BRK.B.US" should split as ("BRK.B", "US")
        code, exchange = resolver._parse_symbol("BRK.B.US")
        assert code == "BRK.B"
        assert exchange == "US"


# ============================================================================
# resolve
# ============================================================================


class TestResolve:
    """Tests for EODHDResolver.resolve(symbol, interactive, validate)."""

    def test_full_symbol_returned_as_is(self, resolver, mock_cache):
        mock_cache.symbol_exists.return_value = True
        result = resolver.resolve("MCD.US")
        assert result == "MCD.US"

    def test_full_symbol_validate_false_skips_cache_check(self, resolver, mock_cache):
        result = resolver.resolve("MCD.US", validate=False)
        assert result == "MCD.US"
        mock_cache.symbol_exists.assert_not_called()

    def test_bare_symbol_single_match_auto_resolves(self, resolver, mock_cache):
        mock_cache.lookup_symbol.return_value = ["US"]
        result = resolver.resolve("MCD", interactive=False)
        assert result == "MCD.US"

    def test_bare_symbol_no_matches_raises_not_found(self, resolver, mock_cache):
        mock_cache.lookup_symbol.return_value = None
        with pytest.raises(SymbolNotFoundError) as exc_info:
            resolver.resolve("ZZZZZZ", interactive=False)
        assert exc_info.value.symbol == "ZZZZZZ"

    def test_bare_symbol_empty_list_raises_not_found(self, resolver, mock_cache):
        mock_cache.lookup_symbol.return_value = []
        with pytest.raises(SymbolNotFoundError):
            resolver.resolve("NOPE", interactive=False)

    def test_bare_symbol_multiple_matches_non_interactive_raises_ambiguous(
        self, resolver, mock_cache
    ):
        mock_cache.lookup_symbol.return_value = ["US", "LSE", "F"]
        with pytest.raises(AmbiguousSymbolError) as exc_info:
            resolver.resolve("MCD", interactive=False)
        assert exc_info.value.symbol == "MCD"
        assert exc_info.value.exchanges == ["US", "LSE", "F"]

    def test_full_symbol_not_found_on_exchange_raises_not_found(
        self, resolver, mock_cache
    ):
        mock_cache.symbol_exists.return_value = False
        mock_cache.lookup_symbol.return_value = None
        with pytest.raises(SymbolNotFoundError):
            resolver.resolve("MCD.XX", validate=True)

    def test_full_symbol_not_found_but_exists_elsewhere(self, resolver, mock_cache):
        mock_cache.symbol_exists.return_value = False
        mock_cache.lookup_symbol.return_value = ["US", "LSE"]
        with pytest.raises(SymbolNotFoundError) as exc_info:
            resolver.resolve("MCD.XX", validate=True)
        assert "not found on exchange" in str(exc_info.value).lower()
        assert "US" in str(exc_info.value)


# ============================================================================
# SymbolNotFoundError
# ============================================================================


class TestSymbolNotFoundError:
    """Tests for the SymbolNotFoundError exception."""

    def test_default_message(self):
        err = SymbolNotFoundError("AAPL")
        assert err.symbol == "AAPL"
        assert "AAPL" in str(err)

    def test_custom_message(self):
        err = SymbolNotFoundError("AAPL", message="custom error")
        assert str(err) == "custom error"
        assert err.symbol == "AAPL"


# ============================================================================
# AmbiguousSymbolError
# ============================================================================


class TestAmbiguousSymbolError:
    """Tests for the AmbiguousSymbolError exception."""

    def test_default_message(self):
        err = AmbiguousSymbolError("MCD", ["US", "LSE"])
        assert err.symbol == "MCD"
        assert err.exchanges == ["US", "LSE"]
        assert "MCD" in str(err)
        assert "US" in str(err)
        assert "LSE" in str(err)

    def test_custom_message(self):
        err = AmbiguousSymbolError("MCD", ["US"], message="custom ambiguous")
        assert str(err) == "custom ambiguous"
        assert err.symbol == "MCD"
        assert err.exchanges == ["US"]
