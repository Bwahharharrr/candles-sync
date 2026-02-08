"""Unit tests for all exchange adapters."""

import os
from datetime import datetime, timezone

import pytest

from candles_sync.adapters.bitfinex import BitfinexAdapter
from candles_sync.adapters.binance import BinanceAdapter
from candles_sync.adapters.yahoo import YahooAdapter
from candles_sync.adapters.eodhd import EODHDAdapter
from candles_sync.adapters import get_adapter, list_adapters
from candles_sync.adapters.base import Candle


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def bitfinex():
    return BitfinexAdapter()


@pytest.fixture
def binance():
    return BinanceAdapter()


@pytest.fixture
def yahoo():
    return YahooAdapter()


@pytest.fixture
def eodhd_token():
    """Set EODHD_API_TOKEN for tests that need it."""
    os.environ["EODHD_API_TOKEN"] = "test-token"
    yield "test-token"
    # cleanup handled by conftest env_cleanup


@pytest.fixture
def eodhd():
    return EODHDAdapter()


# ===========================================================================
# BitfinexAdapter
# ===========================================================================


class TestBitfinexAdapter:
    def test_name_property(self, bitfinex):
        assert bitfinex.name == "BITFINEX"

    def test_adapter_name_class_attribute(self):
        assert BitfinexAdapter.ADAPTER_NAME == "BITFINEX"

    def test_get_supported_timeframes(self, bitfinex):
        tf = bitfinex.get_supported_timeframes()
        assert tf == {"1m": "1m", "1h": "1h", "1D": "1D"}

    def test_format_symbol_adds_prefix(self, bitfinex):
        assert bitfinex.format_symbol("BTCUSD") == "tBTCUSD"

    def test_format_symbol_already_prefixed(self, bitfinex):
        assert bitfinex.format_symbol("tBTCUSD") == "tBTCUSD"

    def test_format_symbol_lowercase_normalized(self, bitfinex):
        assert bitfinex.format_symbol("btcusd") == "tBTCUSD"

    def test_format_symbol_uppercase_prefixed_normalized(self, bitfinex):
        assert bitfinex.format_symbol("TBTCUSD") == "tBTCUSD"

    def test_build_url(self, bitfinex):
        url = bitfinex.build_url("tBTCUSD", "1h")
        assert url == "https://api-pub.bitfinex.com/v2/candles/trade:1h:tBTCUSD/hist"

    def test_build_params_minimal(self, bitfinex):
        params = bitfinex.build_params(start=1000000)
        assert params["start"] == 1000000
        assert params["sort"] == 1
        assert params["limit"] == 10_000  # default API_LIMIT

    def test_build_params_with_end_and_limit(self, bitfinex):
        params = bitfinex.build_params(start=1000, end=2000, limit=500)
        assert params["start"] == 1000
        assert params["end"] == 2000
        assert params["limit"] == 500
        assert params["sort"] == 1

    def test_build_fetch_params_delegates_to_build_params(self, bitfinex):
        """Bitfinex does not override build_fetch_params, so it equals build_params."""
        fetch_params = bitfinex.build_fetch_params("tBTCUSD", "1h", start=5000, end=6000, limit=100)
        base_params = bitfinex.build_params(start=5000, end=6000, limit=100)
        assert fetch_params == base_params

    def test_parse_response_valid(self, bitfinex):
        # Bitfinex: [mts, open, close, high, low, volume]
        data = [
            [1609459200000, 29000, 29100, 29200, 28900, 100],
            [1609545600000, 29100, 29200, 29300, 29000, 150],
        ]
        candles = bitfinex.parse_response(data)
        assert len(candles) == 2
        assert all(isinstance(c, Candle) for c in candles)

        # First candle
        c = candles[0]
        assert c.timestamp == 1609459200000
        assert c.open == 29000
        assert c.close == 29100  # index 2
        assert c.high == 29200   # index 3
        assert c.low == 28900    # index 4
        assert c.volume == 100

    def test_parse_response_sorted_ascending(self, bitfinex):
        # Feed in descending order -- parser should sort ascending
        data = [
            [1609545600000, 29100, 29200, 29300, 29000, 150],
            [1609459200000, 29000, 29100, 29200, 28900, 100],
        ]
        candles = bitfinex.parse_response(data)
        assert candles[0].timestamp < candles[1].timestamp

    def test_parse_response_empty(self, bitfinex):
        assert bitfinex.parse_response([]) == []
        assert bitfinex.parse_response(None) == []
        assert bitfinex.parse_response("error") == []

    def test_parse_response_skips_short_rows(self, bitfinex):
        data = [
            [1609459200000, 29000, 29100],  # too short
            [1609459200000, 29000, 29100, 29200, 28900, 100],  # valid
        ]
        candles = bitfinex.parse_response(data)
        assert len(candles) == 1

    def test_is_rate_limited(self, bitfinex):
        assert bitfinex.is_rate_limited(429) is True
        assert bitfinex.is_rate_limited(200) is False
        assert bitfinex.is_rate_limited(418) is False


# ===========================================================================
# BinanceAdapter
# ===========================================================================


class TestBinanceAdapter:
    def test_name_property(self, binance):
        assert binance.name == "BINANCE"

    def test_adapter_name_class_attribute(self):
        assert BinanceAdapter.ADAPTER_NAME == "BINANCE"

    def test_get_supported_timeframes(self, binance):
        tf = binance.get_supported_timeframes()
        assert tf == {"1m": "1m", "1h": "1h", "1D": "1d"}

    def test_format_symbol_uppercase_no_separators(self, binance):
        assert binance.format_symbol("btc-usdt") == "BTCUSDT"
        assert binance.format_symbol("eth_usdt") == "ETHUSDT"
        assert binance.format_symbol("BTC/USDT") == "BTCUSDT"
        assert binance.format_symbol("BTCUSDT") == "BTCUSDT"

    def test_build_url(self, binance):
        url = binance.build_url("BTCUSDT", "1h")
        assert url == "https://api.binance.com/api/v3/klines"

    def test_build_params_minimal(self, binance):
        params = binance.build_params(start=1000000)
        assert params["startTime"] == 1000000
        assert params["limit"] == 1000  # default API_LIMIT

    def test_build_params_with_end_and_limit(self, binance):
        params = binance.build_params(start=1000, end=2000, limit=500)
        assert params["startTime"] == 1000
        assert params["endTime"] == 2000
        assert params["limit"] == 500

    def test_build_params_limit_capped(self, binance):
        params = binance.build_params(start=1000, limit=9999)
        assert params["limit"] == 1000  # capped at API_LIMIT

    def test_build_fetch_params_includes_symbol_and_interval(self, binance):
        params = binance.build_fetch_params("BTCUSDT", "1h", start=5000, end=6000, limit=100)
        assert params["symbol"] == "BTCUSDT"
        assert params["interval"] == "1h"
        assert params["startTime"] == 5000
        assert params["endTime"] == 6000
        assert params["limit"] == 100

    def test_parse_response_valid(self, binance):
        # Binance: [open_time, open, high, low, close, volume, close_time, ...]
        data = [
            [1609459200000, "29000", "29200", "28900", "29100", "100", 1609462799999, "0", 0, "0", "0", "0"],
            [1609545600000, "29100", "29300", "29000", "29200", "150", 1609549199999, "0", 0, "0", "0", "0"],
        ]
        candles = binance.parse_response(data)
        assert len(candles) == 2

        c = candles[0]
        assert c.timestamp == 1609459200000
        assert c.open == 29000
        assert c.high == 29200
        assert c.low == 28900
        assert c.close == 29100
        assert c.volume == 100

    def test_parse_response_sorted_ascending(self, binance):
        data = [
            [1609545600000, "29100", "29300", "29000", "29200", "150", 0, "0", 0, "0", "0", "0"],
            [1609459200000, "29000", "29200", "28900", "29100", "100", 0, "0", 0, "0", "0", "0"],
        ]
        candles = binance.parse_response(data)
        assert candles[0].timestamp < candles[1].timestamp

    def test_parse_response_empty(self, binance):
        assert binance.parse_response([]) == []
        assert binance.parse_response(None) == []
        assert binance.parse_response("error") == []

    def test_parse_response_skips_short_rows(self, binance):
        data = [
            [1609459200000, "29000"],  # too short
            [1609459200000, "29000", "29200", "28900", "29100", "100"],  # valid
        ]
        candles = binance.parse_response(data)
        assert len(candles) == 1

    def test_is_rate_limited(self, binance):
        assert binance.is_rate_limited(429) is True
        assert binance.is_rate_limited(418) is True  # Binance IP ban
        assert binance.is_rate_limited(200) is False
        assert binance.is_rate_limited(500) is False


# ===========================================================================
# YahooAdapter
# ===========================================================================


class TestYahooAdapter:
    def test_name_property(self, yahoo):
        assert yahoo.name == "YAHOO"

    def test_adapter_name_class_attribute(self):
        assert YahooAdapter.ADAPTER_NAME == "YAHOO"

    def test_get_supported_timeframes(self, yahoo):
        tf = yahoo.get_supported_timeframes()
        assert tf == {"1m": "1m", "1h": "1h", "1D": "1d"}

    def test_format_symbol_passthrough(self, yahoo):
        assert yahoo.format_symbol("AAPL") == "AAPL"
        assert yahoo.format_symbol("BTC-USD") == "BTC-USD"
        assert yahoo.format_symbol("^GSPC") == "^GSPC"

    def test_build_url(self, yahoo):
        url = yahoo.build_url("AAPL", "1d")
        assert url == "https://query1.finance.yahoo.com/v8/finance/chart/AAPL"

    def test_build_params_converts_ms_to_seconds(self, yahoo):
        params = yahoo.build_params(start=1609459200000, end=1609545600000)
        assert params["period1"] == 1609459200  # seconds
        assert params["period2"] == 1609545600

    def test_build_params_default_end(self, yahoo):
        params = yahoo.build_params(start=1609459200000)
        assert params["period1"] == 1609459200
        assert "period2" in params  # defaults to current time

    def test_build_fetch_params_includes_interval(self, yahoo):
        params = yahoo.build_fetch_params("AAPL", "1d", start=1609459200000, end=1609545600000)
        assert params["interval"] == "1d"
        assert params["period1"] == 1609459200
        assert params["period2"] == 1609545600

    def test_parse_response_valid(self, yahoo):
        data = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1609459200, 1609545600],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [130.0, 131.0],
                                    "high": [133.0, 134.0],
                                    "low": [129.0, 130.0],
                                    "close": [132.0, 133.0],
                                    "volume": [100000, 120000],
                                }
                            ]
                        },
                    }
                ]
            }
        }
        candles = yahoo.parse_response(data)
        assert len(candles) == 2

        c = candles[0]
        assert c.timestamp == 1609459200 * 1000  # converted to ms
        assert c.open == 130.0
        assert c.high == 133.0
        assert c.low == 129.0
        assert c.close == 132.0
        assert c.volume == 100000

    def test_parse_response_sorted_ascending(self, yahoo):
        data = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1609545600, 1609459200],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [131.0, 130.0],
                                    "high": [134.0, 133.0],
                                    "low": [130.0, 129.0],
                                    "close": [133.0, 132.0],
                                    "volume": [120000, 100000],
                                }
                            ]
                        },
                    }
                ]
            }
        }
        candles = yahoo.parse_response(data)
        assert candles[0].timestamp < candles[1].timestamp

    def test_parse_response_skips_null_values(self, yahoo):
        data = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1609459200, 1609545600],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [130.0, None],
                                    "high": [133.0, None],
                                    "low": [129.0, None],
                                    "close": [132.0, None],
                                    "volume": [100000, None],
                                }
                            ]
                        },
                    }
                ]
            }
        }
        candles = yahoo.parse_response(data)
        assert len(candles) == 1
        assert candles[0].timestamp == 1609459200 * 1000

    def test_parse_response_null_volume_skips_candle(self, yahoo):
        data = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1609459200],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [130.0],
                                    "high": [133.0],
                                    "low": [129.0],
                                    "close": [132.0],
                                    "volume": [None],
                                }
                            ]
                        },
                    }
                ]
            }
        }
        candles = yahoo.parse_response(data)
        assert len(candles) == 0

    def test_parse_response_empty(self, yahoo):
        assert yahoo.parse_response({}) == []
        assert yahoo.parse_response(None) == []
        assert yahoo.parse_response("error") == []
        assert yahoo.parse_response({"chart": {"result": []}}) == []

    def test_parse_response_no_timestamps(self, yahoo):
        data = {
            "chart": {
                "result": [
                    {
                        "timestamp": [],
                        "indicators": {"quote": [{}]},
                    }
                ]
            }
        }
        assert yahoo.parse_response(data) == []

    def test_is_rate_limited(self, yahoo):
        assert yahoo.is_rate_limited(429) is True
        assert yahoo.is_rate_limited(200) is False
        assert yahoo.is_rate_limited(418) is False


# ===========================================================================
# EODHDAdapter
# ===========================================================================


class TestEODHDAdapter:
    def test_name_property(self, eodhd):
        assert eodhd.name == "EODHD"

    def test_adapter_name_class_attribute(self):
        assert EODHDAdapter.ADAPTER_NAME == "EODHD"

    def test_get_supported_timeframes(self, eodhd):
        tf = eodhd.get_supported_timeframes()
        assert tf == {"1m": "1m", "1h": "1h", "1D": "d"}

    def test_format_symbol_uppercase(self, eodhd):
        assert eodhd.format_symbol("mcd.us") == "MCD.US"
        assert eodhd.format_symbol("AAPL.US") == "AAPL.US"

    def test_build_url_eod(self, eodhd):
        url = eodhd.build_url("MCD.US", "d")
        assert url == "https://eodhd.com/api/eod/MCD.US"

    def test_build_url_intraday(self, eodhd):
        url = eodhd.build_url("MCD.US", "1m")
        assert url == "https://eodhd.com/api/intraday/MCD.US"

        url_1h = eodhd.build_url("MCD.US", "1h")
        assert url_1h == "https://eodhd.com/api/intraday/MCD.US"

    def test_build_params_includes_token_and_fmt(self, eodhd, eodhd_token):
        params = eodhd.build_params(start=1000)
        assert params["api_token"] == "test-token"
        assert params["fmt"] == "json"

    def test_build_params_raises_without_token(self, eodhd):
        # Make sure there's no token
        os.environ.pop("EODHD_API_TOKEN", None)
        with pytest.raises(ValueError, match="EODHD_API_TOKEN"):
            eodhd.build_params(start=1000)

    def test_build_fetch_params_eod(self, eodhd, eodhd_token):
        start_ms = int(datetime(2024, 1, 15, tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int(datetime(2024, 6, 15, tzinfo=timezone.utc).timestamp() * 1000)
        params = eodhd.build_fetch_params("MCD.US", "d", start=start_ms, end=end_ms)
        assert params["api_token"] == "test-token"
        assert params["fmt"] == "json"
        assert params["from"] == "2024-01-15"
        assert params["to"] == "2024-06-15"
        assert params["period"] == "d"

    def test_build_fetch_params_intraday(self, eodhd, eodhd_token):
        start_ms = 1705312800000  # some timestamp in ms
        end_ms = 1705399200000
        params = eodhd.build_fetch_params("MCD.US", "1m", start=start_ms, end=end_ms)
        assert params["api_token"] == "test-token"
        assert params["fmt"] == "json"
        assert params["from"] == start_ms // 1000
        assert params["to"] == end_ms // 1000
        assert params["interval"] == "1m"

    def test_validate_config_raises_without_token(self, eodhd):
        os.environ.pop("EODHD_API_TOKEN", None)
        with pytest.raises(ValueError, match="EODHD_API_TOKEN"):
            eodhd.validate_config()

    def test_validate_config_succeeds_with_token(self, eodhd, eodhd_token):
        eodhd.validate_config()  # should not raise

    def test_parse_response_intraday(self, eodhd):
        data = [
            {"timestamp": 1705312800, "open": 150.5, "high": 150.8, "low": 150.2, "close": 150.6, "volume": 12345},
            {"timestamp": 1705316400, "open": 150.6, "high": 151.0, "low": 150.4, "close": 150.9, "volume": 11000},
        ]
        candles = eodhd.parse_response(data)
        assert len(candles) == 2

        c = candles[0]
        assert c.timestamp == 1705312800 * 1000  # converted to ms
        assert c.open == 150.5
        assert c.high == 150.8
        assert c.low == 150.2
        assert c.close == 150.6
        assert c.volume == 12345

    def test_parse_response_eod(self, eodhd):
        data = [
            {"date": "2024-01-15", "open": 290.0, "high": 295.0, "low": 288.0, "close": 293.0, "volume": 5000000},
            {"date": "2024-01-16", "open": 293.0, "high": 297.0, "low": 291.0, "close": 296.0, "volume": 4800000},
        ]
        candles = eodhd.parse_response(data)
        assert len(candles) == 2

        c = candles[0]
        expected_ts = int(datetime(2024, 1, 15, tzinfo=timezone.utc).timestamp() * 1000)
        assert c.timestamp == expected_ts
        assert c.open == 290.0
        assert c.high == 295.0
        assert c.low == 288.0
        assert c.close == 293.0
        assert c.volume == 5000000

    def test_parse_response_sorted_ascending(self, eodhd):
        data = [
            {"date": "2024-01-16", "open": 293.0, "high": 297.0, "low": 291.0, "close": 296.0, "volume": 4800000},
            {"date": "2024-01-15", "open": 290.0, "high": 295.0, "low": 288.0, "close": 293.0, "volume": 5000000},
        ]
        candles = eodhd.parse_response(data)
        assert candles[0].timestamp < candles[1].timestamp

    def test_parse_response_empty(self, eodhd):
        assert eodhd.parse_response([]) == []
        assert eodhd.parse_response(None) == []
        assert eodhd.parse_response("error") == []

    def test_parse_response_skips_rows_without_timestamp_or_date(self, eodhd):
        data = [
            {"open": 150.0, "high": 151.0, "low": 149.0, "close": 150.5, "volume": 100},
            {"timestamp": 1705312800, "open": 150.5, "high": 150.8, "low": 150.2, "close": 150.6, "volume": 12345},
        ]
        candles = eodhd.parse_response(data)
        assert len(candles) == 1

    def test_is_rate_limited(self, eodhd):
        assert eodhd.is_rate_limited(429) is True
        assert eodhd.is_rate_limited(200) is False
        assert eodhd.is_rate_limited(418) is False


# ===========================================================================
# Adapter Registry
# ===========================================================================


class TestAdapterRegistry:
    def test_list_adapters_returns_all_four(self):
        adapters = list_adapters()
        assert "BINANCE" in adapters
        assert "BITFINEX" in adapters
        assert "YAHOO" in adapters
        assert "EODHD" in adapters
        assert len(adapters) == 4

    def test_get_adapter_bitfinex(self):
        adapter = get_adapter("BITFINEX")
        assert isinstance(adapter, BitfinexAdapter)

    def test_get_adapter_case_insensitive(self):
        adapter = get_adapter("bitfinex")
        assert isinstance(adapter, BitfinexAdapter)

    def test_get_adapter_unknown_raises_value_error(self):
        with pytest.raises(ValueError, match="No adapter registered"):
            get_adapter("NONEXISTENT")
