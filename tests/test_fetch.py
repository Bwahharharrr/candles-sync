"""
Tests for fetch_candles and _redact_url in candles_sync.candles_sync.
"""

import json

import pytest
import requests
from unittest.mock import patch, MagicMock

from candles_sync.candles_sync import fetch_candles, _redact_url
from candles_sync.adapters.bitfinex import BitfinexAdapter
from candles_sync.adapters.base import Candle


# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #

def _make_adapter():
    """Create a BitfinexAdapter for testing."""
    return BitfinexAdapter()


def _bitfinex_row(ts, o=100.0, c=101.0, h=102.0, low=99.0, v=50.0):
    """Build a raw Bitfinex candle row: [mts, open, close, high, low, volume]."""
    return [ts, o, c, h, low, v]


def _make_ok_response(data):
    """Build a mock requests.Response with status 200 and JSON payload."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 200
    resp.json.return_value = data
    return resp


def _make_error_response(status_code, text="error"):
    """Build a mock requests.Response with a non-200 status code."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    return resp


# --------------------------------------------------------------------------- #
#  _redact_url tests
# --------------------------------------------------------------------------- #

class TestRedactUrl:

    def test_url_without_api_token_unchanged(self):
        url = "https://example.com/v1/data?symbol=AAPL&limit=100"
        assert _redact_url(url) == url

    def test_url_with_api_token_redacted(self):
        url = "https://example.com/v1/data?api_token=abc123"
        assert _redact_url(url) == "https://example.com/v1/data?api_token=***"

    def test_api_token_in_middle_of_query_string(self):
        url = "https://example.com/v1/data?symbol=AAPL&api_token=secret99&limit=100"
        result = _redact_url(url)
        assert "api_token=***" in result
        assert "secret99" not in result
        # Surrounding params should be preserved
        assert "symbol=AAPL" in result
        assert "limit=100" in result


# --------------------------------------------------------------------------- #
#  fetch_candles tests
# --------------------------------------------------------------------------- #

@patch("candles_sync.candles_sync.time.sleep")
@patch("candles_sync.candles_sync.requests.get")
class TestFetchCandles:

    def test_successful_fetch_returns_parsed_candles(self, mock_get, mock_sleep):
        """A 200 response with valid data returns a list of Candle objects."""
        adapter = _make_adapter()
        raw_data = [
            _bitfinex_row(1000000, 100, 101, 102, 99, 50),
            _bitfinex_row(1060000, 101, 102, 103, 100, 60),
        ]
        mock_get.return_value = _make_ok_response(raw_data)

        result = fetch_candles(adapter, "tBTCUSD", "1m", start=1000000)

        assert len(result) == 2
        assert all(isinstance(c, Candle) for c in result)
        assert result[0].timestamp == 1000000
        assert result[1].timestamp == 1060000
        # Verify Bitfinex field mapping (row[2] = close, row[3] = high)
        assert result[0].close == 101.0
        assert result[0].high == 102.0
        mock_sleep.assert_not_called()

    def test_http_error_returns_empty_list(self, mock_get, mock_sleep):
        """A non-retryable HTTP error (e.g. 400, 500) returns []."""
        adapter = _make_adapter()
        mock_get.return_value = _make_error_response(500, "Internal Server Error")

        result = fetch_candles(adapter, "tBTCUSD", "1m", start=0)

        assert result == []
        mock_sleep.assert_not_called()

    def test_rate_limit_retries_then_succeeds(self, mock_get, mock_sleep):
        """A 429 response triggers retry; success on subsequent attempt."""
        adapter = _make_adapter()
        rate_limited = _make_error_response(429, "Rate limit exceeded")
        ok = _make_ok_response([_bitfinex_row(1000000)])

        mock_get.side_effect = [rate_limited, rate_limited, ok]

        result = fetch_candles(adapter, "tBTCUSD", "1m", start=1000000)

        assert len(result) == 1
        assert result[0].timestamp == 1000000
        assert mock_sleep.call_count == 2
        # Verify exponential backoff: first sleep = 30, second = 60
        assert mock_sleep.call_args_list[0][0][0] == 30
        assert mock_sleep.call_args_list[1][0][0] == 60

    def test_network_error_retries_then_succeeds(self, mock_get, mock_sleep):
        """A network exception triggers retry; success on subsequent attempt."""
        adapter = _make_adapter()
        ok = _make_ok_response([_bitfinex_row(2000000)])

        mock_get.side_effect = [
            requests.ConnectionError("Connection refused"),
            ok,
        ]

        result = fetch_candles(adapter, "tBTCUSD", "1m", start=2000000)

        assert len(result) == 1
        assert result[0].timestamp == 2000000
        assert mock_sleep.call_count == 1
        assert mock_sleep.call_args_list[0][0][0] == 30

    def test_exhausted_retries_returns_empty_list(self, mock_get, mock_sleep):
        """When MAX_FETCH_RETRIES is exceeded, fetch_candles gives up and returns []."""
        adapter = _make_adapter()
        mock_get.side_effect = requests.ConnectionError("timeout")

        result = fetch_candles(adapter, "tBTCUSD", "1m", start=0)

        assert result == []
        # Should have slept MAX_FETCH_RETRIES times (retries 1..10, then gives up at 11)
        assert mock_sleep.call_count == 10

    def test_json_decode_error_returns_empty_list(self, mock_get, mock_sleep):
        """If the 200 response contains invalid JSON, return []."""
        adapter = _make_adapter()
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.side_effect = ValueError("No JSON object could be decoded")
        mock_get.return_value = resp

        result = fetch_candles(adapter, "tBTCUSD", "1m", start=0)

        assert result == []
        mock_sleep.assert_not_called()
