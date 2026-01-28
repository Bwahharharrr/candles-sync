"""
Yahoo Finance adapter.

API: https://query1.finance.yahoo.com/v8/finance/chart/{symbol}

Note: Yahoo Finance has data availability limits by timeframe:
- 1m: Last 7 days only
- 1h: Last 730 days (2 years)
- 1d: Full history available
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .base import AdapterConfig, Candle, ExchangeAdapter, RateLimitConfig
from . import register_adapter


YAHOO_API_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{}"
API_LIMIT = 10000  # Yahoo doesn't have a strict limit, but we cap for safety
BACKOFF_INITIAL_SECONDS = 5
BACKOFF_MAX_SECONDS = 60


@register_adapter
class YahooAdapter(ExchangeAdapter):
    """
    Adapter for Yahoo Finance chart data API.

    Yahoo Finance response structure:
    {
        "chart": {
            "result": [{
                "timestamp": [...],
                "indicators": {
                    "quote": [{
                        "open": [...],
                        "high": [...],
                        "low": [...],
                        "close": [...],
                        "volume": [...]
                    }]
                }
            }]
        }
    }

    Note: Yahoo uses Unix seconds, not milliseconds.
    """

    @property
    def name(self) -> str:
        return "YAHOO"

    @property
    def config(self) -> AdapterConfig:
        return AdapterConfig(
            api_url=YAHOO_API_URL,
            limit=API_LIMIT,
            timeout_seconds=30,
            rate_limit=RateLimitConfig(
                initial_backoff_seconds=BACKOFF_INITIAL_SECONDS,
                max_backoff_seconds=BACKOFF_MAX_SECONDS,
                rate_limit_status_codes=(429,),
            ),
        )

    def get_supported_timeframes(self) -> Dict[str, str]:
        """
        Yahoo Finance interval codes.

        Supported intervals: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
        We map our internal codes to Yahoo's format.
        """
        return {
            '1m': '1m',
            '1h': '1h',
            '1D': '1d',  # Yahoo uses lowercase 'd'
        }

    def format_symbol(self, symbol: str) -> str:
        """
        Format symbol for Yahoo Finance API.

        Yahoo uses standard ticker symbols:
        - Stocks: AAPL, MSFT, GOOGL
        - Crypto: BTC-USD, ETH-USD
        - Indices: ^GSPC (S&P 500), ^DJI (Dow Jones)
        - Forex: EURUSD=X

        Symbol is returned as-is since Yahoo uses standard formats.
        """
        return symbol

    def build_url(self, symbol: str, timeframe: str) -> str:
        """Build Yahoo Finance chart URL."""
        return YAHOO_API_URL.format(symbol)

    def build_params(
        self,
        start: int,
        end: Optional[int] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Build query parameters for Yahoo Finance API.

        Note: Yahoo uses Unix seconds, not milliseconds.
        The 'period1' and 'period2' params are Unix timestamps in seconds.
        """
        # Convert milliseconds to seconds
        params: Dict[str, Any] = {
            'period1': int(start // 1000),
        }
        if end is not None:
            params['period2'] = int(end // 1000)
        else:
            # Default to current time
            params['period2'] = int(datetime.now(timezone.utc).timestamp())

        # Yahoo doesn't support limit, returns all data in range
        # We'll truncate in parse_response if needed

        return params

    def parse_response(self, data: Any) -> List[Candle]:
        """
        Parse Yahoo Finance API response into Candle objects.

        Yahoo structure is nested JSON with parallel arrays.
        Timestamps are in Unix seconds, we convert to milliseconds.
        """
        if not data or not isinstance(data, dict):
            return []

        try:
            chart = data.get('chart', {})
            result = chart.get('result', [])
            if not result:
                return []

            result_data = result[0]
            timestamps = result_data.get('timestamp', [])
            if not timestamps:
                return []

            indicators = result_data.get('indicators', {})
            quote = indicators.get('quote', [])
            if not quote:
                return []

            quote_data = quote[0]
            opens = quote_data.get('open', [])
            highs = quote_data.get('high', [])
            lows = quote_data.get('low', [])
            closes = quote_data.get('close', [])
            volumes = quote_data.get('volume', [])

        except (KeyError, IndexError, TypeError):
            return []

        candles = []
        for i, ts in enumerate(timestamps):
            try:
                # Skip if any value is None (Yahoo returns null for missing data)
                o = opens[i] if i < len(opens) else None
                h = highs[i] if i < len(highs) else None
                l = lows[i] if i < len(lows) else None
                c = closes[i] if i < len(closes) else None
                v = volumes[i] if i < len(volumes) else None

                if None in (ts, o, h, l, c):
                    continue

                candle = Candle(
                    timestamp=int(ts) * 1000,  # Convert seconds to milliseconds
                    open=float(o),
                    high=float(h),
                    low=float(l),
                    close=float(c),
                    volume=float(v) if v is not None else 0.0,
                )
                candles.append(candle)
            except (ValueError, TypeError, IndexError):
                continue

        # Sort by timestamp ascending
        candles.sort(key=lambda c: c.timestamp)
        return candles

    def find_genesis_timestamp(
        self,
        symbol: str,
        timeframe: str,
        *,
        fetch_fn=None
    ) -> int:
        """
        Find the earliest candle for a symbol on Yahoo Finance.

        Yahoo doesn't have a direct way to get the earliest candle.
        We request from Unix epoch (1970) and take the first result.
        """
        if fetch_fn is None:
            return int(datetime.now(timezone.utc).timestamp() * 1000)

        # Request from epoch - Yahoo returns earliest available data
        candles = fetch_fn(start=0, limit=1)
        if not candles:
            return int(datetime.now(timezone.utc).timestamp() * 1000)

        return candles[0].timestamp
