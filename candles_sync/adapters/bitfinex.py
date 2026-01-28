"""
Bitfinex exchange adapter.

API Documentation: https://docs.bitfinex.com/reference/rest-public-candles
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .base import AdapterConfig, Candle, ExchangeAdapter, RateLimitConfig
from . import register_adapter


BITFINEX_API_URL = "https://api-pub.bitfinex.com/v2/candles/trade:{}:{}/hist"
API_LIMIT = 10_000
BACKOFF_INITIAL_SECONDS = 30
BACKOFF_MAX_SECONDS = 300


@register_adapter
class BitfinexAdapter(ExchangeAdapter):
    """
    Adapter for Bitfinex public candles API.

    Bitfinex candle format: [mts, open, close, high, low, volume]
    Note: Bitfinex uses close before high/low, which differs from standard OHLCV.
    """

    @property
    def name(self) -> str:
        return "BITFINEX"

    @property
    def config(self) -> AdapterConfig:
        return AdapterConfig(
            api_url=BITFINEX_API_URL,
            limit=API_LIMIT,
            timeout_seconds=30,
            rate_limit=RateLimitConfig(
                initial_backoff_seconds=BACKOFF_INITIAL_SECONDS,
                max_backoff_seconds=BACKOFF_MAX_SECONDS,
                rate_limit_status_codes=(429,),
            ),
        )

    def get_supported_timeframes(self) -> Dict[str, str]:
        """Bitfinex uses: 1m, 1h, 1D (uppercase D for daily)."""
        return {
            '1m': '1m',
            '1h': '1h',
            '1D': '1D',
        }

    def format_symbol(self, symbol: str) -> str:
        """
        Format symbol for Bitfinex API.

        Bitfinex trading pairs use 't' prefix (e.g., tBTCUSD).
        If symbol already has 't' prefix, return as-is.
        """
        if symbol.startswith('t'):
            return symbol
        return f"t{symbol}"

    def build_url(self, symbol: str, timeframe: str) -> str:
        """Build Bitfinex candles URL."""
        return BITFINEX_API_URL.format(timeframe, symbol)

    def build_params(
        self,
        start: int,
        end: Optional[int] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Build query parameters for Bitfinex API.

        Uses sort=1 for ascending order (oldest first).
        """
        params: Dict[str, Any] = {
            'start': int(start),
            'sort': 1,  # ascending order
        }
        if end is not None:
            params['end'] = int(end)
        if limit is not None:
            params['limit'] = int(limit)
        else:
            params['limit'] = API_LIMIT
        return params

    def parse_response(self, data: Any) -> List[Candle]:
        """
        Parse Bitfinex API response into Candle objects.

        Bitfinex format: [mts, open, close, high, low, volume]
        We normalize to: timestamp, open, high, low, close, volume
        """
        if not data or not isinstance(data, list):
            return []

        candles = []
        for row in data:
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue

            try:
                # Bitfinex order: mts, open, close, high, low, volume
                candle = Candle(
                    timestamp=int(row[0]),
                    open=float(row[1]),
                    high=float(row[3]),  # Note: high is at index 3
                    low=float(row[4]),   # Note: low is at index 4
                    close=float(row[2]), # Note: close is at index 2
                    volume=float(row[5]),
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
        Find the earliest candle for a symbol on Bitfinex.

        Bitfinex returns the earliest available candle when requesting
        from timestamp 0 with limit=1.
        """
        if fetch_fn is None:
            # Return current time as fallback if no fetch function provided
            return int(datetime.now(timezone.utc).timestamp() * 1000)

        # Fetch single candle from epoch - Bitfinex returns earliest available
        candles = fetch_fn(start=0, limit=1)
        if not candles:
            return int(datetime.now(timezone.utc).timestamp() * 1000)

        return candles[0].timestamp
