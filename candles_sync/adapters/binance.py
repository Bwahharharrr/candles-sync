"""
Binance exchange adapter.

API Documentation: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .base import AdapterConfig, Candle, ExchangeAdapter, RateLimitConfig
from . import register_adapter


BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
API_LIMIT = 1000
BACKOFF_INITIAL_SECONDS = 10
BACKOFF_MAX_SECONDS = 120


@register_adapter
class BinanceAdapter(ExchangeAdapter):
    """
    Adapter for Binance public klines (candlestick) API.

    Binance kline format:
    [
        open_time, open, high, low, close, volume,
        close_time, quote_volume, trades, taker_buy_base, taker_buy_quote, ignore
    ]
    """

    @property
    def name(self) -> str:
        return "BINANCE"

    @property
    def config(self) -> AdapterConfig:
        return AdapterConfig(
            api_url=BINANCE_API_URL,
            limit=API_LIMIT,
            timeout_seconds=30,
            rate_limit=RateLimitConfig(
                initial_backoff_seconds=BACKOFF_INITIAL_SECONDS,
                max_backoff_seconds=BACKOFF_MAX_SECONDS,
                rate_limit_status_codes=(429, 418),  # 418 = IP ban
            ),
        )

    def get_supported_timeframes(self) -> Dict[str, str]:
        """
        Binance uses lowercase interval codes.

        Note: Binance uses '1d' (lowercase) for daily candles.
        """
        return {
            '1m': '1m',
            '1h': '1h',
            '1D': '1d',  # Binance uses lowercase 'd'
        }

    def format_symbol(self, symbol: str) -> str:
        """
        Format symbol for Binance API.

        Binance uses uppercase symbols without separators (e.g., BTCUSDT).
        Removes common separators and ensures uppercase.
        """
        # Remove common separators and convert to uppercase
        formatted = symbol.upper().replace('-', '').replace('_', '').replace('/', '')
        return formatted

    def build_url(self, symbol: str, timeframe: str) -> str:
        """Build Binance klines URL (base URL, params added separately)."""
        return BINANCE_API_URL

    def build_params(
        self,
        start: int,
        end: Optional[int] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Build query parameters for Binance API.

        Note: symbol and interval are added by the caller.
        startTime and endTime are in milliseconds.
        """
        params: Dict[str, Any] = {
            'startTime': int(start),
        }
        if end is not None:
            params['endTime'] = int(end)
        if limit is not None:
            params['limit'] = min(int(limit), API_LIMIT)
        else:
            params['limit'] = API_LIMIT
        return params

    def parse_response(self, data: Any) -> List[Candle]:
        """
        Parse Binance API response into Candle objects.

        Binance format:
        [open_time, open, high, low, close, volume, close_time, ...]

        We use open_time as the candle timestamp for consistency.
        """
        if not data or not isinstance(data, list):
            return []

        candles = []
        for row in data:
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue

            try:
                # Binance order: open_time, open, high, low, close, volume, ...
                candle = Candle(
                    timestamp=int(row[0]),  # open_time in ms
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
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
        Find the earliest candle for a symbol on Binance.

        Binance returns candles starting from startTime, so we request
        from timestamp 0 to get the earliest available.
        """
        if fetch_fn is None:
            return int(datetime.now(timezone.utc).timestamp() * 1000)

        # Fetch single candle from epoch
        candles = fetch_fn(start=0, limit=1)
        if not candles:
            return int(datetime.now(timezone.utc).timestamp() * 1000)

        return candles[0].timestamp
