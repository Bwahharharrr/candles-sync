"""
EODHD.com adapter for fetching EOD and intraday candle data.

API Documentation: https://eodhd.com/financial-apis/api-for-historical-data-and-volumes/

Supports:
- EOD (daily) candles: unlimited history
- Intraday candles: 1m (120 days), 1h (7200 days)
"""

import functools
import os
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .base import AdapterConfig, Candle, ExchangeAdapter, RateLimitConfig
from . import register_adapter


# EODHD API base URL
EODHD_API_URL = "https://eodhd.com/api"

# Default API limit (candles per request)
API_LIMIT = 10000

# Intraday history limits in days
INTRADAY_LIMITS = {
    "1m": 120,
    "5m": 600,
    "1h": 7200,
}

# Earliest date for EOD data queries. EODHD has limited data before this date
# for most exchanges.
EODHD_EARLIEST_EOD_DATE = datetime(2000, 1, 1, tzinfo=timezone.utc)


def get_api_token() -> str:
    """Get EODHD API token from environment variable."""
    token = os.environ.get("EODHD_API_TOKEN", "")
    if not token:
        raise ValueError(
            "EODHD_API_TOKEN environment variable not set. "
            "Get your API token from https://eodhd.com/"
        )
    return token


@register_adapter
class EODHDAdapter(ExchangeAdapter):
    """
    Adapter for EODHD.com historical and intraday data API.

    Symbol format: SYMBOL.EXCHANGE (e.g., MCD.US, AAPL.US, TSLA.US)
    """

    ADAPTER_NAME = "EODHD"

    @property
    def name(self) -> str:
        return self.ADAPTER_NAME

    def validate_config(self) -> None:
        """Validate that EODHD configuration is complete. Call before starting sync."""
        get_api_token()  # raises ValueError if not set

    @functools.cached_property
    def config(self) -> AdapterConfig:
        return AdapterConfig(
            api_url=EODHD_API_URL,
            limit=API_LIMIT,
            timeout_seconds=30,
            rate_limit=RateLimitConfig(
                initial_backoff_seconds=30.0,
                max_backoff_seconds=300.0,
                rate_limit_status_codes=(429,),
            ),
        )

    def get_supported_timeframes(self) -> Dict[str, str]:
        """
        Map internal timeframes to EODHD API formats.

        For EOD: uses 'period=d'
        For intraday: uses 'interval=1m' or 'interval=1h'
        """
        return {
            "1m": "1m",
            "1h": "1h",
            "1D": "d",  # EODHD uses 'd' for daily
        }

    def format_symbol(self, symbol: str) -> str:
        """
        Ensure symbol is in SYMBOL.EXCHANGE format.

        EODHD requires symbols like 'MCD.US', 'AAPL.US', etc.
        If no exchange suffix provided, user should resolve it separately
        using resolve_symbol() or the EODHDResolver class.

        Raises:
            ValueError: If symbol is missing the required .EXCHANGE suffix.
        """
        formatted = symbol.upper()
        if "." not in formatted:
            raise ValueError(
                f"EODHD symbol must be in SYMBOL.EXCHANGE format (e.g., 'AAPL.US'), "
                f"got '{symbol}'. Use --get-tickers or resolve_symbol() to find the "
                f"correct format."
            )
        return formatted

    def resolve_symbol(
        self,
        symbol: str,
        interactive: bool = None,
        validate: bool = False,
    ) -> str:
        """
        Resolve a bare symbol to full SYMBOL.EXCHANGE format using the resolver.

        This is a convenience method that uses the EODHDResolver for pull-up
        match logic. For more control, use EODHDResolver directly.

        Args:
            symbol: Bare symbol (e.g., 'MCD') or full symbol (e.g., 'MCD.US')
            interactive: Allow interactive selection for ambiguous symbols
            validate: Validate that the symbol exists in the metadata cache

        Returns:
            Full symbol in SYMBOL.EXCHANGE format

        Raises:
            SymbolNotFoundError: If symbol not found
            AmbiguousSymbolError: If ambiguous in non-interactive mode
        """
        # Lazy import to avoid circular imports
        from candles_sync.providers.eodhd.resolver import EODHDResolver
        resolver = EODHDResolver()
        return resolver.resolve(symbol, interactive=interactive, validate=validate)

    def _is_intraday(self, timeframe: str) -> bool:
        """Check if timeframe is intraday (not daily)."""
        return timeframe in ("1m", "1h")

    def build_url(self, symbol: str, timeframe: str) -> str:
        """
        Build the appropriate API endpoint URL.

        EOD endpoint: /api/eod/{SYMBOL}
        Intraday endpoint: /api/intraday/{SYMBOL}
        """
        formatted_symbol = self.format_symbol(symbol)

        if self._is_intraday(timeframe):
            return f"{EODHD_API_URL}/intraday/{formatted_symbol}"
        else:
            return f"{EODHD_API_URL}/eod/{formatted_symbol}"

    def build_params(
        self,
        start: int,
        end: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Build base query parameters for the API request.

        Note: Time-related params (from/to/period/interval) are added
        separately in fetch_candles() since they depend on the timeframe.
        """
        return {
            "api_token": get_api_token(),
            "fmt": "json",
        }

    def build_fetch_params(
        self,
        symbol: str,
        timeframe: str,
        start: int,
        end: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        params = self.build_params(start, end, limit)
        time_params = self.build_time_params(start, end, timeframe)
        params.update(time_params)
        return params

    def build_time_params(
        self,
        start: int,
        end: Optional[int],
        timeframe: str,
    ) -> Dict[str, Any]:
        """
        Build time-related parameters based on the timeframe.

        EOD params: from=YYYY-MM-DD, to=YYYY-MM-DD, period=d
        Intraday params: from={unix_ts}, to={unix_ts}, interval=1m|1h

        Args:
            start: Start timestamp in milliseconds
            end: End timestamp in milliseconds (defaults to current time if None)
            timeframe: Exchange-formatted timeframe ('1m', '1h', or 'd')
        """
        # Default end to current time if not provided
        if end is None:
            end = int(datetime.now(timezone.utc).timestamp() * 1000)

        params: Dict[str, Any] = {}

        if timeframe in ("1m", "1h"):
            # Intraday uses Unix timestamps (seconds)
            params["from"] = start // 1000  # Convert ms to seconds
            params["to"] = end // 1000
            params["interval"] = timeframe
        else:
            # EOD uses YYYY-MM-DD dates
            start_dt = datetime.fromtimestamp(start / 1000, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(end / 1000, tz=timezone.utc)
            params["from"] = start_dt.strftime("%Y-%m-%d")
            params["to"] = end_dt.strftime("%Y-%m-%d")
            params["period"] = "d"

        return params

    def parse_response(self, data: Any) -> List[Candle]:
        """
        Parse EODHD API response into Candle objects.

        EOD format:
        [{"date": "2024-01-15", "open": 150.5, "high": 152.3,
          "low": 149.8, "close": 151.2, "volume": 1234567}, ...]

        Intraday format:
        [{"timestamp": 1705312800, "open": 150.5, "high": 150.8,
          "low": 150.2, "close": 150.6, "volume": 12345}, ...]
        """
        if not data or not isinstance(data, list):
            return []

        candles = []
        for row in data:
            try:
                # Determine timestamp format
                if "timestamp" in row:
                    # Intraday: Unix timestamp in seconds
                    timestamp_ms = int(row["timestamp"]) * 1000
                elif "date" in row:
                    # EOD: Date string YYYY-MM-DD
                    dt = datetime.strptime(row["date"], "%Y-%m-%d")
                    dt = dt.replace(tzinfo=timezone.utc)
                    timestamp_ms = int(dt.timestamp() * 1000)
                else:
                    continue

                candle = Candle(
                    timestamp=timestamp_ms,
                    open=float(row.get("open", 0)),
                    high=float(row.get("high", 0)),
                    low=float(row.get("low", 0)),
                    close=float(row.get("close", 0)),
                    volume=float(row.get("volume", 0)),
                )
                candles.append(candle)
            except (KeyError, ValueError, TypeError):
                # Skip malformed rows
                continue

        # Sort by timestamp ascending
        candles.sort(key=lambda c: c.timestamp)
        return candles

    def find_genesis_timestamp(
        self,
        symbol: str,
        timeframe: str,
        fetch_fn: Callable[[int, int], List[Candle]],
    ) -> int:
        """
        Find the earliest available timestamp for a symbol.

        For intraday, respects EODHD's history limits:
        - 1m: 120 days back
        - 1h: 7200 days back

        For EOD, searches back to year 2000.

        Returns:
            Timestamp in milliseconds of the earliest candle, or current time
            if no data is found.
        """
        import time

        now_ms = int(time.time() * 1000)

        if self._is_intraday(timeframe):
            max_days = INTRADAY_LIMITS.get(timeframe, 120)
            max_history_ms = max_days * 24 * 60 * 60 * 1000
            earliest_possible = now_ms - max_history_ms

            candles = fetch_fn(earliest_possible, now_ms)
            if candles:
                return candles[0].timestamp
            # No data yet — start from beginning of queryable window so sync
            # will pick up candles as they become available.
            return earliest_possible
        else:
            oldest_start = int(EODHD_EARLIEST_EOD_DATE.timestamp() * 1000)

            candles = fetch_fn(oldest_start, now_ms)
            if candles:
                return candles[0].timestamp

            return oldest_start
