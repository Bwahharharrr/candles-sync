"""
Base adapter classes for exchange integrations.

Provides abstract base class and dataclasses for normalizing
candle data across different exchanges and data sources.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class Candle:
    """Normalized candle representation across all adapters."""
    timestamp: int  # milliseconds since epoch
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class RateLimitConfig:
    """Rate limiting configuration for an adapter."""
    initial_backoff_seconds: float = 30.0
    max_backoff_seconds: float = 300.0
    rate_limit_status_codes: tuple = (429,)


@dataclass(frozen=True)
class AdapterConfig:
    """Configuration for an exchange adapter."""
    api_url: str
    limit: int = 1000
    timeout_seconds: int = 30
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)


class ExchangeAdapter(ABC):
    """
    Abstract base class for exchange adapters.

    Each adapter encapsulates exchange-specific logic for:
    - API URL construction
    - Request parameter formatting
    - Response parsing and normalization
    - Symbol formatting
    - Timeframe translation
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the canonical name of this exchange (uppercase)."""
        pass

    @property
    @abstractmethod
    def config(self) -> AdapterConfig:
        """Return the adapter configuration."""
        pass

    @abstractmethod
    def get_supported_timeframes(self) -> Dict[str, str]:
        """
        Return mapping of internal timeframe keys to exchange-specific formats.

        Internal keys: '1m', '1h', '1D'
        Values: exchange-specific format (e.g., '1m', '1h', '1d', '1day', etc.)
        """
        pass

    def translate_timeframe(self, timeframe: str) -> str:
        """
        Translate internal timeframe to exchange-specific format.

        Raises ValueError if timeframe is not supported.
        """
        supported = self.get_supported_timeframes()
        if timeframe not in supported:
            raise ValueError(
                f"Timeframe '{timeframe}' not supported by {self.name}. "
                f"Supported: {list(supported.keys())}"
            )
        return supported[timeframe]

    def validate_timeframe(self, timeframe: str) -> bool:
        """Check if a timeframe is supported by this adapter."""
        return timeframe in self.get_supported_timeframes()

    @abstractmethod
    def format_symbol(self, symbol: str) -> str:
        """
        Format a symbol for this exchange's API.

        Args:
            symbol: The raw symbol (e.g., 'BTCUSD', 'tBTCUSD', 'BTC-USD')

        Returns:
            Exchange-formatted symbol
        """
        pass

    @abstractmethod
    def build_url(self, symbol: str, timeframe: str) -> str:
        """
        Build the API endpoint URL for fetching candles.

        Args:
            symbol: Exchange-formatted symbol
            timeframe: Exchange-formatted timeframe

        Returns:
            Full API endpoint URL
        """
        pass

    @abstractmethod
    def build_params(
        self,
        start: int,
        end: Optional[int] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Build query parameters for the API request.

        Args:
            start: Start timestamp in milliseconds
            end: End timestamp in milliseconds (optional)
            limit: Maximum number of candles to fetch (optional)

        Returns:
            Dictionary of query parameters
        """
        pass

    @abstractmethod
    def parse_response(self, data: Any) -> List[Candle]:
        """
        Parse API response data into normalized Candle objects.

        Args:
            data: Raw API response (typically JSON)

        Returns:
            List of Candle objects in ascending order by timestamp
        """
        pass

    @abstractmethod
    def find_genesis_timestamp(
        self,
        symbol: str,
        timeframe: str,
        *,
        fetch_fn=None
    ) -> int:
        """
        Find the earliest available candle timestamp for a symbol.

        Args:
            symbol: Exchange-formatted symbol
            timeframe: Exchange-formatted timeframe
            fetch_fn: Optional function to perform the HTTP fetch

        Returns:
            Timestamp in milliseconds of the earliest candle
        """
        pass

    def is_rate_limited(self, status_code: int) -> bool:
        """Check if a response status code indicates rate limiting."""
        return status_code in self.config.rate_limit.rate_limit_status_codes

    def get_interval_ms(self, timeframe: str) -> int:
        """
        Get the interval in milliseconds for a timeframe.

        Args:
            timeframe: Internal timeframe key ('1m', '1h', '1D')

        Returns:
            Interval in milliseconds
        """
        intervals = {
            '1m': 60_000,
            '1h': 3_600_000,
            '1D': 86_400_000,
        }
        if timeframe not in intervals:
            raise ValueError(f"Unknown timeframe: {timeframe}")
        return intervals[timeframe]
