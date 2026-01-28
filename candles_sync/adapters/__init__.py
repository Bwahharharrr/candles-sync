"""
Exchange adapter registry and factory.

Usage:
    from candles_sync.adapters import get_adapter, list_adapters

    adapter = get_adapter('bitfinex')
    print(list_adapters())  # ['BITFINEX', 'BINANCE', 'YAHOO']
"""

from typing import Dict, List, Type

from .base import ExchangeAdapter, Candle, RateLimitConfig, AdapterConfig

# Registry of all available adapters
_ADAPTERS: Dict[str, Type[ExchangeAdapter]] = {}


def register_adapter(cls: Type[ExchangeAdapter]) -> Type[ExchangeAdapter]:
    """
    Decorator to register an adapter class in the global registry.

    Usage:
        @register_adapter
        class MyAdapter(ExchangeAdapter):
            ...
    """
    # Instantiate temporarily to get the name
    instance = cls()
    name = instance.name.upper()
    _ADAPTERS[name] = cls
    return cls


def get_adapter(exchange: str) -> ExchangeAdapter:
    """
    Factory function to get an adapter instance by exchange name.

    Args:
        exchange: Exchange name (case-insensitive)

    Returns:
        ExchangeAdapter instance

    Raises:
        ValueError: If no adapter is registered for the exchange
    """
    name = exchange.upper()
    if name not in _ADAPTERS:
        available = list(_ADAPTERS.keys())
        raise ValueError(
            f"No adapter registered for exchange '{exchange}'. "
            f"Available adapters: {available}"
        )
    return _ADAPTERS[name]()


def list_adapters() -> List[str]:
    """
    Return list of all registered adapter names.

    Returns:
        List of uppercase exchange names
    """
    return sorted(_ADAPTERS.keys())


# Import adapters to trigger registration
from . import bitfinex
from . import binance
from . import yahoo

__all__ = [
    'ExchangeAdapter',
    'Candle',
    'RateLimitConfig',
    'AdapterConfig',
    'register_adapter',
    'get_adapter',
    'list_adapters',
]
