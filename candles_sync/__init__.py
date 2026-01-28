from .candles_sync import synchronize_candle_data
from .adapters import get_adapter, list_adapters, ExchangeAdapter

__version__ = "0.1.0"
__all__ = [
    "synchronize_candle_data",
    "get_adapter",
    "list_adapters",
    "ExchangeAdapter",
]

if __name__ == "__main__":
    main()