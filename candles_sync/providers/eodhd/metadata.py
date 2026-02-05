"""
EODHD metadata caching for exchanges and symbols.

Caches exchange and symbol data locally to avoid repeated API calls.
Cache location: ~/.corky/providers/eodhd/meta/

Files:
- exchanges.json: List of all available exchanges
- symbols/{EXCHANGE_CODE}.json: Symbols for each exchange
- index.json: Reverse index mapping bare symbol -> list of exchanges
"""

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from candles_sync.adapters.eodhd import get_api_token, EODHD_API_URL


# Default cache TTL: 24 hours
DEFAULT_TTL_SECONDS = 24 * 60 * 60

# Cache directory
CACHE_DIR = Path.home() / ".corky" / "providers" / "eodhd" / "meta"


class MetadataCache:
    """
    Cache for EODHD exchange and symbol metadata.

    Provides efficient access to exchange/symbol data with:
    - 24-hour TTL with configurable force-refresh
    - Atomic writes (write to .tmp then rename)
    - Reverse index for fast symbol lookup
    """

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ):
        """
        Initialize the metadata cache.

        Args:
            cache_dir: Custom cache directory (default: ~/.corky/providers/eodhd/meta/)
            ttl_seconds: Cache TTL in seconds (default: 24 hours)
        """
        self.cache_dir = cache_dir or CACHE_DIR
        self.ttl_seconds = ttl_seconds
        self._session = requests.Session()
        self._ensure_cache_dirs()

    def _ensure_cache_dirs(self) -> None:
        """Create cache directories if they don't exist."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        (self.cache_dir / "symbols").mkdir(exist_ok=True)

    def _is_stale(self, filepath: Path) -> bool:
        """Check if a cache file is stale (older than TTL)."""
        if not filepath.exists():
            return True
        age = time.time() - filepath.stat().st_mtime
        return age > self.ttl_seconds

    def _atomic_write(self, filepath: Path, data: Any) -> None:
        """Write data to file atomically using tmp file + rename."""
        tmp_path = filepath.with_suffix(".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            tmp_path.rename(filepath)
        except BaseException:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    def _read_cache(self, filepath: Path) -> Optional[Any]:
        """Read and parse a cache file."""
        if not filepath.exists():
            return None
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None

    def _fetch_from_api(self, endpoint: str) -> Any:
        """Fetch data from EODHD API."""
        url = f"{EODHD_API_URL}/{endpoint}"
        params = {
            "api_token": get_api_token(),
            "fmt": "json",
        }
        resp = self._session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_exchanges(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        Get list of all available exchanges.

        Args:
            force_refresh: Force refresh from API regardless of TTL

        Returns:
            List of exchange dicts with keys: Code, Name, Country, etc.
        """
        filepath = self.cache_dir / "exchanges.json"

        if not force_refresh and not self._is_stale(filepath):
            cached = self._read_cache(filepath)
            if cached is not None:
                return cached

        # Fetch from API
        data = self._fetch_from_api("exchanges-list/")
        self._atomic_write(filepath, data)
        return data

    def get_exchange_symbols(
        self,
        exchange_code: str,
        force_refresh: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Get list of symbols for an exchange.

        Args:
            exchange_code: Exchange code (e.g., 'US', 'LSE')
            force_refresh: Force refresh from API regardless of TTL

        Returns:
            List of symbol dicts with keys: Code, Name, Exchange, etc.
        """
        exchange_code = exchange_code.upper()
        filepath = self.cache_dir / "symbols" / f"{exchange_code}.json"

        if not force_refresh and not self._is_stale(filepath):
            cached = self._read_cache(filepath)
            if cached is not None:
                return cached

        # Fetch from API
        data = self._fetch_from_api(f"exchange-symbol-list/{exchange_code}")
        self._atomic_write(filepath, data)
        return data

    def get_index(self, force_refresh: bool = False) -> Dict[str, List[str]]:
        """
        Get or build the reverse index mapping bare symbol -> exchanges.

        Args:
            force_refresh: Force rebuild of index

        Returns:
            Dict mapping symbol code to list of exchange codes
        """
        filepath = self.cache_dir / "index.json"

        if not force_refresh and not self._is_stale(filepath):
            cached = self._read_cache(filepath)
            if cached is not None:
                return cached

        # Build index from all exchange symbol files
        index: Dict[str, List[str]] = {}
        symbols_dir = self.cache_dir / "symbols"

        if symbols_dir.exists():
            for symbol_file in symbols_dir.glob("*.json"):
                exchange_code = symbol_file.stem
                symbols = self._read_cache(symbol_file)
                if symbols:
                    for sym in symbols:
                        code = sym.get("Code", "").upper()
                        if code:
                            if code not in index:
                                index[code] = []
                            if exchange_code not in index[code]:
                                index[code].append(exchange_code)

        self._atomic_write(filepath, index)
        return index

    def rebuild_index(self, exchanges: Optional[List[str]] = None) -> Dict[str, List[str]]:
        """
        Rebuild the index from scratch, optionally for specific exchanges.

        Builds the new index to a temp file first, then atomically replaces
        the old one to prevent data loss if the process crashes mid-rebuild.

        Args:
            exchanges: List of exchange codes to include (default: all cached)

        Returns:
            Updated index dict
        """
        if exchanges:
            # Refresh specified exchanges
            for code in exchanges:
                self.get_exchange_symbols(code, force_refresh=True)

        # Build the new index in-memory, then write atomically
        return self.get_index(force_refresh=True)

    def refresh_all(self, exchanges: Optional[List[str]] = None) -> None:
        """
        Refresh all metadata from API.

        Args:
            exchanges: Specific exchange codes to refresh (default: all)
        """
        # Refresh exchanges list
        all_exchanges = self.get_exchanges(force_refresh=True)

        # Determine which exchanges to refresh
        if exchanges:
            codes = [e.upper() for e in exchanges]
        else:
            codes = [e.get("Code", "") for e in all_exchanges if e.get("Code")]

        # Refresh each exchange's symbols
        for code in codes:
            try:
                self.get_exchange_symbols(code, force_refresh=True)
            except Exception as e:
                print(f"Warning: Failed to refresh {code}: {e}")

        # Rebuild the index
        self.rebuild_index()

    def lookup_symbol(self, symbol: str) -> Optional[List[str]]:
        """
        Look up which exchanges have a given symbol.

        Args:
            symbol: Bare symbol code (e.g., 'AAPL', 'MCD')

        Returns:
            List of exchange codes where this symbol exists, or None if not found
        """
        index = self.get_index()
        symbol = symbol.upper()
        return index.get(symbol)

    def symbol_exists(self, symbol: str, exchange: str) -> bool:
        """
        Check if a symbol exists on a specific exchange.

        Args:
            symbol: Bare symbol code
            exchange: Exchange code

        Returns:
            True if the symbol exists on the exchange
        """
        exchanges = self.lookup_symbol(symbol)
        if not exchanges:
            return False
        return exchange.upper() in exchanges

    def get_full_symbol(self, symbol: str, exchange: str) -> str:
        """
        Get the full symbol in SYMBOL.EXCHANGE format.

        Args:
            symbol: Bare symbol code
            exchange: Exchange code

        Returns:
            Full symbol like 'AAPL.US'
        """
        return f"{symbol.upper()}.{exchange.upper()}"
