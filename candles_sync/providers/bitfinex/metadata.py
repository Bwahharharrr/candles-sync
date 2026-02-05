"""
Bitfinex metadata caching for trading pairs.

Caches trading pair data locally to avoid repeated API calls.
Cache location: ~/.corky/providers/bitfinex/meta/

Files:
- tickers.json: List of all available trading pairs
"""

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


# Bitfinex public API endpoint for trading pairs
BITFINEX_PAIRS_URL = "https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange"

# Default cache TTL: 24 hours
DEFAULT_TTL_SECONDS = 24 * 60 * 60

# Cache directory
CACHE_DIR = Path.home() / ".corky" / "providers" / "bitfinex" / "meta"


class BitfinexMetadataCache:
    """
    Cache for Bitfinex trading pair metadata.

    Provides efficient access to trading pairs with:
    - 24-hour TTL with configurable force-refresh
    - Atomic writes (write to .tmp then rename)
    """

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ):
        """
        Initialize the metadata cache.

        Args:
            cache_dir: Custom cache directory (default: ~/.corky/providers/bitfinex/meta/)
            ttl_seconds: Cache TTL in seconds (default: 24 hours)
        """
        self.cache_dir = cache_dir or CACHE_DIR
        self.ttl_seconds = ttl_seconds
        self._session = requests.Session()
        self._ensure_cache_dirs()

    def _ensure_cache_dirs(self) -> None:
        """Create cache directories if they don't exist."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

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

    def _read_cache(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """Read and parse a cache file."""
        if not filepath.exists():
            return None
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None

    def _fetch_from_api(self) -> List[str]:
        """Fetch trading pairs from Bitfinex API."""
        resp = self._session.get(BITFINEX_PAIRS_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # API returns double-nested array: [["BTCUSD", "ETHUSD", ...]]
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        return []

    def get_tickers(self, force_refresh: bool = False) -> List[Dict[str, str]]:
        """
        Get list of all available trading pairs.

        Args:
            force_refresh: Force refresh from API regardless of TTL

        Returns:
            List of ticker dicts with keys: symbol (e.g., 'tBTCUSD'), raw (e.g., 'BTCUSD')
        """
        filepath = self.cache_dir / "tickers.json"

        if not force_refresh and not self._is_stale(filepath):
            cached = self._read_cache(filepath)
            if cached is not None and "tickers" in cached:
                return cached["tickers"]

        # Fetch from API
        raw_pairs = self._fetch_from_api()

        # Transform to structured format
        # Raw pairs are like "BTCUSD", "AAVE:USD" - prepend 't' for trading symbol
        tickers = []
        for raw in raw_pairs:
            symbol = f"t{raw.replace(':', '')}"
            tickers.append({"symbol": symbol, "raw": raw})

        # Store with metadata
        cache_data = {
            "fetched_at": int(time.time()),
            "tickers": tickers,
        }
        self._atomic_write(filepath, cache_data)
        return tickers

    def refresh(self) -> List[Dict[str, str]]:
        """Force refresh the cache and return tickers."""
        return self.get_tickers(force_refresh=True)
