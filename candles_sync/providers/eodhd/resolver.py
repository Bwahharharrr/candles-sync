"""
EODHD symbol resolver with pull-up match logic.

Handles symbol resolution for EODHD:
1. If symbol already has exchange suffix (e.g., MCD.US) -> accept as-is
2. If bare symbol matches exactly one exchange -> auto-resolve
3. If multiple matches -> interactive selection or error
"""

import sys
from typing import List, Optional, Tuple

from .metadata import MetadataCache


class SymbolNotFoundError(Exception):
    """Raised when a symbol cannot be found in any exchange."""

    def __init__(self, symbol: str, message: Optional[str] = None):
        self.symbol = symbol
        if message is None:
            message = f"Symbol '{symbol}' not found in any exchange"
        super().__init__(message)


class AmbiguousSymbolError(Exception):
    """Raised when a symbol exists on multiple exchanges and no exchange is specified."""

    def __init__(self, symbol: str, exchanges: List[str], message: Optional[str] = None):
        self.symbol = symbol
        self.exchanges = exchanges
        if message is None:
            exchange_list = ", ".join(exchanges)
            message = f"Symbol '{symbol}' is ambiguous. Found on exchanges: {exchange_list}"
        super().__init__(message)


class EODHDResolver:
    """
    Symbol resolver with pull-up match logic for EODHD.

    Handles the following cases:
    1. Full symbol (SYMBOL.EXCHANGE): Validates and returns as-is
    2. Bare symbol with single match: Auto-resolves to SYMBOL.EXCHANGE
    3. Bare symbol with multiple matches:
       - Interactive mode: Shows numbered list for selection
       - Non-interactive: Raises AmbiguousSymbolError
    """

    def __init__(self, cache: Optional[MetadataCache] = None):
        """
        Initialize the resolver.

        Args:
            cache: MetadataCache instance (creates one if not provided)
        """
        self.cache = cache or MetadataCache()

    def _is_tty(self) -> bool:
        """Check if we're running in an interactive terminal."""
        return sys.stdin.isatty() and sys.stdout.isatty()

    def _parse_symbol(self, symbol: str) -> Tuple[str, Optional[str]]:
        """
        Parse a symbol into (code, exchange) parts.

        Args:
            symbol: Input symbol (e.g., 'MCD' or 'MCD.US')

        Returns:
            Tuple of (symbol_code, exchange_code or None)
        """
        symbol = symbol.upper().strip()
        if "." in symbol:
            parts = symbol.rsplit(".", 1)
            return parts[0], parts[1]
        return symbol, None

    def _prompt_selection(self, symbol: str, exchanges: List[str]) -> str:
        """
        Show interactive prompt for exchange selection.

        Args:
            symbol: The bare symbol code
            exchanges: List of exchanges where symbol exists

        Returns:
            Selected exchange code
        """
        print(f"\nSymbol '{symbol}' found on multiple exchanges:\n")
        for i, exchange in enumerate(exchanges, 1):
            print(f"  {i}. {symbol}.{exchange}")
        print()

        while True:
            try:
                choice = input(f"Select exchange (1-{len(exchanges)}): ").strip()
                idx = int(choice) - 1
                if 0 <= idx < len(exchanges):
                    return exchanges[idx]
                print(f"Please enter a number between 1 and {len(exchanges)}")
            except ValueError:
                print("Please enter a valid number")
            except (EOFError, KeyboardInterrupt):
                print("\nCancelled")
                sys.exit(1)

    def resolve(
        self,
        symbol: str,
        interactive: Optional[bool] = None,
        validate: bool = True,
    ) -> str:
        """
        Resolve a symbol to its full SYMBOL.EXCHANGE format.

        Args:
            symbol: Input symbol (bare like 'MCD' or full like 'MCD.US')
            interactive: Force interactive/non-interactive mode
                        (default: auto-detect based on TTY)
            validate: If True, validate that full symbols exist in cache

        Returns:
            Full symbol in SYMBOL.EXCHANGE format

        Raises:
            SymbolNotFoundError: Symbol not found in any exchange
            AmbiguousSymbolError: Multiple matches in non-interactive mode
        """
        code, exchange = self._parse_symbol(symbol)

        # Case 1: Full symbol provided
        if exchange is not None:
            if validate:
                # Verify the symbol exists on the specified exchange
                if not self.cache.symbol_exists(code, exchange):
                    # Check if it exists elsewhere
                    alternatives = self.cache.lookup_symbol(code)
                    if alternatives:
                        alt_list = ", ".join(f"{code}.{e}" for e in alternatives)
                        raise SymbolNotFoundError(
                            symbol,
                            f"Symbol '{code}' not found on exchange '{exchange}'. "
                            f"Found on: {alt_list}"
                        )
                    else:
                        raise SymbolNotFoundError(symbol)
            return f"{code}.{exchange}"

        # Case 2 & 3: Bare symbol - lookup in index
        exchanges = self.cache.lookup_symbol(code)

        if not exchanges:
            raise SymbolNotFoundError(code)

        if len(exchanges) == 1:
            # Single match - auto-resolve
            return f"{code}.{exchanges[0]}"

        # Multiple matches
        if interactive is None:
            interactive = self._is_tty()

        if interactive:
            selected_exchange = self._prompt_selection(code, exchanges)
            return f"{code}.{selected_exchange}"
        else:
            raise AmbiguousSymbolError(code, exchanges)

    def resolve_batch(
        self,
        symbols: List[str],
        interactive: bool = False,
        skip_errors: bool = False,
    ) -> List[Tuple[str, Optional[str]]]:
        """
        Resolve multiple symbols in batch.

        Args:
            symbols: List of input symbols
            interactive: Allow interactive selection for ambiguous symbols
            skip_errors: If True, return None for errors instead of raising

        Returns:
            List of tuples (original_symbol, resolved_symbol or None if error)
        """
        results = []
        for symbol in symbols:
            try:
                resolved = self.resolve(symbol, interactive=interactive)
                results.append((symbol, resolved))
            except (SymbolNotFoundError, AmbiguousSymbolError) as e:
                if skip_errors:
                    results.append((symbol, None))
                else:
                    raise
        return results

    def suggest(self, symbol: str) -> List[str]:
        """
        Get suggestions for a symbol.

        Args:
            symbol: Input symbol (bare or full)

        Returns:
            List of possible full symbols (e.g., ['MCD.US', 'MCD.LSE'])
        """
        code, exchange = self._parse_symbol(symbol)

        if exchange:
            # Full symbol - return as-is if valid, or alternatives
            if self.cache.symbol_exists(code, exchange):
                return [f"{code}.{exchange}"]
            else:
                exchanges = self.cache.lookup_symbol(code)
                return [f"{code}.{e}" for e in (exchanges or [])]
        else:
            # Bare symbol - return all matches
            exchanges = self.cache.lookup_symbol(code)
            return [f"{code}.{e}" for e in (exchanges or [])]
