# candles-sync

A CLI tool and Python library for syncing historical candle data from multiple exchanges.

Use it standalone from the command line, or import it into your own projects to programmatically sync OHLCV data.

## Supported Exchanges

- [Bitfinex](https://www.bitfinex.com/) — Cryptocurrency exchange (no API key required)
- [EODHD](https://eodhd.com/) — Stocks, ETFs, and indices across 70+ global exchanges (API key required)

## Configuration

### EODHD API Token

EODHD requires an API token. Get one from [eodhd.com](https://eodhd.com/) and set it as an environment variable:

```bash
export EODHD_API_TOKEN="your_api_token_here"
```

Or add it to your shell profile (`~/.bashrc`, `~/.zshrc`, etc.) for persistence.

## Arguments

| Argument | Bitfinex | EODHD | Description |
|----------|:--------:|:-----:|-------------|
| `--exchange` | ✓ | ✓ | Exchange name (`BITFINEX`, `EODHD`) |
| `--ticker` | ✓ | ✓ | Symbol (`tBTCUSD` for Bitfinex, `MCD.US` for EODHD) |
| `--timeframe` | ✓ | ✓ | `1m`, `1h`, or `1D` (default: all) |
| `--end` | ✓ | ✓ | End date (default: now) |
| `--verbose` | ✓ | ✓ | Enable detailed output |
| `--live` | ✓ | ✓ | Live polling interval in seconds |
| `--bulk` | ✗ | ✓ | Bulk sync an entire exchange |
| `--bulk-date` | ✗ | ✓ | Date for bulk sync |
| `--refresh-metadata` | ✗ | ✓ | Force refresh EODHD metadata cache |
| `--get-exchanges` | ✗ | ✓ | List available EODHD exchanges |
| `--get-tickers` | ✓ | ✓ | List available tickers for an exchange |
| `--format` | ✓ | ✓ | Output format: `table`, `json`, `simple` |

## Sample Commands

### Basic Bitfinex sync
```bash
candles-sync --exchange BITFINEX --ticker tBTCUSD
```

### Basic EODHD sync
```bash
candles-sync --exchange EODHD --ticker MCD.US
```

### Specific timeframe
```bash
candles-sync --exchange BITFINEX --ticker tBTCUSD --timeframe 1h
```

### Live polling mode
```bash
candles-sync --exchange BITFINEX --ticker tBTCUSD --timeframe 1m --live 60
```

### Bulk EODHD sync
```bash
# Fetch latest EOD for all US stocks
candles-sync --bulk US

# Fetch EOD for a specific date
candles-sync --bulk US --bulk-date 2024-01-15

# Fetch London Stock Exchange
candles-sync --bulk LSE
```

**What bulk sync does:**
- Fetches **daily (1D) candles for ALL tickers** on an exchange in a single API call
- Uses EODHD's [Bulk API](https://eodhd.com/financial-apis/bulk-api-eod-splits-dividends): `GET /api/eod-bulk-last-day/{EXCHANGE}`
- Writes to `~/.corky/EODHD/candles/{SYMBOL}/1D/{YEAR}.csv`
- Skips duplicates automatically

**Bulk vs normal sync:**

| | Normal sync | Bulk sync |
|---|---|---|
| Scope | Single ticker | Entire exchange |
| Timeframes | 1m, 1h, 1D | 1D only |
| API calls | Many (paginated) | One call |
| Use case | Full history | Daily updates |

Bulk sync is ideal for **daily cron jobs** to keep all tickers current with minimal API usage.

## EODHD Timeframe Availability

Not all EODHD tickers support all timeframes. Intraday data (1m, 1h) availability varies by exchange:

| Exchange | 1m | 1h | 1D |
|----------|:--:|:--:|:--:|
| US (NYSE/NASDAQ) | ✓ | ✓ | ✓ |
| Forex/Crypto | ✓ | ✓ | ✓ |
| Other exchanges | ⚠ | ⚠ | ✓ |

⚠ = Availability varies by ticker. May return empty results.

**Note:** EODHD does not provide metadata to check intraday support programmatically. If you request 1m/1h data for a ticker that only supports daily data, the API returns empty results silently.

For details, see [EODHD Intraday API documentation](https://eodhd.com/financial-apis/intraday-historical-data-api).

### Discovery commands
```bash
# List all EODHD exchanges
candles-sync --get-exchanges

# List Bitfinex trading pairs
candles-sync --get-tickers BITFINEX

# List US stock symbols (EODHD)
candles-sync --get-tickers US

# Output as JSON for scripting
candles-sync --get-tickers BITFINEX --format json

# Output one symbol per line (pipeable)
candles-sync --get-tickers US --format simple | head -10
```

## Output File Location

Candle data is stored in `~/.corky/` with the following structure:

```
~/.corky/
└── {EXCHANGE}/
    └── candles/
        └── {TICKER}/
            └── {TIMEFRAME}/
                └── {partition}.csv
```

Partition naming varies by timeframe:
- `1m`: `YYYY-MM-DD.csv` (daily partitions)
- `1h`: `YYYY-MM.csv` (monthly partitions)
- `1D`: `YYYY.csv` (yearly partitions)

## EODHD Metadata Storage

EODHD adapter caches metadata locally to reduce API calls:

```
~/.corky/providers/eodhd/meta/
├── exchanges.json           # List of available exchanges
├── symbols/
│   └── {EXCHANGE}.json      # Symbols for each exchange
└── index.json               # Combined symbol index
```

Metadata has a 24-hour TTL and is automatically refreshed when stale. Use `--refresh-metadata` to force a refresh.

## Bitfinex Metadata Storage

Bitfinex trading pairs are cached locally:

```
~/.corky/providers/bitfinex/meta/
└── tickers.json            # List of trading pairs
```

Metadata has a 24-hour TTL and is automatically refreshed when stale.
