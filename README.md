# candles-sync

A CLI tool for syncing historical candle data from multiple exchanges.

## Arguments

| Argument | Bitfinex | EODHD | Description |
|----------|----------|-------|-------------|
| `--exchange` | yes | yes | Exchange name (`BITFINEX`, `EODHD`) |
| `--ticker` | yes | yes | Symbol (`tBTCUSD` for Bitfinex, `MCD.US` for EODHD) |
| `--timeframe` | yes | yes | `1m`, `1h`, or `1D` (default: all) |
| `--end` | yes | yes | End date (default: now) |
| `--verbose` | yes | yes | Enable detailed output |
| `--live` | yes | yes | Live polling interval in seconds |
| `--bulk` | no | yes | Bulk sync an entire exchange |
| `--bulk-date` | no | yes | Date for bulk sync |
| `--refresh-metadata` | no | yes | Force refresh EODHD metadata cache |

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
candles-sync --exchange EODHD --bulk US --bulk-date 2024-01-15
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
