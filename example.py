#!/usr/bin/env python3

import argparse
import sys
import time
from candles_sync.candles_sync import synchronize_candle_data, Fore, Style, INFO, ERROR, SUCCESS, COLOR_VAR, COLOR_TYPE, COLOR_REQ


def parse_args():
    """
    Parses command-line arguments with colorized help.
    The --timeframe argument is removed, as we always fetch 1m, 1h, and 1D.
    """
    parser = argparse.ArgumentParser(
        description=f"""
{INFO} Fetch missing historical candle data from an exchange. {Style.RESET_ALL}
  {COLOR_VAR}--exchange{Style.RESET_ALL}   {COLOR_TYPE}(str){Style.RESET_ALL} {COLOR_REQ} Exchange name
  {COLOR_VAR}--ticker{Style.RESET_ALL}     {COLOR_TYPE}(str){Style.RESET_ALL} {COLOR_REQ} Trading pair
  {COLOR_VAR}--end{Style.RESET_ALL}        {COLOR_TYPE}(str){Style.RESET_ALL} End date (optional)
  {COLOR_VAR}--timeframe{Style.RESET_ALL}  {COLOR_TYPE}(str){Style.RESET_ALL} Specific timeframe (1m, 1h, or 1D)
  {COLOR_VAR}--live{Style.RESET_ALL}       {COLOR_TYPE}(int){Style.RESET_ALL} Poll interval in seconds (enables live mode)
  {COLOR_VAR}--verbose{Style.RESET_ALL}    {COLOR_TYPE}(flag){Style.RESET_ALL} Verbose debug output for live polling

{INFO} EODHD-specific options:
  {COLOR_VAR}--bulk{Style.RESET_ALL}             {COLOR_TYPE}(str){Style.RESET_ALL} Bulk sync all tickers on an exchange (e.g., US)
  {COLOR_VAR}--refresh-metadata{Style.RESET_ALL} {COLOR_TYPE}(flag){Style.RESET_ALL} Force refresh EODHD metadata cache

The script automatically fetches/syncs 3 timeframes: 1m, 1h, and 1D.

{INFO} Examples:
  python example.py --exchange BITFINEX --ticker tBTCUSD --end "2024-02-10"
  python example.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1h
  python example.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1m --live 10
  python example.py --exchange EODHD --ticker MCD.US --timeframe 1D
  python example.py --bulk US
  python example.py --refresh-metadata
""", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--exchange",  help="Exchange name (e.g., BITFINEX, EODHD)")
    parser.add_argument("--ticker",    help="Trading pair (e.g., tBTCUSD, MCD.US)")
    parser.add_argument("--end",       help="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    parser.add_argument("--timeframe", help="Specific timeframe to sync (1m, 1h, or 1D). If not specified, all timeframes will be synced.")
    parser.add_argument("--live",      type=int, metavar="SECS", help="Poll interval in seconds (enables live polling mode)")
    parser.add_argument("--verbose",   action="store_true", help="Verbose debug output for live polling")
    # EODHD-specific options
    parser.add_argument("--bulk",      metavar="EXCHANGE", help="Bulk sync all tickers on an exchange (EODHD only)")
    parser.add_argument("--bulk-date", metavar="DATE", help="Date for bulk sync (YYYY-MM-DD, default: latest)")
    parser.add_argument("--refresh-metadata", action="store_true", help="Force refresh EODHD metadata cache")
    if len(sys.argv) == 1:
        print(f"\n{ERROR} No arguments provided! Please specify the required parameters.\n")
        parser.print_help()
        sys.exit(1)
    return parser.parse_args()


def handle_eodhd_metadata_refresh():
    """Refresh EODHD metadata cache."""
    from candles_sync.providers.eodhd import MetadataCache
    print(f"{INFO} Refreshing EODHD metadata cache...")
    cache = MetadataCache()
    cache.refresh_all()
    print(f"{SUCCESS} Metadata cache refreshed successfully.")


def handle_bulk_sync(exchange_code: str, date: str = None, verbose: bool = False):
    """Handle EODHD bulk sync for an exchange."""
    from candles_sync.providers.eodhd import bulk_sync_exchange
    print(f"{INFO} Starting bulk sync for exchange: {exchange_code}")
    results = bulk_sync_exchange(exchange_code, date=date, verbose=verbose)
    success_count = sum(1 for v in results.values() if v)
    error_count = sum(1 for v in results.values() if not v)
    print(f"\n{SUCCESS} Bulk sync complete: {success_count} tickers succeeded, {error_count} failed")
    return error_count == 0


def main():
    args = parse_args()

    # Handle EODHD-specific commands first
    if args.refresh_metadata:
        try:
            handle_eodhd_metadata_refresh()
        except Exception as e:
            print(f"\n{ERROR} Failed to refresh metadata: {e}\n")
            sys.exit(1)
        return

    if args.bulk:
        try:
            ok = handle_bulk_sync(args.bulk, date=args.bulk_date, verbose=args.verbose)
            if not ok:
                sys.exit(1)
        except Exception as e:
            print(f"\n{ERROR} Bulk sync failed: {e}\n")
            sys.exit(1)
        return

    # For normal sync, require exchange and ticker
    if not args.exchange or not args.ticker:
        print(f"\n{ERROR} --exchange and --ticker are required for normal sync.\n")
        print(f"  Use --bulk EXCHANGE for bulk sync, or --refresh-metadata for metadata refresh.\n")
        sys.exit(1)

    # Determine which timeframes to synchronize
    if args.timeframe:
        if args.timeframe not in ["1m", "1h", "1D"]:
            print(f"\n{ERROR} Invalid timeframe: {args.timeframe}. Valid options are: 1m, 1h, 1D\n")
            sys.exit(1)
        timeframes = [args.timeframe]
    else:
        timeframes = ["1m", "1h", "1D"]

    # Live polling mode
    if args.live:
        if args.live < 1:
            print(f"\n{ERROR} --live interval must be at least 1 second\n")
            sys.exit(1)
        tf = timeframes[0]  # Use first/only timeframe for live mode
        if len(timeframes) > 1:
            print(f"{INFO} Live mode uses single timeframe, defaulting to: {tf}")
        print(f"{INFO} Live polling {args.exchange}/{args.ticker}/{tf} every {args.live}s (Ctrl+C to stop)")
        try:
            while True:
                synchronize_candle_data(
                    exchange=args.exchange,
                    ticker=args.ticker,
                    timeframe=tf,
                    end_date_str=args.end,
                    verbose=args.verbose,
                    polling=True,
                    debug=args.verbose,
                )
                time.sleep(args.live)
        except KeyboardInterrupt:
            print(f"\n{INFO} Stopped.")
        return

    # Normal sync mode
    print(f"{INFO} Synchronizing: {', '.join(timeframes)}")
    for tf in timeframes:
        ok = synchronize_candle_data(
            exchange=args.exchange,
            ticker=args.ticker,
            timeframe=tf,
            end_date_str=args.end,
            verbose=True
        )
        if not ok:
            print(f"\n{ERROR} Synchronization failed for {tf}.\n")
            sys.exit(1)

    if args.timeframe:
        print(f"\n{SUCCESS} Synchronization completed successfully for timeframe: {args.timeframe}.\n")
    else:
        print(f"\n{SUCCESS} Synchronization completed successfully for all timeframes (1m, 1h, 1D).\n")


# Example of directly using the synchronize_candle_data function
# Uncomment this code to use it instead of the command-line interface

# success = synchronize_candle_data(
#     exchange="BITFINEX",
#     ticker="tBTCUSD",
#     timeframe="1h",
#     end_date_str="2024-02-10",  # Optional
#     verbose=True                 # Optional
# )
#
# if success:
#     print("Synchronization completed successfully")


if __name__ == "__main__":
    main()