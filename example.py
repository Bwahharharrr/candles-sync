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

The script automatically fetches/syncs 3 timeframes: 1m, 1h, and 1D.

{INFO} Examples:
  python example.py --exchange BITFINEX --ticker tBTCUSD --end "2024-02-10"
  python example.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1h
  python example.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1m --live 10
""", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--exchange",  required=True, help="Exchange name (e.g., BITFINEX)")
    parser.add_argument("--ticker",    required=True, help="Trading pair (e.g., tBTCUSD)")
    parser.add_argument("--end",       help="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    parser.add_argument("--timeframe", help="Specific timeframe to sync (1m, 1h, or 1D). If not specified, all timeframes will be synced.")
    parser.add_argument("--live",      type=int, metavar="SECS", help="Poll interval in seconds (enables live polling mode)")
    parser.add_argument("--verbose",   action="store_true", help="Verbose debug output for live polling")
    if len(sys.argv) == 1:
        print(f"\n{ERROR} No arguments provided! Please specify the required parameters.\n")
        parser.print_help()
        sys.exit(1)
    return parser.parse_args()


def main():
    args = parse_args()

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