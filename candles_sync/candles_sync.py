"""
candles_sync.py

This script synchronizes historical candle data for a given exchange/ticker/timeframe.
It uses the Bitfinex API to fetch candle data and saves daily CSV files in:
    {ROOT_PATH}/{EXCHANGE}/candles/{TICKER}/{timeframe}

Features:
- First-time sync (no ".gotstart" file): queries from the epoch (0) to build the full history.
- Incremental sync (".gotstart" exists): uses user-supplied start/end dates (if provided) to restrict saving.
- Checks for any missing daily CSV files (from the day of the last recorded candle up to the expected end)
  and fills the gaps by repeatedly querying the API until the gap is filled.
- The temporary "day" column used for grouping is dropped from the final CSV.
"""

import argparse
import os
import requests
import pandas as pd
import decimal
import sys
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

# ---------------------
# COLOR SETUP (ANSI)
# ---------------------
try:
    import colorama
    from colorama import Fore, Style
    colorama.init(autoreset=True)
except ImportError:
    class NoColor:
        def __getattr__(self, item):
            return ''
    Fore = Style = NoColor()

# Log tag definitions
INFO = Fore.GREEN + "[INFO]" + Style.RESET_ALL
WARNING = Fore.YELLOW + "[WARNING]" + Style.RESET_ALL
ERROR = Fore.RED + "[ERROR]" + Style.RESET_ALL
SUCCESS = Fore.GREEN + "[SUCCESS]" + Style.RESET_ALL
UPDATE = Fore.MAGENTA + "[UPDATE]" + Style.RESET_ALL

# Additional color definitions for CLI output
COLOR_DIR = Fore.CYAN
COLOR_FILE = Fore.YELLOW
COLOR_TIMESTAMPS = Fore.MAGENTA
COLOR_ROWS = Fore.RED
COLOR_NEW = Fore.WHITE
COLOR_VAR = Fore.CYAN
COLOR_TYPE = Fore.YELLOW
COLOR_DESC = Fore.MAGENTA
COLOR_REQ = Fore.RED + "[REQUIRED]" + Style.RESET_ALL

# Bitfinex API URL template
BITFINEX_API_URL = "https://api-pub.bitfinex.com/v2/candles/trade:{}:{}/hist"


ROOT_PATH = os.path.expanduser("~/.corky")


def ensure_directory(exchange: str, ticker: str, timeframe: str) -> str:
    """
    Ensures that the directory {ROOT_PATH}/{exchange}/candles/{ticker}/{timeframe} exists.
    Returns the directory path.
    """
    dir_path = os.path.join(ROOT_PATH, exchange, "candles", ticker, timeframe)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        print(f"{INFO} Created directory: {COLOR_DIR}{dir_path}{Style.RESET_ALL}")
    else:
        print(f"{INFO} Directory already exists: {COLOR_DIR}{dir_path}{Style.RESET_ALL}")
    return dir_path


def get_last_file_timestamp(dir_path: str):
    """
    Retrieves the most recent CSV file's last timestamp (in milliseconds)
    based on file names in the format YYYY-MM-DD.csv.
    Returns the timestamp or None if no valid file is found.
    """
    csv_files = sorted(
        [f for f in os.listdir(dir_path) if f.endswith('.csv')],
        key=lambda x: datetime.strptime(x.split('.')[0], '%Y-%m-%d')
    )
    if not csv_files:
        return None
    last_file = csv_files[-1]
    last_file_path = os.path.join(dir_path, last_file)
    df = pd.read_csv(last_file_path, dtype=str, parse_dates=False)
    if "timestamp" not in df.columns or df.empty:
        print(f"{WARNING} No valid timestamp data found in {COLOR_FILE}{last_file_path}{Style.RESET_ALL}.")
        return None
    df["timestamp"] = df["timestamp"].astype("int64")
    last_timestamp = df["timestamp"].max()
    human_time = datetime.fromtimestamp(last_timestamp / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    print(f"{INFO} Last recorded timestamp from {COLOR_FILE}{last_file}{Style.RESET_ALL}: {COLOR_TIMESTAMPS}{last_timestamp}{Style.RESET_ALL} ({human_time})")
    return last_timestamp


def fetch_bitfinex_candles(symbol: str, timeframe: str, start: int, limit: int = 10000):
    """
    Fetches up to 10,000 candles from Bitfinex starting from the specified timestamp.
    Returns the list of candles or None if the request fails.
    """
    url = BITFINEX_API_URL.format(timeframe, symbol)
    params = {"limit": limit, "sort": 1, "start": start}
    full_url = f"{url}?{urlencode(params)}"
    print(f"{INFO} Fetching candles from Bitfinex API...")
    print(f"       Full URL (Copy & Paste in Browser/Postman): {COLOR_FILE}{full_url}{Style.RESET_ALL}")
    try:
        response = requests.get(full_url)
        if response.status_code == 200:
            data = response.json()
            if not data:
                print(f"{WARNING} No candles received from API.")
                return None
            timestamps = [candle[0] for candle in data]
            first_ts = min(timestamps)
            last_ts = max(timestamps)
            first_time = datetime.fromtimestamp(first_ts / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            last_time = datetime.fromtimestamp(last_ts / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f"{SUCCESS} Received {len(data)} candles from API ({COLOR_TIMESTAMPS}{first_time}{Style.RESET_ALL} → {COLOR_TIMESTAMPS}{last_time}{Style.RESET_ALL})")
            return data
        else:
            print(f"{ERROR} Failed to fetch candles: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"{ERROR} Request failed due to network issue: {e}")
        return None


def validate_and_update_candles(df_existing: pd.DataFrame, df_new: pd.DataFrame, csv_path: str) -> int:
    """
    Merges existing CSV data with new candle data, preserving exact decimal precision.
    Writes the merged data back to csv_path.
    Returns the number of rows updated.
    """
    df_existing = pd.read_csv(csv_path, dtype=str, parse_dates=False)
    df_existing["timestamp"] = df_existing["timestamp"].astype("int64")
    df_new["timestamp"] = df_new["timestamp"].astype("int64")
    numeric_columns = ["open", "close", "high", "low", "volume"]
    
    # Convert numeric columns to Decimal for precise comparison
    for col in numeric_columns:
        if col in df_existing.columns:
            df_existing[col] = df_existing[col].apply(lambda x: decimal.Decimal(x) if x else decimal.Decimal("0"))
        if col in df_new.columns:
            df_new[col] = df_new[col].apply(lambda x: decimal.Decimal(str(x)))

    # Drop temporary columns if present
    df_existing.drop(columns=["day"], errors="ignore", inplace=True)
    df_new.drop(columns=["day"], errors="ignore", inplace=True)

    # Merge dataframes
    merged_df = pd.merge(df_existing, df_new, on="timestamp", how="outer",
                        suffixes=("_old", "_new"), indicator=True)

    # Track actual changes
    updated_rows = 0
    for idx, row in merged_df.iterrows():
        if row['_merge'] == 'both':  # Only check rows that exist in both datasets
            row_changed = False
            for col in numeric_columns:
                old_col, new_col = f"{col}_old", f"{col}_new"
                if old_col in row and new_col in row:
                    if row[old_col] != row[new_col]:
                        row_changed = True
                        break
            if row_changed:
                updated_rows += 1

    # Update the values and prepare for saving
    for col in numeric_columns:
        old_col, new_col = f"{col}_old", f"{col}_new"
        if old_col in merged_df.columns and new_col in merged_df.columns:
            merged_df[col] = merged_df[new_col].combine_first(merged_df[old_col])
            merged_df[col] = merged_df[col].astype(str)
            merged_df.drop(columns=[old_col, new_col], inplace=True)

    merged_df.drop(columns=["_merge"], inplace=True)
    merged_df.to_csv(csv_path, index=False)

    return updated_rows


def save_candles_to_csv(candles, dir_path: str, symbol: str, timeframe: str,
                        range_start_ms: int = None, range_end_ms: int = None):
    """
    Saves candle data to daily CSV files.
    
    - If a ".gotstart" file exists, only candles within the user-supplied range are saved.
    - Otherwise (first-time sync), all fetched candles are saved.
    - Creates empty CSV files for missing days.
    - The temporary "day" column is used solely for grouping and is dropped before saving.
    """
    gotstart_exists = os.path.exists(os.path.join(dir_path, ".gotstart"))
    use_range = gotstart_exists

    if not candles:
        if range_start_ms is not None and range_end_ms is not None and use_range:
            start_dt = datetime.fromtimestamp(range_start_ms / 1000, timezone.utc).date()
            end_dt = datetime.fromtimestamp(range_end_ms / 1000, timezone.utc).date()
            day_count = (end_dt - start_dt).days + 1
            for i in range(day_count):
                day_i = start_dt + timedelta(days=i)
                csv_filename = f"{day_i}.csv"
                csv_path_day = os.path.join(dir_path, csv_filename)
                if not os.path.exists(csv_path_day):
                    empty_df = pd.DataFrame(columns=["timestamp", "open", "close", "high", "low", "volume"])
                    empty_df.to_csv(csv_path_day, index=False)
                    print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} Created empty CSV for missing day {COLOR_TIMESTAMPS}{day_i}{Style.RESET_ALL} → {COLOR_FILE}.../{symbol}/{timeframe}/{csv_filename}{Style.RESET_ALL}")
        else:
            print(f"{WARNING} No candles to save. Exiting...")
        return

    # Create DataFrame from candle data.
    df_new = pd.DataFrame(candles, columns=["timestamp", "open", "close", "high", "low", "volume"])
    for col in ["open", "close", "high", "low", "volume"]:
        df_new[col] = df_new[col].apply(lambda x: decimal.Decimal(str(x)))
    if use_range and range_start_ms is not None and range_end_ms is not None:
        df_new = df_new[(df_new["timestamp"] >= range_start_ms) & (df_new["timestamp"] <= range_end_ms)]
    
    # Create a temporary "day" column for grouping.
    df_new["day"] = pd.to_datetime(df_new["timestamp"], unit="ms", utc=True).dt.date

    days_in_range = set()
    if use_range and range_start_ms is not None and range_end_ms is not None:
        start_dt = datetime.fromtimestamp(range_start_ms / 1000, timezone.utc).date()
        end_dt = datetime.fromtimestamp(range_end_ms / 1000, timezone.utc).date()
        day_count = (end_dt - start_dt).days + 1
        for i in range(day_count):
            days_in_range.add(start_dt + timedelta(days=i))

    grouped = df_new.groupby("day")
    for day, group in grouped:
        csv_filename = f"{day}.csv"
        csv_path_day = os.path.join(dir_path, csv_filename)
        # Drop the temporary "day" column before saving.
        group_to_save = group.drop(columns=["day"])
        # Convert numeric columns back to string.
        group_to_save["timestamp"] = group_to_save["timestamp"].astype(str)
        group_to_save["open"] = group_to_save["open"].astype(str)
        group_to_save["close"] = group_to_save["close"].astype(str)
        group_to_save["high"] = group_to_save["high"].astype(str)
        group_to_save["low"] = group_to_save["low"].astype(str)
        group_to_save["volume"] = group_to_save["volume"].astype(str)
        if os.path.exists(csv_path_day):
            df_existing = pd.read_csv(csv_path_day, dtype=str, parse_dates=False)
            if "timestamp" in df_existing.columns:
                df_existing["timestamp"] = df_existing["timestamp"].astype("int64")
            updated_rows = validate_and_update_candles(df_existing, group_to_save, csv_path_day)
            if updated_rows > 0:
                print(f"{UPDATE} {COLOR_ROWS}{updated_rows}{Style.RESET_ALL} rows corrected in {COLOR_FILE}.../{symbol}/{timeframe}/{csv_filename}{Style.RESET_ALL}")
        else:
            group_to_save.to_csv(csv_path_day, index=False, float_format="%.17g")
            start_ts = group_to_save["timestamp"].astype(int).min()
            end_ts = group_to_save["timestamp"].astype(int).max()
            start_time_str = datetime.fromtimestamp(start_ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
            end_time_str = datetime.fromtimestamp(end_ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
            print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} {len(group_to_save)} candles [{COLOR_TIMESTAMPS}{start_time_str} - {end_time_str}{Style.RESET_ALL}] → {COLOR_FILE}.../{symbol}/{timeframe}/{csv_filename}{Style.RESET_ALL}")
    if use_range and range_start_ms is not None and range_end_ms is not None:
        days_with_data = set(grouped.groups.keys())
        missing_days = days_in_range - days_with_data
        for day in sorted(missing_days):
            csv_filename = f"{day}.csv"
            csv_path_day = os.path.join(dir_path, csv_filename)
            if not os.path.exists(csv_path_day):
                empty_df = pd.DataFrame(columns=["timestamp", "open", "close", "high", "low", "volume"])
                empty_df.to_csv(csv_path_day, index=False)
                print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} Created empty CSV for missing day {COLOR_TIMESTAMPS}{day}{Style.RESET_ALL} → {COLOR_FILE}.../{symbol}/{timeframe}/{csv_filename}{Style.RESET_ALL}")


def find_missing_dates(dir_path: str, expected_start=None, expected_end=None):
    """
    Scans the directory for missing dates between expected_start and expected_end.
    Returns a list of missing date ranges.
    If expected_start or expected_end is not provided, they default to the earliest and latest dates found.
    """
    csv_files = sorted(
        [f for f in os.listdir(dir_path) if f.endswith('.csv')],
        key=lambda x: datetime.strptime(x.split('.')[0], '%Y-%m-%d')
    )
    if not csv_files:
        return []
    available_dates = sorted(datetime.strptime(f.split('.')[0], '%Y-%m-%d').date() for f in csv_files)
    if expected_start is None:
        expected_start = available_dates[0]
    if expected_end is None:
        expected_end = available_dates[-1]
    all_dates = set(expected_start + timedelta(days=i) for i in range((expected_end - expected_start).days + 1))
    missing_dates = sorted(all_dates - set(available_dates))
    missing_ranges = []
    start_date = None
    for i in range(len(missing_dates)):
        if start_date is None:
            start_date = missing_dates[i]
        if i == len(missing_dates) - 1 or missing_dates[i] + timedelta(days=1) != missing_dates[i + 1]:
            end_date = missing_dates[i]
            missing_ranges.append((start_date, end_date))
            start_date = None
    if missing_ranges:
        print(f"{WARNING} Missing {len(missing_ranges)} date range(s) in dataset. Rebuilding missing days.")
        for start, end in missing_ranges:
            if start == end:
                print(f"  - {COLOR_TIMESTAMPS}{start}{Style.RESET_ALL}")
            else:
                print(f"  - {COLOR_TIMESTAMPS}{start} → {end}{Style.RESET_ALL}")
    return missing_ranges


def synchronize_candle_data(exchange: str,
                            ticker: str,
                            timeframe: str,
                            end_date_str: str = None,
                            verbose: bool = False) -> bool:
    # Add compact output at the start
    if not verbose:
        end_str = f" → {end_date_str}" if end_date_str else ""
        print(f"{INFO} Syncing {COLOR_VAR}{exchange}/{ticker}/{timeframe}{Style.RESET_ALL}{end_str}")
    else:
        print(f"\n{INFO} Running synchronization with the following parameters:\n")
        print(f"  {COLOR_VAR}--exchange{Style.RESET_ALL}  {COLOR_TYPE}(str){Style.RESET_ALL}  {exchange}")
        print(f"  {COLOR_VAR}--ticker{Style.RESET_ALL}    {COLOR_TYPE}(str){Style.RESET_ALL}  {ticker}")
        print(f"  {COLOR_VAR}--timeframe{Style.RESET_ALL} {COLOR_TYPE}(str){Style.RESET_ALL}  {timeframe}")
        if end_date_str:
            print(f"  {COLOR_VAR}--end{Style.RESET_ALL}       {COLOR_TYPE}(str){Style.RESET_ALL}  {end_date_str}")
        print(f"\n{INFO} Starting data synchronization...\n")

    dir_path = ensure_directory(exchange, ticker, timeframe)
    gotstart_path = os.path.join(dir_path, ".gotstart")

    if not os.path.exists(gotstart_path):
        print(f"{WARNING} No '.gotstart' file found. Starting full historical sync...")
        last_timestamp = 0
    else:
        print(f"{INFO} '.gotstart' file exists. Verifying data continuity...")

    # Parse end date/time
    end_ts_ms = None
    end_date = None
    if end_date_str:
        try:
            dt_end = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        except ValueError:
            dt_end = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_ts_ms = int(dt_end.timestamp() * 1000)
        end_date = dt_end.date()

    if os.path.exists(gotstart_path):
        # Get list of existing CSV files
        csv_files = sorted(
            [f for f in os.listdir(dir_path) if f.endswith('.csv')],
            key=lambda x: datetime.strptime(x.split('.')[0], '%Y-%m-%d')
        )
        
        if not csv_files:
            print(f"{WARNING} No CSV files found despite '.gotstart' existing. Starting fresh sync...")
        else:
            earliest_file_date = datetime.strptime(csv_files[0].split('.')[0], '%Y-%m-%d').date()
            latest_file_date = datetime.strptime(csv_files[-1].split('.')[0], '%Y-%m-%d').date()
            today_date = datetime.now(timezone.utc).date()
            
            # Determine the effective date range
            effective_end = end_date if end_date else today_date

            # Check for missing dates in the range
            missing_ranges = find_missing_dates(dir_path, earliest_file_date, effective_end)
            
            if not missing_ranges and end_date:
                # Only exit early if we have all files AND an end date was specified
                print(f"{SUCCESS} All files exist for the date range {COLOR_TIMESTAMPS}{earliest_file_date}{Style.RESET_ALL} → {COLOR_TIMESTAMPS}{effective_end}{Style.RESET_ALL}")
                return True
            
            today_date = datetime.now(timezone.utc).date()
            refreshed_today = False  # Track if we've already fetched today's data

            if missing_ranges:
                print(f"{WARNING} Found gaps in the date range. Fetching missing data...")
                for (rng_start, rng_end) in missing_ranges:
                    ms_s = int(datetime.combine(rng_start, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
                    ms_e = int(datetime.combine(rng_end, datetime.max.time(), tzinfo=timezone.utc).timestamp() * 1000)
                    current_start = ms_s
                    
                    # Make initial request
                    candles = fetch_bitfinex_candles(symbol=ticker, timeframe=timeframe, start=current_start, limit=10000)
                    if not candles:
                        # If no data received, create empty files for the remaining range
                        current_date = datetime.fromtimestamp(current_start / 1000, timezone.utc).date()
                        while current_date <= rng_end:
                            csv_filename = f"{current_date}.csv"
                            csv_path = os.path.join(dir_path, csv_filename)
                            if not os.path.exists(csv_path):
                                empty_df = pd.DataFrame(columns=["timestamp", "open", "close", "high", "low", "volume"])
                                empty_df.to_csv(csv_path, index=False)
                                print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} Created empty CSV for missing day {COLOR_TIMESTAMPS}{current_date}{Style.RESET_ALL} → {COLOR_FILE}.../{ticker}/{timeframe}/{csv_filename}{Style.RESET_ALL}")
                            current_date += timedelta(days=1)
                        continue

                    save_candles_to_csv(candles, dir_path, ticker, timeframe,
                                      range_start_ms=ms_s, range_end_ms=ms_e)
                    
                    # Check if this range included today's data
                    if rng_end >= today_date:
                        refreshed_today = True
                    
                    # If we received fewer than 10,000 candles, we have all available data for this range
                    if len(candles) < 10000:
                        print(f"{INFO} Received fewer than 10,000 candles ({len(candles)}). All available data retrieved for this range.")
                        continue

                    # Only continue fetching if we got the maximum number of candles
                    while len(candles) == 10000:
                        current_start = max(c[0] for c in candles)
                        if current_start >= ms_e:
                            break
                        candles = fetch_bitfinex_candles(symbol=ticker, timeframe=timeframe, start=current_start, limit=10000)
                        if not candles:
                            break
                        save_candles_to_csv(candles, dir_path, ticker, timeframe,
                                          range_start_ms=ms_s, range_end_ms=ms_e)

            if not end_date and not refreshed_today:
                # Only check for updates to current date if we haven't already fetched it
                today_start = int(datetime.combine(today_date, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
                print(f"{INFO} No end date specified. Checking for updates to current data...")
                candles = fetch_bitfinex_candles(symbol=ticker, timeframe=timeframe, start=today_start, limit=10000)
                if candles:
                    save_candles_to_csv(candles, dir_path, ticker, timeframe)
                    if len(candles) == 10000:
                        print(f"{WARNING} Received maximum number of candles. There might be more data available.")

            return True

    # If we get here, we need to do a full sync
    last_timestamp = 0
    # ... rest of the existing synchronize_candle_data function for full sync ...


def parse_args():
    """
    Parses command-line arguments with a color-coded help output.
    """
    parser = argparse.ArgumentParser(
        description="""
{info} Fetch missing historical candle data from an exchange. {reset}

  {var}--exchange{reset}   {type}(str){reset}    {req} {desc}Exchange name (e.g., BITFINEX){reset}
  {var}--ticker{reset}     {type}(str){reset}    {req} {desc}Trading pair (e.g., tBTCUSD){reset}
  {var}--timeframe{reset}  {type}(str){reset}    {req} {desc}Timeframe (e.g., 1m, 1h, 1D){reset}
  {var}--end{reset}        {type}(str){reset}    {desc}End date (YYYY-MM-DD or YYYY-MM-DD HH:MM, optional){reset}

{info} Example Usage: {reset}
  {file}python candles_sync.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1h --end "2024-02-10"{reset}
  {file}python candles_sync.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1h{reset} (Fetch all data)
""".format(info=INFO, var=COLOR_VAR, type=COLOR_TYPE, desc=COLOR_DESC,
           req=COLOR_REQ, reset=Style.RESET_ALL, file=COLOR_FILE),
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--exchange", required=True, help="Exchange name (e.g., BITFINEX)")
    parser.add_argument("--ticker", required=True, help="Trading pair (e.g., tBTCUSD)")
    parser.add_argument("--timeframe", required=True, help="Timeframe (e.g., 1m, 1h, 1D)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM, optional)")
    if len(sys.argv) == 1:
        print(f"\n{ERROR} No arguments provided! Please specify the required parameters.\n")
        parser.print_help()
        sys.exit(1)
    return parser.parse_args()


def main():
    """Main entry point for the script."""
    args = parse_args()
    success = synchronize_candle_data(
        exchange=args.exchange,
        ticker=args.ticker,
        timeframe=args.timeframe,
        end_date_str=args.end,
        verbose=True  # Use verbose output when run from main
    )

    if success:
        print(f"\n{SUCCESS} Synchronization completed successfully.\n")
    else:
        print(f"\n{ERROR} Synchronization failed.\n")


if __name__ == "__main__":
    # This allows the script to be run directly
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    main()

