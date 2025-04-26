#!/usr/bin/env python3
"""
candles_sync.py

This script synchronizes historical candle data for a given exchange/ticker/timeframe.
It uses the Bitfinex API to fetch candle data and saves daily CSV files in:
    {ROOT_PATH}/{EXCHANGE}/candles/{TICKER}/{timeframe}

Features:
- First‐time sync (no ".gotstart" file): queries from the epoch (0) to build the full history.
- Incremental sync (".gotstart" exists): finds missing date ranges and backfills them.
- Robust backoff on network errors and rate limits.
- Preserves all logging output, ANSI colors, function names, and CLI unchanged.
"""

import argparse
import os
import sys
import time
import requests
import pandas as pd
import decimal
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
        def __getattr__(self, item): return ''
    Fore = Style = NoColor()

# Log tag definitions
INFO    = Fore.GREEN   + "[INFO]"    + Style.RESET_ALL
WARNING = Fore.YELLOW  + "[WARNING]" + Style.RESET_ALL
ERROR   = Fore.RED     + "[ERROR]"   + Style.RESET_ALL
SUCCESS = Fore.GREEN   + "[SUCCESS]" + Style.RESET_ALL
UPDATE  = Fore.MAGENTA + "[UPDATE]"  + Style.RESET_ALL

# Additional color definitions for CLI output
COLOR_DIR        = Fore.CYAN
COLOR_FILE       = Fore.YELLOW
COLOR_TIMESTAMPS = Fore.MAGENTA
COLOR_ROWS       = Fore.RED
COLOR_NEW        = Fore.WHITE
COLOR_VAR        = Fore.CYAN
COLOR_TYPE       = Fore.YELLOW
COLOR_DESC       = Fore.MAGENTA
COLOR_REQ        = Fore.RED + "[REQUIRED]" + Style.RESET_ALL

# Bitfinex API URL template
BITFINEX_API_URL = "https://api-pub.bitfinex.com/v2/candles/trade:{}:{}/hist"
ROOT_PATH        = os.path.expanduser("~/.corky")


def ensure_directory(exchange: str, ticker: str, timeframe: str) -> str:
    """Ensures that the data directory exists."""
    dir_path = os.path.join(ROOT_PATH, exchange, "candles", ticker, timeframe)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        print(f"{INFO} Created directory: {COLOR_DIR}{dir_path}{Style.RESET_ALL}")
    else:
        print(f"{INFO} Directory already exists: {COLOR_DIR}{dir_path}{Style.RESET_ALL}")
    return dir_path


def get_last_file_timestamp(dir_path: str):
    """
    Retrieves the most recent CSV file's max timestamp.
    """
    files = sorted(
        f for f in os.listdir(dir_path) if f.endswith(".csv")
    )
    if not files: return None
    latest = files[-1]
    df = pd.read_csv(os.path.join(dir_path, latest), dtype=str)
    if "timestamp" not in df.columns or df.empty:
        print(f"{WARNING} No valid timestamp in {latest}")
        return None
    df["timestamp"] = df["timestamp"].astype("int64")
    ts = df["timestamp"].max()
    human = datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"{INFO} Last recorded timestamp from {COLOR_FILE}{latest}{Style.RESET_ALL}: {COLOR_TIMESTAMPS}{ts}{Style.RESET_ALL} ({human})")
    return ts


def fetch_bitfinex_candles(symbol: str, timeframe: str, start: int, limit: int = 10000):
    """
    Fetches up to `limit` candles starting at `start`, with exponential backoff.
    """
    url = BITFINEX_API_URL.format(timeframe, symbol)
    params = {"limit": limit, "sort": 1, "start": start}

    delay, max_delay = 30, 300
    while True:
        full = f"{url}?{urlencode(params)}"
        print(f"{INFO} Fetching candles from Bitfinex API...")
        print(f"       URL: {COLOR_FILE}{full}{Style.RESET_ALL}")
        try:
            resp = requests.get(full)
        except Exception as e:
            print(f"{ERROR} Network error: {e}. Retrying in {delay}s...")
            time.sleep(delay); delay = min(max_delay, delay*2); continue

        if resp.status_code == 200:
            data = resp.json()
            if not data:
                print(f"{WARNING} No candles returned from API.")
                return None
            # reset backoff
            delay = 30
            ts = [c[0] for c in data]
            first = datetime.fromtimestamp(min(ts)/1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            last  = datetime.fromtimestamp(max(ts)/1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"{SUCCESS} Received {len(data)} candles ({COLOR_TIMESTAMPS}{first}{Style.RESET_ALL} → {COLOR_TIMESTAMPS}{last}{Style.RESET_ALL})")
            return data
        if resp.status_code == 429:
            print(f"{WARNING} Rate limit hit (429). Retrying in {delay}s...")
            time.sleep(delay); delay = min(max_delay, delay*2); continue
        print(f"{ERROR} HTTP {resp.status_code}: {resp.text}")
        return None


def validate_and_update_candles(df_existing: pd.DataFrame, df_new: pd.DataFrame, csv_path: str) -> int:
    """Merges existing vs new, counts changed rows, writes CSV."""
    df_old = pd.read_csv(csv_path, dtype=str)
    df_old["timestamp"] = df_old["timestamp"].astype("int64")
    df_new["timestamp"] = df_new["timestamp"].astype("int64")
    cols = ["open","close","high","low","volume"]
    for col in cols:
        if col in df_old: df_old[col] = df_old[col].apply(lambda x: decimal.Decimal(x or "0"))
        df_new[col] = df_new[col].apply(lambda x: decimal.Decimal(str(x)))
    df_old.drop(columns=["day"], errors="ignore", inplace=True)
    df_new.drop(columns=["day"], errors="ignore", inplace=True)

    merged = pd.merge(df_old, df_new, on="timestamp", how="outer",
                      suffixes=("_old","_new"), indicator=True)
    updated = 0
    for _, r in merged.iterrows():
        if r["_merge"]=="both" and any(r[f"{c}_old"]!=r[f"{c}_new"] for c in cols):
            updated+=1

    for c in cols:
        o,n = f"{c}_old", f"{c}_new"
        if o in merged and n in merged:
            merged[c] = merged[n].combine_first(merged[o]).astype(str)
            merged.drop(columns=[o,n], inplace=True)
    merged.drop(columns=["_merge"], inplace=True)
    merged.to_csv(csv_path, index=False)
    return updated


def save_candles_to_csv(candles, dir_path: str, symbol: str, timeframe: str,
                        range_start_ms: int=None, range_end_ms: int=None):
    """
    Saves candle data in `candles` to daily CSVs under dir_path.
    Groups by UTC day, preserves colors and prints, drops temp “day”.
    """
    df = pd.DataFrame(candles, columns=["timestamp","open","close","high","low","volume"])
    df["day"] = pd.to_datetime(df["timestamp"],unit="ms",utc=True).dt.date
    for day, grp in df.groupby("day"):
        fname = os.path.join(dir_path, f"{day}.csv")
        g = grp.drop(columns=["day"])
        # convert back to string
        for c in ["timestamp","open","close","high","low","volume"]:
            g[c] = g[c].astype(str)
        if os.path.exists(fname):
            updated = validate_and_update_candles(pd.read_csv(fname,dtype=str), g, fname)
            if updated>0:
                print(f"{UPDATE} {COLOR_ROWS}{updated}{Style.RESET_ALL} rows corrected in {COLOR_FILE}.../{symbol}/{timeframe}/{day}.csv{Style.RESET_ALL}")
        else:
            g.to_csv(fname, index=False, float_format="%.17g")
            start_ts, end_ts = int(g["timestamp"].astype(int).min()), int(g["timestamp"].astype(int).max())
            st = datetime.fromtimestamp(start_ts/1000,timezone.utc).strftime("%Y-%m-%d %H:%M")
            et = datetime.fromtimestamp(end_ts/1000,timezone.utc).strftime("%Y-%m-%d %H:%M")
            print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} {len(g)} candles [{COLOR_TIMESTAMPS}{st} - {et}{Style.RESET_ALL}] → {COLOR_FILE}.../{symbol}/{timeframe}/{day}.csv{Style.RESET_ALL}")


def find_missing_dates(dir_path: str, start: datetime.date, end: datetime.date):
    """
    Returns list of (start,end) tuples for missing date ranges in [start..end].
    """
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    have = {datetime.strptime(f[:-4],"%Y-%m-%d").date() for f in files}
    all_days = {start + timedelta(days=i) for i in range((end-start).days+1)}
    miss = sorted(all_days - have)
    if not miss: return []
    ranges, a = [], miss[0]
    prev = a
    for d in miss[1:]:
        if d == prev + timedelta(days=1):
            prev = d
            continue
        ranges.append((a, prev))
        a, prev = d, d
    ranges.append((a, prev))
    print(f"{WARNING} Missing {len(ranges)} date range(s). Rebuilding missing days.")
    for s,e in ranges:
        if s==e:
            print(f"  - {COLOR_TIMESTAMPS}{s}{Style.RESET_ALL}")
        else:
            print(f"  - {COLOR_TIMESTAMPS}{s} → {e}{Style.RESET_ALL}")
    return ranges


def _sync_range(dir_path, ticker, timeframe, start_d, end_d, gotstart_path, make_marker):
    """Core loop: fetch batches over [start_d..end_d] and fill CSVs."""
    ms_end = int(datetime.combine(end_d, datetime.max.time(), tzinfo=timezone.utc).timestamp()*1000)
    cur = int(datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc).timestamp()*1000)
    first_batch = True
    while cur <= ms_end:
        data = fetch_bitfinex_candles(ticker, timeframe, cur)
        if not data:
            # blank days for entire range
            d = start_d
            while d <= end_d:
                fp = os.path.join(dir_path, f"{d}.csv")
                if not os.path.exists(fp):
                    pd.DataFrame(columns=["timestamp","open","close","high","low","volume"])\
                      .to_csv(fp, index=False)
                    print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} Created empty CSV for missing day {COLOR_TIMESTAMPS}{d}{Style.RESET_ALL} → {COLOR_FILE}{d}.csv{Style.RESET_ALL}")
                d += timedelta(days=1)
            break
        first_ts = min(c[0] for c in data)
        last_ts  = max(c[0] for c in data)
        save_candles_to_csv(data, dir_path, ticker, timeframe, first_ts, last_ts)
        # create marker once
        if make_marker and first_batch:
            open(gotstart_path,"w").close()
            print(f"{INFO} Created {COLOR_FILE}.gotstart{Style.RESET_ALL}")
            first_batch = False
        # fill any days within this batch that might be empty
        sd = datetime.fromtimestamp(first_ts/1000,timezone.utc).date()
        ed = datetime.fromtimestamp(last_ts/1000,timezone.utc).date()
        d = sd
        while d <= ed:
            fp = os.path.join(dir_path, f"{d}.csv")
            if not os.path.exists(fp):
                pd.DataFrame(columns=["timestamp","open","close","high","low","volume"])\
                  .to_csv(fp, index=False)
                print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} Created empty CSV for missing day {COLOR_TIMESTAMPS}{d}{Style.RESET_ALL} → {COLOR_FILE}{d}.csv{Style.RESET_ALL}")
            d += timedelta(days=1)
        # decide whether to continue
        day_end_ts = int(datetime.combine(ed, datetime.max.time(), tzinfo=timezone.utc).timestamp()*1000)
        if len(data)<10000 or last_ts>=day_end_ts:
            print(f"{INFO} Completed fetching for range {sd}→{ed}")
            break
        cur = last_ts + 1


def synchronize_candle_data(exchange: str,
                           ticker: str,
                           timeframe: str,
                           end_date_str: str=None,
                           verbose: bool=False) -> bool:
    """Main orchestration: full or incremental sync."""
    if not verbose:
        es = f" → {end_date_str}" if end_date_str else ""
        print(f"{INFO} Syncing {COLOR_VAR}{exchange}/{ticker}/{timeframe}{Style.RESET_ALL}{es}")
    else:
        print(f"\n{INFO} Running synchronization with parameters:\n"
              f"  {COLOR_VAR}--exchange{Style.RESET_ALL} {exchange}\n"
              f"  {COLOR_VAR}--ticker{Style.RESET_ALL}   {ticker}\n"
              f"  {COLOR_VAR}--timeframe{Style.RESET_ALL} {timeframe}")
        if end_date_str:
            print(f"  {COLOR_VAR}--end{Style.RESET_ALL}       {end_date_str}")
        print()

    dir_path      = ensure_directory(exchange, ticker, timeframe)
    gotstart_path = os.path.join(dir_path, ".gotstart")

    # parse end date
    if end_date_str:
        fmt = "%Y-%m-%d %H:%M" if " " in end_date_str else "%Y-%m-%d"
        end_date = datetime.strptime(end_date_str, fmt).date()
    else:
        end_date = datetime.now(timezone.utc).date()

    # FULL SYNC
    if not os.path.exists(gotstart_path):
        print(f"{WARNING} No '.gotstart' file found. Starting full historical sync...")
        _sync_range(dir_path, ticker, timeframe, datetime(1970,1,1).date(), end_date, gotstart_path, True)
        return True

    # INCREMENTAL SYNC
    print(f"{INFO} '.gotstart' exists. Verifying data continuity...")
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    if not files:
        print(f"{WARNING} Marker present but no CSVs. Re-running full sync.")
        os.remove(gotstart_path)
        return synchronize_candle_data(exchange,ticker,timeframe,end_date_str,verbose)

    start_date = datetime.strptime(files[0][:-4], "%Y-%m-%d").date()
    last_date  = end_date

    # Always resync today's file if no end date is specified
    today = datetime.now(timezone.utc).date()
    if not end_date_str:
        print(f"{INFO} No end date specified - always resyncing today's file ({today})...")
        _sync_range(dir_path, ticker, timeframe, today, today, gotstart_path, False)
        last_ts = get_last_file_timestamp(dir_path)
        if last_ts:
            lt = datetime.fromtimestamp(last_ts/1000, timezone.utc)
            print(f"{INFO} Latest timestamp: {COLOR_TIMESTAMPS}{lt:%Y-%m-%d %H:%M:%S UTC}{Style.RESET_ALL}")

    gaps = find_missing_dates(dir_path, start_date, last_date)
    if not gaps:
        print(f"{SUCCESS} All files exist for {start_date} → {last_date}")
        return True

    for s,e in gaps:
        print(f"{WARNING} Filling gap {s} → {e}")
        _sync_range(dir_path, ticker, timeframe, s, e, gotstart_path, False)
    return True


def parse_args():
    """
    Parses command-line arguments with colorized help.
    """
    parser = argparse.ArgumentParser(
        description=f"""
{INFO} Fetch missing historical candle data from an exchange. {Style.RESET_ALL}
  {COLOR_VAR}--exchange{Style.RESET_ALL}   {COLOR_TYPE}(str){Style.RESET_ALL} {COLOR_REQ} Exchange name
  {COLOR_VAR}--ticker{Style.RESET_ALL}     {COLOR_TYPE}(str){Style.RESET_ALL} {COLOR_REQ} Trading pair
  {COLOR_VAR}--timeframe{Style.RESET_ALL}  {COLOR_TYPE}(str){Style.RESET_ALL} {COLOR_REQ} Timeframe
  {COLOR_VAR}--end{Style.RESET_ALL}        {COLOR_TYPE}(str){Style.RESET_ALL} End date (optional)
{INFO} Example:
  python candles_sync.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1h --end "2024-02-10"
""", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--exchange",  required=True, help="Exchange name (e.g., BITFINEX)")
    parser.add_argument("--ticker",    required=True, help="Trading pair (e.g., tBTCUSD)")
    parser.add_argument("--timeframe", required=True, help="Timeframe (e.g., 1m, 1h, 1D)")
    parser.add_argument("--end",       help="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    if len(sys.argv)==1:
        print(f"\n{ERROR} No arguments provided! Please specify the required parameters.\n")
        parser.print_help(); sys.exit(1)
    return parser.parse_args()


def main():
    args = parse_args()
    ok = synchronize_candle_data(
        exchange=args.exchange,
        ticker=args.ticker,
        timeframe=args.timeframe,
        end_date_str=args.end,
        verbose=True
    )
    if ok:
        print(f"\n{SUCCESS} Synchronization completed successfully.\n")
    else:
        print(f"\n{ERROR} Synchronization failed.\n")


if __name__=="__main__":
    main()
