#!/usr/bin/env python3
"""
candles_sync.py

This script synchronizes historical candle data for a given exchange/ticker,
for 3 timeframes automatically:
    - 1m -> stored daily (YYYY-MM-DD.csv)
    - 1h -> stored monthly (YYYY-MM.csv)
    - 1D -> stored yearly (YYYY.csv)

Features:
- First‐time sync (no ".gotstart" file): queries from the epoch (0) to build full history.
- Incremental sync (".gotstart" exists): finds missing partitions and backfills.
- Robust backoff on network errors and rate limits.
- Preserves all logging output, ANSI colors, function names, and CLI usage.

Usage:
    python candles_sync.py --exchange BITFINEX --ticker tBTCUSD [--end YYYY-MM-DD]
    (It will fetch/sync 1m, 1h, and 1D partitions automatically.)
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
        def __getattr__(self, item): 
            return ''
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
    """Ensures that the data directory for a given timeframe exists."""
    dir_path = os.path.join(ROOT_PATH, exchange, "candles", ticker, timeframe)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        print(f"{INFO} Created directory: {COLOR_DIR}{dir_path}{Style.RESET_ALL}")
    else:
        print(f"{INFO} Directory already exists: {COLOR_DIR}{dir_path}{Style.RESET_ALL}")
    return dir_path


def get_last_file_timestamp(dir_path: str):
    """
    Retrieves the most recent CSV file's max timestamp from
    the existing partition files (daily, monthly, yearly).
    """
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    if not files:
        return None
    latest = files[-1]
    df = pd.read_csv(os.path.join(dir_path, latest), dtype=str)
    if "timestamp" not in df.columns or df.empty:
        print(f"{WARNING} No valid timestamp in {latest}")
        return None
    df["timestamp"] = df["timestamp"].astype("int64")
    ts = df["timestamp"].max()
    human = datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"{INFO} Last recorded timestamp from {COLOR_FILE}{latest}{Style.RESET_ALL}: "
          f"{COLOR_TIMESTAMPS}{ts}{Style.RESET_ALL} ({human})")
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
            time.sleep(delay)
            delay = min(max_delay, delay * 2)
            continue

        if resp.status_code == 200:
            data = resp.json()
            if not data:
                print(f"{WARNING} No candles returned from API.")
                return None
            # reset backoff
            delay = 30
            ts = [c[0] for c in data]
            first = datetime.fromtimestamp(min(ts) / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            last  = datetime.fromtimestamp(max(ts) / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"{SUCCESS} Received {len(data)} candles "
                  f"({COLOR_TIMESTAMPS}{first}{Style.RESET_ALL} → {COLOR_TIMESTAMPS}{last}{Style.RESET_ALL})")
            return data

        if resp.status_code == 429:
            print(f"{WARNING} Rate limit hit (429). Retrying in {delay}s...")
            time.sleep(delay)
            delay = min(max_delay, delay * 2)
            continue

        print(f"{ERROR} HTTP {resp.status_code}: {resp.text}")
        return None


def validate_and_update_candles(df_existing: pd.DataFrame, df_new: pd.DataFrame, csv_path: str) -> int:
    """Merges existing vs new, counts changed rows, writes CSV."""
    df_old = pd.read_csv(csv_path, dtype=str)
    df_old["timestamp"] = df_old["timestamp"].astype("int64")
    df_new["timestamp"] = df_new["timestamp"].astype("int64")
    cols = ["open","close","high","low","volume"]
    for col in cols:
        if col in df_old:
            df_old[col] = df_old[col].apply(lambda x: decimal.Decimal(x or "0"))
        df_new[col] = df_new[col].apply(lambda x: decimal.Decimal(str(x)))

    # Remove any leftover partition columns
    for leftover in ["day","partition"]:
        df_old.drop(columns=[leftover], errors="ignore", inplace=True)
        df_new.drop(columns=[leftover], errors="ignore", inplace=True)

    merged = pd.merge(df_old, df_new, on="timestamp", how="outer",
                      suffixes=("_old","_new"), indicator=True)
    updated = 0
    for _, r in merged.iterrows():
        if r["_merge"] == "both" and any(r[f"{c}_old"] != r[f"{c}_new"] for c in cols):
            updated += 1

    for c in cols:
        o, n = f"{c}_old", f"{c}_new"
        if o in merged and n in merged:
            merged[c] = merged[n].combine_first(merged[o]).astype(str)
            merged.drop(columns=[o, n], inplace=True)

    merged.drop(columns=["_merge"], inplace=True)
    merged.to_csv(csv_path, index=False)
    return updated


def partition_from_timestamp(timeframe: str, ts: int) -> str:
    """
    Returns the partition string (file basename) given a timestamp & timeframe:
      - 1m  => daily partition (YYYY-MM-DD)
      - 1h  => monthly partition (YYYY-MM)
      - 1D  => yearly partition (YYYY)
    """
    dt = datetime.fromtimestamp(ts / 1000, timezone.utc)
    if timeframe == "1m":
        return dt.strftime("%Y-%m-%d")
    elif timeframe == "1h":
        return dt.strftime("%Y-%m")
    else:
        # covers '1D'
        return dt.strftime("%Y")


def generate_partitions(timeframe: str, start_date: datetime, end_date: datetime):
    """
    Returns a sorted list of partition strings from start_date..end_date inclusive,
    depending on the timeframe partition style.
    """
    partitions = []
    cur = start_date

    # For daily intervals (1m)
    if timeframe == "1m":
        while cur <= end_date:
            partitions.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        return partitions

    # For monthly intervals (1h)
    elif timeframe == "1h":
        cur = cur.replace(day=1)
        while cur <= end_date:
            partitions.append(cur.strftime("%Y-%m"))
            year = cur.year + (cur.month // 12)
            month = (cur.month % 12) + 1
            cur = cur.replace(year=year, month=month, day=1)
        return partitions

    # For yearly intervals (1D)
    else:
        cur = cur.replace(month=1, day=1)
        while cur <= end_date:
            partitions.append(cur.strftime("%Y"))
            cur = cur.replace(year=cur.year + 1, month=1, day=1)
        return partitions


def save_candles_to_csv(candles, dir_path: str, symbol: str, timeframe: str,
                        range_start_ms: int=None, range_end_ms: int=None):
    """
    Saves candle data in `candles` to partitioned CSVs under dir_path.

    For timeframe:
      1m => group by YYYY-MM-DD
      1h => group by YYYY-MM
      1D => group by YYYY
    """
    df = pd.DataFrame(candles, columns=["timestamp","open","close","high","low","volume"])
    df["partition"] = df["timestamp"].apply(lambda x: partition_from_timestamp(timeframe, x))

    for part_val, grp in df.groupby("partition"):
        fname = os.path.join(dir_path, f"{part_val}.csv")
        g = grp.drop(columns=["partition"])
        for c in ["timestamp","open","close","high","low","volume"]:
            g[c] = g[c].astype(str)

        if os.path.exists(fname):
            updated = validate_and_update_candles(pd.read_csv(fname,dtype=str), g, fname)
            if updated > 0:
                print(f"{UPDATE} {COLOR_ROWS}{updated}{Style.RESET_ALL} rows corrected in "
                      f"{COLOR_FILE}.../{symbol}/{timeframe}/{part_val}.csv{Style.RESET_ALL}")
        else:
            g.to_csv(fname, index=False, float_format="%.17g")
            start_ts = int(g["timestamp"].astype(int).min())
            end_ts = int(g["timestamp"].astype(int).max())
            st = datetime.fromtimestamp(start_ts/1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
            et = datetime.fromtimestamp(end_ts/1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
            print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} {len(g)} candles "
                  f"[{COLOR_TIMESTAMPS}{st} - {et}{Style.RESET_ALL}] → "
                  f"{COLOR_FILE}.../{symbol}/{timeframe}/{part_val}.csv{Style.RESET_ALL}")


def find_missing_partitions(dir_path: str, timeframe: str,
                            start_date: datetime, end_date: datetime):
    """
    Returns a list of missing partition strings in [start_date..end_date]
    for the given timeframe.
    """
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    have = set(f[:-4] for f in files)

    all_needed = generate_partitions(timeframe, start_date, end_date)
    all_needed_set = set(all_needed)
    missing = sorted(all_needed_set - have)
    if not missing:
        return []

    print(f"{WARNING} Missing {len(missing)} partition(s). Rebuilding missing partitions:")
    for m in missing:
        print(f"  - {COLOR_TIMESTAMPS}{m}{Style.RESET_ALL}")
    return missing


def _sync_range(dir_path, ticker, timeframe,
                start_dt: datetime, end_dt: datetime,
                gotstart_path, make_marker):
    """
    Core loop: fetch batches over [start_dt..end_dt] and fill CSVs
    for the given timeframe partition style.
    """
    ms_end = int(end_dt.replace(hour=23, minute=59, second=59).timestamp() * 1000)
    cur = int(start_dt.replace(hour=0, minute=0, second=0).timestamp() * 1000)
    first_batch = True

    while cur <= ms_end:
        data = fetch_bitfinex_candles(ticker, timeframe, cur)
        if not data:
            # Create empty CSV for each missing partition in range
            missing_partitions = generate_partitions(timeframe, start_dt, end_dt)
            for mp in missing_partitions:
                fp = os.path.join(dir_path, f"{mp}.csv")
                if not os.path.exists(fp):
                    pd.DataFrame(columns=["timestamp","open","close","high","low","volume"]) \
                      .to_csv(fp, index=False)
                    print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} Created empty CSV for missing partition "
                          f"{COLOR_TIMESTAMPS}{mp}{Style.RESET_ALL} → {COLOR_FILE}{mp}.csv{Style.RESET_ALL}")
            break

        first_ts = min(c[0] for c in data)
        last_ts  = max(c[0] for c in data)

        save_candles_to_csv(data, dir_path, ticker, timeframe, first_ts, last_ts)

        # create marker once
        if make_marker and first_batch:
            open(gotstart_path,"w").close()
            print(f"{INFO} Created {COLOR_FILE}.gotstart{Style.RESET_ALL}")
            first_batch = False

        # Fill any empty partition files within this batch
        earliest_dt = datetime.fromtimestamp(first_ts/1000, timezone.utc)
        latest_dt   = datetime.fromtimestamp(last_ts/1000, timezone.utc)
        missing_partitions = generate_partitions(timeframe, earliest_dt, latest_dt)
        for mp in missing_partitions:
            fp = os.path.join(dir_path, f"{mp}.csv")
            if not os.path.exists(fp):
                pd.DataFrame(columns=["timestamp","open","close","high","low","volume"]) \
                  .to_csv(fp, index=False)
                print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} Created empty CSV for missing partition "
                      f"{COLOR_TIMESTAMPS}{mp}{Style.RESET_ALL} → {COLOR_FILE}{mp}.csv{Style.RESET_ALL}")

        # decide whether to continue
        if len(data) < 10000:
            print(f"{INFO} Completed fetching for timeframe chunk up to {latest_dt.date()}")
            break

        # if we got 10k candles, step beyond last_ts and continue
        cur = last_ts + 1


def synchronize_candle_data(exchange: str,
                            ticker: str,
                            timeframe: str,
                            end_date_str: str=None,
                            verbose: bool=False) -> bool:
    """
    Main orchestration: full or incremental sync for a single timeframe
    (daily, monthly, or yearly partition).
    """
    if not verbose:
        es = f" → {end_date_str}" if end_date_str else ""
        print(f"{INFO} Syncing {COLOR_VAR}{exchange}/{ticker}/{timeframe}{Style.RESET_ALL}{es}")
    else:
        print(f"\n{INFO} Running synchronization with parameters:\n"
              f"  {COLOR_VAR}--exchange{Style.RESET_ALL} {exchange}\n"
              f"  {COLOR_VAR}--ticker{Style.RESET_ALL}   {ticker}\n"
              f"  [Timeframe internally set to '{timeframe}']" )
        if end_date_str:
            print(f"  {COLOR_VAR}--end{Style.RESET_ALL}       {end_date_str}")
        print()

    dir_path      = ensure_directory(exchange, ticker, timeframe)
    gotstart_path = os.path.join(dir_path, ".gotstart")

    # parse end date
    if end_date_str:
        fmt = "%Y-%m-%d %H:%M" if " " in end_date_str else "%Y-%m-%d"
        end_date = datetime.strptime(end_date_str, fmt).replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc)

    # FULL SYNC
    if not os.path.exists(gotstart_path):
        print(f"{WARNING} No '.gotstart' file found. Starting full historical sync for {timeframe}...")
        _sync_range(dir_path, ticker, timeframe,
                    datetime(1970,1,1, tzinfo=timezone.utc), end_date,
                    gotstart_path, True)
        return True

    # INCREMENTAL SYNC
    print(f"{INFO} '.gotstart' exists for {timeframe}. Verifying data continuity...")
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    if not files:
        print(f"{WARNING} Marker present but no CSVs. Re-running full sync for {timeframe}.")
        os.remove(gotstart_path)
        return synchronize_candle_data(exchange, ticker, timeframe, end_date_str, verbose)

    # find earliest known partition
    earliest_str = files[0][:-4]
    try:
        if timeframe == "1m":
            earliest_dt = datetime.strptime(earliest_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        elif timeframe == "1h":
            earliest_dt = datetime.strptime(earliest_str, "%Y-%m").replace(day=1, tzinfo=timezone.utc)
        else:
            earliest_dt = datetime.strptime(earliest_str, "%Y").replace(month=1, day=1, tzinfo=timezone.utc)
    except:
        earliest_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)

    now_utc_date = datetime.now(timezone.utc)
    if not end_date_str:
        print(f"{INFO} No end date specified - always resyncing current partition for {timeframe}...")
        _sync_range(dir_path, ticker, timeframe, now_utc_date, now_utc_date, gotstart_path, False)
        last_ts = get_last_file_timestamp(dir_path)
        if last_ts:
            lt = datetime.fromtimestamp(last_ts/1000, timezone.utc)
            print(f"{INFO} Latest timestamp for {timeframe}: "
                  f"{COLOR_TIMESTAMPS}{lt:%Y-%m-%d %H:%M:%S UTC}{Style.RESET_ALL}")

    missing = find_missing_partitions(dir_path, timeframe, earliest_dt, end_date)
    if not missing:
        print(f"{SUCCESS} All files exist for {timeframe} from {earliest_dt.date()} → {end_date.date()}")
        return True

    # fill the gaps
    for mp in missing:
        print(f"{WARNING} Filling gap: {mp} for {timeframe}")
        if timeframe == "1m":
            s_dt = datetime.strptime(mp, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            e_dt = s_dt
        elif timeframe == "1h":
            s_dt = datetime.strptime(mp, "%Y-%m").replace(day=1, tzinfo=timezone.utc)
            year = s_dt.year + (s_dt.month // 12)
            month = (s_dt.month % 12) + 1
            nm = s_dt.replace(year=year, month=month, day=1)
            e_dt = (nm - timedelta(days=1)).replace(hour=23, minute=59, second=59)
        else:  # 1D
            s_dt = datetime.strptime(mp, "%Y").replace(month=1, day=1, tzinfo=timezone.utc)
            e_dt = s_dt.replace(month=12, day=31, hour=23, minute=59, second=59)
        if e_dt > end_date:
            e_dt = end_date

        _sync_range(dir_path, ticker, timeframe, s_dt, e_dt, gotstart_path, False)
    return True


# Note: parse_args has been moved to example.py


# Note: main function has been moved to example.py


if __name__=="__main__":
    # Main functionality has been moved to example.py
    print("Please run example.py instead.")
    print("Example: python example.py --exchange BITFINEX --ticker tBTCUSD")
