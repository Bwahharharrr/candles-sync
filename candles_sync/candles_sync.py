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
- Incremental sync (".gotstart" exists):
    1) Finds missing partitions grouped by consecutive runs (e.g. daily runs).
    2) For each run, performs a chunked fetch from:
         (last-known-candle + 1) or earliest partition of that run,
       whichever is later, **up to** that run's last partition (end of that day/month/year).
    3) Writes data or creates empty CSV files for each missing partition in the run.
    4) Jumps to the next missing run without re-fetching unneeded ranges.
    5) After all runs, re-checks the last partition to ensure it’s updated to `--end`.

Usage:
    python candles_sync.py --exchange BITFINEX --ticker tBTCUSD [--end YYYY-MM-DD]
    or
    python candles_sync.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1m
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

try:
    import colorama
    from colorama import Fore, Style
    colorama.init(autoreset=True)
except ImportError:
    class NoColor:
        def __getattr__(self, item):
            return ''
    Fore = Style = NoColor()

INFO    = Fore.GREEN   + "[INFO]"    + Style.RESET_ALL
WARNING = Fore.YELLOW  + "[WARNING]" + Style.RESET_ALL
ERROR   = Fore.RED     + "[ERROR]"   + Style.RESET_ALL
SUCCESS = Fore.GREEN   + "[SUCCESS]" + Style.RESET_ALL
UPDATE  = Fore.MAGENTA + "[UPDATE]"  + Style.RESET_ALL

COLOR_DIR        = Fore.CYAN
COLOR_FILE       = Fore.YELLOW
COLOR_TIMESTAMPS = Fore.MAGENTA
COLOR_ROWS       = Fore.RED
COLOR_NEW        = Fore.WHITE
COLOR_VAR        = Fore.CYAN
COLOR_TYPE       = Fore.YELLOW
COLOR_DESC       = Fore.MAGENTA
COLOR_REQ        = Fore.RED + "[REQUIRED]" + Style.RESET_ALL

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
    the existing partition files (daily, monthly, or yearly).
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


def get_first_file_timestamp(dir_path: str, filename: str) -> int:
    """Returns the earliest (min) timestamp in a single CSV file."""
    path = os.path.join(dir_path, filename)
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, dtype=str)
    if "timestamp" not in df.columns or df.empty:
        return None
    df["timestamp"] = df["timestamp"].astype("int64")
    return df["timestamp"].min()


def fetch_bitfinex_candles(symbol: str, timeframe: str, start: int, end: int = None, limit: int = 10000):
    """
    Fetches up to `limit` candles from Bitfinex, ascending (sort=1),
    starting at `start` up to `end` if given.
    Retries on network/429 with exponential backoff.
    Returns list of candle arrays or empty list if no data.
    """
    url = BITFINEX_API_URL.format(timeframe, symbol)
    params = {"limit": limit, "sort": 1, "start": start}
    if end is not None:
        params["end"] = end

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
                return []
            # Reset backoff
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
        return []


def validate_and_update_candles(df_existing: pd.DataFrame, df_new: pd.DataFrame, csv_path: str) -> int:
    """
    Merges old + new data on timestamp, preserving and updating
    rows in the existing CSV. Returns how many rows got updated.
    """
    df_old = pd.read_csv(csv_path, dtype=str)
    df_old["timestamp"] = df_old["timestamp"].astype("int64")
    df_new["timestamp"] = df_new["timestamp"].astype("int64")
    cols = ["open", "close", "high", "low", "volume"]

    for col in cols:
        if col in df_old:
            df_old[col] = df_old[col].apply(lambda x: decimal.Decimal(x or "0"))
        df_new[col] = df_new[col].apply(lambda x: decimal.Decimal(str(x)))

    # Remove leftover columns
    for leftover in ["day", "partition"]:
        df_old.drop(columns=[leftover], errors="ignore", inplace=True)
        df_new.drop(columns=[leftover], errors="ignore", inplace=True)

    merged = pd.merge(
        df_old, df_new, on="timestamp", how="outer",
        suffixes=("_old", "_new"), indicator=True
    )

    updated = 0
    for _, r in merged.iterrows():
        if r["_merge"] == "both" and any(r[f"{c}_old"] != r[f"{c}_new"] for c in cols):
            updated += 1

    # Combine columns
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
    Given timeframe & timestamp, returns the partition name:
     - "1m" => daily "YYYY-MM-DD"
     - "1h" => monthly "YYYY-MM"
     - "1D" => yearly "YYYY"
    """
    dt = datetime.fromtimestamp(ts / 1000, timezone.utc)
    if timeframe == "1m":
        return dt.strftime("%Y-%m-%d")
    elif timeframe == "1h":
        return dt.strftime("%Y-%m")
    else:
        return dt.strftime("%Y")


def partition_start_end_dates(tf: str, part_str: str):
    """
    For a partition name, returns (startDate, endDate) in UTC.
      - 1m => day start..day end
      - 1h => month start..month end
      - 1D => year start..year end
    """
    if tf == "1m":
        dt_start = datetime.strptime(part_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        dt_end   = dt_start.replace(hour=23, minute=59, second=59)
    elif tf == "1h":
        dt_start = datetime.strptime(part_str, "%Y-%m").replace(day=1, tzinfo=timezone.utc)
        # Next month - 1 second
        year = dt_start.year + (dt_start.month // 12)
        month = (dt_start.month % 12) + 1
        dt_end_candidate = dt_start.replace(year=year, month=month, day=1, hour=0, minute=0, second=0)
        dt_end = dt_end_candidate - timedelta(seconds=1)
    else:  # '1D'
        dt_start = datetime.strptime(part_str, "%Y").replace(month=1, day=1, tzinfo=timezone.utc)
        dt_end_candidate = dt_start.replace(year=dt_start.year+1, month=1, day=1, hour=0, minute=0, second=0)
        dt_end = dt_end_candidate - timedelta(seconds=1)
    return dt_start, dt_end


def generate_partitions(timeframe: str, start_date: datetime, end_date: datetime):
    """
    Given timeframe, returns a list of partition names covering start_date..end_date.
    """
    partitions = []
    cur = start_date

    if timeframe == "1m":
        while cur <= end_date:
            partitions.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        return partitions

    elif timeframe == "1h":
        cur = cur.replace(day=1, hour=0, minute=0, second=0)
        while cur <= end_date:
            partitions.append(cur.strftime("%Y-%m"))
            year = cur.year + (cur.month // 12)
            month = (cur.month % 12) + 1
            cur = cur.replace(year=year, month=month, day=1, hour=0, minute=0, second=0)
        return partitions

    else:  # '1D'
        cur = cur.replace(month=1, day=1, hour=0, minute=0, second=0)
        while cur <= end_date:
            partitions.append(cur.strftime("%Y"))
            cur = cur.replace(year=cur.year + 1, month=1, day=1, hour=0, minute=0, second=0)
        return partitions


def save_candles_to_csv(candles, dir_path: str, symbol: str, timeframe: str):
    """
    Given raw candle data, partition it by day/month/year and save
    into CSV files. If a file already exists, merges data.
    """
    if not candles:
        return

    df = pd.DataFrame(candles, columns=["timestamp", "open", "close", "high", "low", "volume"])
    df["partition"] = df["timestamp"].apply(lambda x: partition_from_timestamp(timeframe, x))

    for part_val, grp in df.groupby("partition"):
        fname = os.path.join(dir_path, f"{part_val}.csv")
        g = grp.drop(columns=["partition"])
        for c in ["timestamp", "open", "close", "high", "low", "volume"]:
            g[c] = g[c].astype(str)

        if os.path.exists(fname):
            updated = validate_and_update_candles(pd.read_csv(fname, dtype=str), g, fname)
            if updated > 0:
                print(f"{UPDATE} {COLOR_ROWS}{updated}{Style.RESET_ALL} rows corrected in "
                      f"{COLOR_FILE}.../{symbol}/{timeframe}/{part_val}.csv{Style.RESET_ALL}")
        else:
            g.to_csv(fname, index=False, float_format="%.17g")
            start_ts = int(g["timestamp"].astype(int).min())
            end_ts = int(g["timestamp"].astype(int).max())
            st = datetime.fromtimestamp(start_ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
            et = datetime.fromtimestamp(end_ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
            print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} {len(g)} candles "
                  f"[{COLOR_TIMESTAMPS}{st} - {et}{Style.RESET_ALL}] → "
                  f"{COLOR_FILE}.../{symbol}/{timeframe}/{part_val}.csv{Style.RESET_ALL}")


def parse_partition_date(tf: str, part_str: str) -> datetime:
    """
    Inverse of partition_from_timestamp: parse a partition name into a datetime.
    """
    if tf == "1m":
        return datetime.strptime(part_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    elif tf == "1h":
        return datetime.strptime(part_str, "%Y-%m").replace(day=1, tzinfo=timezone.utc)
    else:
        return datetime.strptime(part_str, "%Y").replace(month=1, day=1, tzinfo=timezone.utc)


def _is_next_consecutive(tf: str, current_str: str, next_str: str) -> bool:
    """
    Determines if next_str is the immediate "consecutive" partition after current_str
    (e.g. for daily, it's exactly 1 day later).
    """
    current_dt = parse_partition_date(tf, current_str)
    next_dt    = parse_partition_date(tf, next_str)

    if tf == "1m":
        return (next_dt - current_dt) == timedelta(days=1)
    elif tf == "1h":
        # For monthly => current_dt plus exactly 1 month
        year = current_dt.year + (current_dt.month // 12)
        month = (current_dt.month % 12) + 1
        candidate = current_dt.replace(year=year, month=month, day=1)
        return candidate == next_dt
    else:
        # '1D' => yearly => next_dt.year == current_dt.year + 1, same month/day=1
        candidate = current_dt.replace(year=current_dt.year + 1, month=1, day=1)
        return candidate == next_dt


def _group_consecutive_partitions(tf: str, missing: list[str]) -> list[list[str]]:
    """
    Groups a sorted list of missing partition strings into consecutive runs.
    """
    if not missing:
        return []

    groups = []
    current_group = [missing[0]]

    for i in range(len(missing) - 1):
        this_part = missing[i]
        next_part = missing[i+1]
        if _is_next_consecutive(tf, this_part, next_part):
            current_group.append(next_part)
        else:
            groups.append(current_group)
            current_group = [next_part]

    if current_group:
        groups.append(current_group)

    return groups


def _create_empty_partitions(dir_path: str, timeframe: str,
                             symbol: str, from_ts: int, to_ts: int):
    """
    For [from_ts..to_ts], create empty CSV files for daily/monthly/yearly partitions
    if they don't already exist.
    """
    if to_ts < from_ts:
        return
    start_dt = datetime.fromtimestamp(from_ts / 1000, timezone.utc)
    end_dt   = datetime.fromtimestamp(to_ts   / 1000, timezone.utc)
    parts = generate_partitions(timeframe, start_dt, end_dt)
    for p in parts:
        fname = os.path.join(dir_path, p + ".csv")
        if not os.path.exists(fname):
            pd.DataFrame(columns=["timestamp", "open", "close", "high", "low", "volume"]) \
                .to_csv(fname, index=False)
            print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} Created empty CSV for missing partition "
                  f"{COLOR_TIMESTAMPS}{p}{Style.RESET_ALL} → {COLOR_FILE}{p}.csv{Style.RESET_ALL}")


def _bulk_fill_missing(dir_path: str, ticker: str, timeframe: str,
                       start_ms: int, end_ms: int or None):
    """
    Fetch candles in chunk(s) from start_ms..end_ms. For each chunk:
      - fill empty CSVs if there's a gap
      - save the chunk
      - move 'cur' to last_candle+1
      - break if no more data or chunk < 10000
    """
    cur = start_ms
    while True:
        if end_ms is not None and cur > end_ms:
            break

        data = fetch_bitfinex_candles(ticker, timeframe, cur, end=end_ms, limit=10000)
        if not data:
            if end_ms is not None and cur <= end_ms:
                _create_empty_partitions(dir_path, timeframe, ticker, cur, end_ms)
            break

        earliest_in_chunk = min(c[0] for c in data)
        if earliest_in_chunk > cur:
            _create_empty_partitions(dir_path, timeframe, ticker, cur, earliest_in_chunk - 1)

        save_candles_to_csv(data, dir_path, ticker, timeframe)

        last_ts = max(c[0] for c in data)
        if last_ts <= cur:
            print(f"{ERROR} Failsafe triggered: last_ts={last_ts} <= cur={cur}. Breaking loop.")
            break

        cur = last_ts + 1
        if len(data) < 10000:
            if end_ms is not None and cur <= end_ms:
                _create_empty_partitions(dir_path, timeframe, ticker, cur, end_ms)
            break


def _fill_missing_partitions_by_group(
    dir_path: str,
    ticker: str,
    timeframe: str,
    groups: list[list[str]],
    last_ts_so_far: int or None,
    fill_end_ms: int or None
):
    """
    For each consecutive-run of missing partitions, do a chunk-based fill
    from (last_ts_so_far+1 or earliest_in_run) up to that run's last partition.
    Then stop and move on to the next group.
    """
    for idx, group in enumerate(groups, start=1):
        first_p = group[0]
        last_p  = group[-1]

        # Show output in the style you requested
        print(
            f"{WARNING} Missing partition Group {idx} "
            f"(last candle on record → next candle on record):"
        )
        for mp in group:
            print(f"    - {COLOR_TIMESTAMPS}{mp}{Style.RESET_ALL}")
        print()  # spacing

        # Convert earliest & last partition in this group to timestamps
        earliest_dt, _ = partition_start_end_dates(timeframe, first_p)
        _, last_dt_end = partition_start_end_dates(timeframe, last_p)

        earliest_ms = int(earliest_dt.timestamp() * 1000)
        last_ms     = int(last_dt_end.timestamp()  * 1000)

        if last_ts_so_far is None:
            # no data => start from earliest
            start_ms = earliest_ms
        else:
            # start from whichever is later (last candle+1 or earliest partition)
            candidate = last_ts_so_far + 1
            start_ms = min(candidate, earliest_ms)

        # but never fetch beyond the group-end
        group_end_ms = last_ms
        if fill_end_ms is not None:
            group_end_ms = min(group_end_ms, fill_end_ms)

        # Now do the chunk-based fill just for that range
        print(
            f"{INFO} Running fill for Group {idx} from {start_ms} ms → {group_end_ms} ms "
            f"(covering partitions {first_p} .. {last_p})"
        )
        _bulk_fill_missing(dir_path, ticker, timeframe, start_ms, group_end_ms)

        # update last_ts_so_far from newly created/updated files
        # We don't need to do multiple chunked reads; we can just re-scan CSV if needed.
        files_in_dir = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
        for fcsv in files_in_dir:
            path = os.path.join(dir_path, fcsv)
            df_check = pd.read_csv(path, dtype=str)
            if not df_check.empty and "timestamp" in df_check.columns:
                df_check["timestamp"] = df_check["timestamp"].astype("int64")
                mx = df_check["timestamp"].max()
                if (last_ts_so_far is None) or (mx > last_ts_so_far):
                    last_ts_so_far = mx

    return last_ts_so_far


def synchronize_candle_data(
    exchange: str,
    ticker: str,
    timeframe: str,
    end_date_str: str=None,
    verbose: bool=False
) -> bool:
    """
    Main function to sync historical candles from Bitfinex for a single timeframe.

    1) If no .gotstart => full sync from epoch -> end_date (or indefinite if none).
    2) If .gotstart => find missing partitions, group them by consecutive runs, and fill
       each run separately (avoiding re-fetching the entire range over and over).
    3) Re-check the last partition for completeness up to end_date.
    """
    if not verbose:
        es = f" → {end_date_str}" if end_date_str else ""
        print(f"{INFO} Syncing {COLOR_VAR}{exchange}/{ticker}/{timeframe}{Style.RESET_ALL}{es}")
    else:
        print(f"\n{INFO} Running synchronization with parameters:\n"
              f"  {COLOR_VAR}--exchange{Style.RESET_ALL} {exchange}\n"
              f"  {COLOR_VAR}--ticker{Style.RESET_ALL}   {ticker}\n"
              f"  [Timeframe internally set to '{timeframe}']")
        if end_date_str:
            print(f"  {COLOR_VAR}--end{Style.RESET_ALL}       {end_date_str}")
        print()

    dir_path      = ensure_directory(exchange, ticker, timeframe)
    gotstart_path = os.path.join(dir_path, ".gotstart")

    # Parse end_date (if any)
    if end_date_str:
        fmt = "%Y-%m-%d %H:%M" if " " in end_date_str else "%Y-%m-%d"
        end_date = datetime.strptime(end_date_str, fmt).replace(tzinfo=timezone.utc)
    else:
        end_date = None

    # If user didn't specify end_date, default to now
    fill_end_date = end_date if end_date else datetime.now(timezone.utc)
    fill_end_ms   = int(fill_end_date.timestamp() * 1000)

    # FULL SYNC if marker doesn't exist
    if not os.path.exists(gotstart_path):
        print(f"{WARNING} No '.gotstart' file found. Starting full historical sync for {timeframe}...")
        start_ms = int(datetime(1970,1,1, tzinfo=timezone.utc).timestamp() * 1000)
        end_ms   = fill_end_ms

        _bulk_fill_missing(dir_path, ticker, timeframe, start_ms, end_ms)
        # Mark that we got started
        open(gotstart_path, "w").close()
        print(f"{INFO} Created {COLOR_FILE}.gotstart{Style.RESET_ALL}")
        return True

    # Otherwise do incremental
    print(f"{INFO} '.gotstart' exists for {timeframe}. Checking missing partitions...")

    # If no CSV files but .gotstart is present, treat like full sync
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    if not files:
        print(f"{WARNING} Marker present but no CSVs. Re-running full sync for {timeframe}.")
        os.remove(gotstart_path)
        return synchronize_candle_data(exchange, ticker, timeframe, end_date_str, verbose)

    # Find earliest existing partition
    earliest_file = files[0][:-4]
    earliest_dt = parse_partition_date(timeframe, earliest_file)
    # Find all missing partitions
    missing = sorted(set(generate_partitions(timeframe, earliest_dt, fill_end_date)) -
                     set(f[:-4] for f in files if f.endswith(".csv")))

    if missing:
        print(f"{WARNING} Missing {len(missing)} partition(s). Rebuilding missing partitions:\n")
        # Group them
        grouped = _group_consecutive_partitions(timeframe, missing)

        last_ts_so_far = None
        # find the maximum known candle so far
        for fcsv in files:
            path = os.path.join(dir_path, fcsv)
            df_existing = pd.read_csv(path, dtype=str)
            if not df_existing.empty and "timestamp" in df_existing.columns:
                df_existing["timestamp"] = df_existing["timestamp"].astype("int64")
                mx = df_existing["timestamp"].max()
                if (last_ts_so_far is None) or (mx > last_ts_so_far):
                    last_ts_so_far = mx

        # Fill by group, one group at a time
        last_ts_so_far = _fill_missing_partitions_by_group(
            dir_path, ticker, timeframe, grouped, last_ts_so_far, fill_end_ms
        )
    else:
        print(f"{INFO} No missing partitions detected. Data seems up to date for {timeframe}.")

    # Now re-check the last partition to ensure it's fully up to date
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    if files:
        last_file = files[-1][:-4]
        print(f"{WARNING} Re-checking last partition to ensure it's updated: {last_file}")
        lf_start, lf_end = partition_start_end_dates(timeframe, last_file)

        partition_csv = os.path.join(dir_path, last_file + ".csv")
        df_last = pd.read_csv(partition_csv, dtype=str)
        if not df_last.empty and "timestamp" in df_last.columns:
            df_last["timestamp"] = df_last["timestamp"].astype("int64")
            last_ts_in_partition = df_last["timestamp"].max()
            recheck_start_ms = last_ts_in_partition + 1
        else:
            recheck_start_ms = int(lf_start.timestamp() * 1000)

        # do a final fill for that partition up to fill_end_ms
        print(f"{INFO} Updating last partition from {recheck_start_ms} ms to {fill_end_ms} ms")
        _bulk_fill_missing(dir_path, ticker, timeframe, recheck_start_ms, fill_end_ms)

    # Final: show the last known timestamp
    last_ts_final = get_last_file_timestamp(dir_path)
    if last_ts_final:
        dt_human = datetime.fromtimestamp(last_ts_final / 1000, timezone.utc)
        print(f"{INFO} Final latest timestamp for {timeframe}: "
              f"{COLOR_TIMESTAMPS}{dt_human:%Y-%m-%d %H:%M:%S UTC}{Style.RESET_ALL}")

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync candle data from Bitfinex.")
    parser.add_argument("--exchange", required=True, help="Exchange name, e.g. BITFINEX")
    parser.add_argument("--ticker", required=True, help="Ticker symbol, e.g. tBTCUSD")
    parser.add_argument("--timeframe", required=False, default="1m", help="Timeframe to sync (1m, 1h, 1D).")
    parser.add_argument("--end", required=False, help="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    args = parser.parse_args()

    print(f"{INFO} Synchronizing single timeframe: {args.timeframe}")
    res = synchronize_candle_data(
        args.exchange,
        args.ticker,
        args.timeframe,
        end_date_str=args.end,
        verbose=True
    )
    if res:
        print(f"\n{SUCCESS} Synchronization completed successfully for timeframe: {args.timeframe}.\n")
