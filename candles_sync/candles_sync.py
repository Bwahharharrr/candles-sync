#!/usr/bin/env python3
"""
candles_sync.py

This script synchronizes historical candle data for a given exchange/ticker
for a single timeframe (1m, 1h, 1D). You can run it repeatedly to keep your
local partitions up to date.

Partitions:
    - 1m -> stored daily (YYYY-MM-DD.csv)
    - 1h -> stored monthly (YYYY-MM.csv)
    - 1D -> stored yearly (YYYY.csv)

Features:
- First‐time sync (no ".gotstart" file): queries from the epoch (0) to build full history.
- Incremental sync (".gotstart" exists):
    1) Finds missing partitions grouped by consecutive runs (e.g., daily runs).
    2) For each run, performs a chunked fetch from:
         (last-known-candle **inclusive**) or earliest partition of that run,
       **whichever is earlier**, up to that run's last partition (end of that day/month/year).
    3) Writes data or creates empty CSV files for each missing partition in the run.
    4) Jumps to the next missing run without re-fetching unneeded ranges.
    5) After all runs, re-checks the last partition and **always re-polls the last saved candle**
       to refresh its values (e.g., volume corrections).
    6) **Chunk audit (no re-query):** every API response chunk (if it has at least 2 candles) is
       audited for internal gaps between its **first** and **last** returned candles. Any missing
       intervals inside that window are **synthesized locally** with `volume=0` and
       `O=H=L=C=previous_close` (no extra API calls).

Usage:
    python candles_sync.py --exchange BITFINEX --ticker tBTCUSD [--end YYYY-MM-DD]
    or
    python candles_sync.py --exchange BITFINEX --ticker tBTCUSD --timeframe 1m
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import requests
import pandas as pd
import decimal
import json
from typing import List, Optional, Tuple
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode

# ------------------------------ Constants --------------------------------- #

BITFINEX_API_URL = "https://api-pub.bitfinex.com/v2/candles/trade:{}:{}/hist"
ROOT_PATH = os.path.expanduser("~/.corky")

API_LIMIT = 10000
BACKOFF_INITIAL_SECONDS = 30
BACKOFF_MAX_SECONDS = 300
HTTP_TIMEOUT_SECONDS = 30

VALID_TIMEFRAMES = {"1m", "1h", "1D"}
CSV_COLUMNS = ["timestamp", "open", "close", "high", "low", "volume"]
GOTSTART_FILE = ".gotstart"
USER_AGENT = "CandleSync/1.0"

DATE_FMT_DAY = "%Y-%m-%d"
DATE_FMT_MONTH = "%Y-%m"
DATE_FMT_YEAR = "%Y"

INTERVAL_MS = {"1m": 60_000, "1h": 3_600_000, "1D": 86_400_000}

# Behavior toggles
ALWAYS_INCLUDE_LAST_ON_RANGE_START = True  # inclusive start at last candle on record

# Polling / compact output
POLLING_PREFIX = "[candles-sync]"

# ------------------------------ Colors ------------------------------------ #

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

HEADERS = {"User-Agent": USER_AGENT}

# ------------------------------ Helpers ----------------------------------- #

def normalize_exchange(exchange: str) -> str:
    """
    Normalize the exchange input. Currently this enforces uppercase.
    """
    return (exchange or "").upper()

def ensure_directory(exchange: str, ticker: str, timeframe: str, *, polling: bool = False) -> str:
    """
    Ensures that the data directory for a given timeframe exists.
    Returns the path to the directory.
    """
    dir_path = os.path.join(ROOT_PATH, exchange, "candles", ticker, timeframe)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        if not polling:
            print(f"{INFO} Created directory: {COLOR_DIR}{dir_path}{Style.RESET_ALL}")
    else:
        if not polling:
            print(f"{INFO} Directory already exists: {COLOR_DIR}{dir_path}{Style.RESET_ALL}")
    return dir_path

def _to_decimal(value) -> decimal.Decimal:
    """
    Robust conversion to Decimal from any scalar. Falls back to Decimal(0) on bad input.
    """
    try:
        return decimal.Decimal(str(value))
    except Exception:
        return decimal.Decimal(0)

def _decimal_to_canonical_str(d: decimal.Decimal) -> str:
    """
    Canonical number formatting for CSV:
      - fixed-point (no scientific)
      - strip trailing zeros
      - strip trailing decimal point
    Examples:
      118590.0 -> "118590"
      0.082088240000 -> "0.08208824"
      0 -> "0"
    """
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s else "0"

def _num_to_canonical_str(x) -> str:
    return _decimal_to_canonical_str(_to_decimal(x))

def _canonicalize_numeric_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply canonical string formatting to OHLCV; timestamp becomes integer string.
    Returns a new DataFrame with string-typed columns.
    """
    out = df.copy()
    # Timestamp
    out["timestamp"] = pd.to_numeric(out["timestamp"], errors="coerce").astype("Int64").dropna().astype("int64")
    out["timestamp"] = out["timestamp"].apply(lambda v: str(int(v)))
    # OHLCV
    for c in ["open", "close", "high", "low", "volume"]:
        if c in out.columns:
            out[c] = out[c].apply(_num_to_canonical_str)
        else:
            out[c] = ""
    # Reorder/limit to expected columns
    return out[CSV_COLUMNS]

def _to_int_series(s: pd.Series) -> pd.Series:
    """
    Convert a string/number series to int64 safely, coercing errors to NaN and dropping them.
    """
    return pd.to_numeric(s, errors="coerce").dropna().astype("int64")

def _safe_read_csv(path: str) -> pd.DataFrame:
    """
    Read a CSV as strings. Returns an empty DataFrame with expected columns if malformed.
    """
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception:
        return pd.DataFrame(columns=CSV_COLUMNS)
    if "timestamp" not in df.columns:
        return pd.DataFrame(columns=CSV_COLUMNS)
    # Ensure presence & order
    for col in CSV_COLUMNS:
        if col not in df.columns:
            df[col] = pd.Series(dtype=str)
    return df[CSV_COLUMNS]

def _merge_candle_frames(df_old: pd.DataFrame, df_new: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Merge df_new into df_old using 'timestamp' as key.
    Compare numeric fields as Decimals; prefer new values when both present.
    Returns (merged_df_sorted_and_canonicalized, updated_count).
    """
    # Normalize shapes/columns
    for df in (df_old, df_new):
        for col in CSV_COLUMNS:
            if col not in df.columns:
                df[col] = pd.Series(dtype=str)
        df.drop(columns=[c for c in df.columns if c not in CSV_COLUMNS], inplace=True, errors="ignore")

    # Dtypes
    df_old["timestamp"] = _to_int_series(df_old["timestamp"])
    df_new["timestamp"] = _to_int_series(df_new["timestamp"])

    num_cols = ["open", "close", "high", "low", "volume"]
    for col in num_cols:
        df_old[col] = df_old[col].apply(_to_decimal)
        df_new[col] = df_new[col].apply(_to_decimal)

    merged = pd.merge(
        df_old, df_new, on="timestamp", how="outer", suffixes=("_old", "_new"), indicator=True
    )

    updated = 0
    for _, r in merged.iterrows():
        if r["_merge"] == "both" and any(r[f"{c}_old"] != r[f"{c}_new"] for c in num_cols):
            updated += 1

    out = pd.DataFrame()
    out["timestamp"] = merged["timestamp"]
    for c in num_cols:
        o, n = f"{c}_old", f"{c}_new"
        # Prefer new; convert to canonical string
        out[c] = merged[n].combine_first(merged[o]).apply(_decimal_to_canonical_str)

    # Finalize: sort, set timestamp to string, ensure column order
    out = out.sort_values("timestamp").reset_index(drop=True)
    out["timestamp"] = out["timestamp"].apply(lambda v: str(int(v)))
    out = out[CSV_COLUMNS]
    return out, int(updated)

def fetch_bitfinex_candles(
    symbol: str,
    timeframe: str,
    start: int,
    end: Optional[int] = None,
    limit: int = API_LIMIT,
    *,
    polling: bool = False
) -> List[List[float]]:
    """
    Fetches up to `limit` candles from Bitfinex, ascending (sort=1),
    starting at `start` up to `end` if given.
    Retries on network/429 with exponential backoff.
    Returns list of candle arrays or empty list if no data.

    API rows are: [mts, open, close, high, low, volume].

    When `polling=True`, console output is reduced to two concise lines per request:
      - "[candles-sync]: Fetching: <full-url>"
      - "[candles-sync]: API Returned [N elements]: <first> … <last>"
    """
    url = BITFINEX_API_URL.format(timeframe, symbol)
    params = {"limit": limit, "sort": 1, "start": start}
    if end is not None:
        params["end"] = end

    delay = BACKOFF_INITIAL_SECONDS
    max_delay = BACKOFF_MAX_SECONDS

    while True:
        full = f"{url}?{urlencode(params)}"
        if polling:
            print(f"{POLLING_PREFIX}: Fetching: {full}")
        else:
            print(f"{INFO} Fetching candles from Bitfinex API...")
            print(f"       URL: {COLOR_FILE}{full}{Style.RESET_ALL}")

        try:
            resp = requests.get(full, headers=HEADERS, timeout=HTTP_TIMEOUT_SECONDS)
        except Exception as e:
            if not polling:
                print(f"{ERROR} Network error: {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay = min(max_delay, delay * 2)
            continue

        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception as e:
                if not polling:
                    print(f"{ERROR} Failed to decode JSON response: {e}")
                return []

            # Reset backoff
            delay = BACKOFF_INITIAL_SECONDS

            if polling:
                n = 0 if not data else len(data)
                if n == 0:
                    print(f"{POLLING_PREFIX}: API Returned [0 elements]")
                else:
                    first = json.dumps(data[0], separators=(",", ":"))
                    last  = json.dumps(data[-1], separators=(",", ":"))
                    print(f"{POLLING_PREFIX}: API Returned [{n} elements]: {first} … {last}")
                return data

            # Non-polling verbose path (original behavior)
            if not data:
                print(f"{WARNING} No candles returned from API.")
                return []

            try:
                ts = [int(c[0]) for c in data]
                first_h = datetime.fromtimestamp(min(ts) / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                last_h  = datetime.fromtimestamp(max(ts) / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                print(f"{SUCCESS} Received {len(data)} candles "
                      f"({COLOR_TIMESTAMPS}{first_h}{Style.RESET_ALL} → {COLOR_TIMESTAMPS}{last_h}{Style.RESET_ALL})")
            except Exception:
                print(f"{SUCCESS} Received {len(data)} candles.")
            return data

        if resp.status_code == 429:
            if not polling:
                print(f"{WARNING} Rate limit hit (429). Retrying in {delay}s...")
            time.sleep(delay)
            delay = min(max_delay, delay * 2)
            continue

        if not polling:
            print(f"{ERROR} HTTP {resp.status_code}: {resp.text}")
        return []

def _get_interval_ms(timeframe: str) -> int:
    if timeframe not in INTERVAL_MS:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return INTERVAL_MS[timeframe]

def _synthesize_gap_rows(expected_ts: List[int], prev_close_any) -> pd.DataFrame:
    """
    Create synthetic rows for expected timestamps with volume=0 and
    O/H/L/C all equal to prev_close.
    """
    prev_close = _num_to_canonical_str(prev_close_any)
    rows = []
    for ts in expected_ts:
        rows.append({
            "timestamp": int(ts),
            "open": prev_close,
            "close": prev_close,
            "high": prev_close,
            "low": prev_close,
            "volume": "0"
        })
    df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    # Ensure canonical string formatting & column order
    return _canonicalize_numeric_df(df)

def _audit_api_chunk_fill_internal_gaps(
    timeframe: str,
    candles: List[List[float]],
    *,
    polling: bool = False
) -> List[List[str]]:
    """
    Audit the incoming API chunk. If the chunk has >= 2 candles, scan for missing
    intervals between the first and last candle in the chunk. For each gap:
      - DO NOT re-query the API.
      - Synthesize missing intervals with volume=0 and O/H/L/C equal to the
        previous candle's close (the candle immediately before the gap).
    Returns a new list of candles (as canonical strings) containing the original
    chunk plus any synthesized rows.
    """
    if not candles or len(candles) < 2:
        # Canonicalize if needed
        if not candles:
            return []
        df = pd.DataFrame(candles, columns=CSV_COLUMNS)
        return _canonicalize_numeric_df(df).values.tolist()

    df = pd.DataFrame(candles, columns=CSV_COLUMNS)

    # Normalize timestamp and sort; leave OHLCV as-is for now (canonicalize later)
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64").dropna().astype("int64")
    df = df.drop_duplicates(subset=["timestamp"], keep="last")
    df = df.sort_values("timestamp").reset_index(drop=True)

    if len(df) < 2:
        return _canonicalize_numeric_df(df).values.tolist()

    interval = _get_interval_ms(timeframe)
    inserts: List[pd.DataFrame] = []
    total_synth = 0
    total_expected = 0

    for i in range(len(df) - 1):
        prev_ts = int(df.iloc[i]["timestamp"])
        next_ts = int(df.iloc[i + 1]["timestamp"])
        gap = next_ts - prev_ts
        if gap <= interval:
            continue

        expected_ts = list(range(prev_ts + interval, next_ts, interval))
        total_expected += len(expected_ts)
        prev_close_val = df.iloc[i]["close"]

        synth_df = _synthesize_gap_rows(expected_ts, prev_close_val)
        total_synth += len(synth_df)
        if not synth_df.empty:
            inserts.append(synth_df)

    if inserts:
        add_df = pd.concat(inserts, ignore_index=True)
        # Canonicalize the existing part, then concat, then sort/dedup
        base_df = _canonicalize_numeric_df(df)
        merged = pd.concat([base_df, add_df], ignore_index=True)
        merged["timestamp"] = pd.to_numeric(merged["timestamp"], errors="coerce").astype("Int64").dropna().astype("int64")
        merged = merged.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
        # Ensure everything is canonical strings again
        merged = _canonicalize_numeric_df(merged)
        if not polling:
            print(f"{UPDATE} Chunk audit synthesized "
                  f"{COLOR_ROWS}{total_synth}{Style.RESET_ALL} candle(s) "
                  f"(out of {total_expected} expected across gaps).")
        return merged.values.tolist()

    # No gaps; just canonicalize and return
    return _canonicalize_numeric_df(df).values.tolist()

def partition_from_timestamp(timeframe: str, ts: int) -> str:
    """
    Given timeframe & timestamp, returns the partition name:
     - "1m" => daily "YYYY-MM-DD"
     - "1h" => monthly "YYYY-MM"
     - "1D" => yearly "YYYY"
    """
    if timeframe not in VALID_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    dt = datetime.fromtimestamp(ts / 1000, timezone.utc)
    if timeframe == "1m":
        return dt.strftime(DATE_FMT_DAY)
    elif timeframe == "1h":
        return dt.strftime(DATE_FMT_MONTH)
    else:
        return dt.strftime(DATE_FMT_YEAR)

def partition_start_end_dates(tf: str, part_str: str) -> Tuple[datetime, datetime]:
    """
    For a partition name, returns (startDate, endDate) in UTC.
      - 1m => day start..day end
      - 1h => month start..month end
      - 1D => year start..year end
    """
    if tf == "1m":
        dt_start = datetime.strptime(part_str, DATE_FMT_DAY).replace(tzinfo=timezone.utc)
        dt_end = dt_start + timedelta(days=1) - timedelta(seconds=1)
    elif tf == "1h":
        dt_start = datetime.strptime(part_str, DATE_FMT_MONTH).replace(day=1, tzinfo=timezone.utc)
        year = dt_start.year + (dt_start.month // 12)
        month = (dt_start.month % 12) + 1
        dt_end_candidate = dt_start.replace(year=year, month=month, day=1, hour=0, minute=0, second=0)
        dt_end = dt_end_candidate - timedelta(seconds=1)
    else:  # '1D'
        dt_start = datetime.strptime(part_str, DATE_FMT_YEAR).replace(month=1, day=1, tzinfo=timezone.utc)
        dt_end_candidate = dt_start.replace(year=dt_start.year + 1, month=1, day=1, hour=0, minute=0, second=0)
        dt_end = dt_end_candidate - timedelta(seconds=1)
    return dt_start, dt_end

def generate_partitions(timeframe: str, start_date: datetime, end_date: datetime) -> List[str]:
    """
    Given timeframe, returns a list of partition names covering start_date..end_date.
    """
    partitions: List[str] = []
    cur = start_date

    if timeframe == "1m":
        cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)
        while cur <= end_date:
            partitions.append(cur.strftime(DATE_FMT_DAY))
            cur += timedelta(days=1)
        return partitions

    elif timeframe == "1h":
        cur = cur.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while cur <= end_date:
            partitions.append(cur.strftime(DATE_FMT_MONTH))
            year = cur.year + (cur.month // 12)
            month = (cur.month % 12) + 1
            cur = cur.replace(year=year, month=month, day=1)
        return partitions

    else:  # '1D'
        cur = cur.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        while cur <= end_date:
            partitions.append(cur.strftime(DATE_FMT_YEAR))
            cur = cur.replace(year=cur.year + 1, month=1, day=1)
        return partitions

def validate_and_update_candles(df_new: pd.DataFrame, csv_path: str) -> int:
    """
    Merges existing CSV (if any) with df_new on timestamp, preserving and updating
    rows. Returns how many rows were corrected/updated. Ensures canonical formatting.
    """
    df_old = _safe_read_csv(csv_path)
    out, updated = _merge_candle_frames(df_old, df_new)
    # out is already canonicalized
    out.to_csv(csv_path, index=False)
    return int(updated)

def save_candles_to_csv(candles: List[List[float]], dir_path: str, symbol: str, timeframe: str, *, polling: bool = False) -> None:
    """
    Given raw candle data, partition it by day/month/year and save
    into CSV files. If a file already exists, merges data.
    Always writes canonical string formatting to CSV (no trailing '.0').
    """
    if not candles:
        return

    df = pd.DataFrame(candles, columns=CSV_COLUMNS)
    # Normalize timestamp for grouping; keep values raw for now
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64").dropna().astype("int64")
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    df["partition"] = df["timestamp"].apply(lambda x: partition_from_timestamp(timeframe, int(x)))

    for part_val, grp in df.groupby("partition"):
        fname = os.path.join(dir_path, f"{part_val}.csv")
        g = grp.drop(columns=["partition"]).copy()
        # Canonicalize numeric formatting BEFORE any merge/write
        g = _canonicalize_numeric_df(g)

        if os.path.exists(fname):
            updated = validate_and_update_candles(g, fname)
            if updated > 0 and not polling:
                print(f"{UPDATE} {COLOR_ROWS}{updated}{Style.RESET_ALL} rows corrected in "
                      f"{COLOR_FILE}.../{symbol}/{timeframe}/{part_val}.csv{Style.RESET_ALL}")
        else:
            # New file path
            g = g.sort_values("timestamp").reset_index(drop=True)
            g.to_csv(fname, index=False)
            if not polling:
                start_ts = int(pd.to_numeric(g["timestamp"]).min())
                end_ts = int(pd.to_numeric(g["timestamp"]).max())
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
        return datetime.strptime(part_str, DATE_FMT_DAY).replace(tzinfo=timezone.utc)
    elif tf == "1h":
        return datetime.strptime(part_str, DATE_FMT_MONTH).replace(day=1, tzinfo=timezone.utc)
    else:
        return datetime.strptime(part_str, DATE_FMT_YEAR).replace(month=1, day=1, tzinfo=timezone.utc)

def _is_next_consecutive(tf: str, current_str: str, next_str: str) -> bool:
    """
    Determines if next_str is the immediate "consecutive" partition after current_str
    (e.g., for daily, it's exactly 1 day later).
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

def _group_consecutive_partitions(tf: str, missing: List[str]) -> List[List[str]]:
    """
    Groups a sorted list of missing partition strings into consecutive runs.
    """
    if not missing:
        return []

    groups: List[List[str]] = []
    current_group = [missing[0]]

    for i in range(len(missing) - 1):
        this_part = missing[i]
        next_part = missing[i + 1]
        if _is_next_consecutive(tf, this_part, next_part):
            current_group.append(next_part)
        else:
            groups.append(current_group)
            current_group = [next_part]

    if current_group:
        groups.append(current_group)

    return groups

def _create_empty_partitions(dir_path: str, timeframe: str, symbol: str, from_ts: int, to_ts: int, *, polling: bool = False) -> None:
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
            pd.DataFrame(columns=CSV_COLUMNS).to_csv(fname, index=False)
            if not polling:
                print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} Created empty CSV for missing partition "
                      f"{COLOR_TIMESTAMPS}{p}{Style.RESET_ALL} → {COLOR_FILE}{p}.csv{Style.RESET_ALL}")

def _bulk_fill_missing(
    dir_path: str,
    ticker: str,
    timeframe: str,
    start_ms: int,
    end_ms: Optional[int],
    *,
    polling: bool = False
) -> None:
    """
    Fetch candles in chunk(s) from start_ms..end_ms. For each chunk:
      - audit the chunk internally and fill gaps between first/last candles (no re-query)
      - fill empty CSVs if there's a gap before the first returned candle
      - save the (audited) chunk
      - move 'cur' to last_candle+1
      - break if no more data or chunk < API_LIMIT
    """
    cur = start_ms
    while True:
        if end_ms is not None and cur > end_ms:
            break

        data = fetch_bitfinex_candles(ticker, timeframe, cur, end=end_ms, limit=API_LIMIT, polling=polling)
        if not data:
            if end_ms is not None and cur <= end_ms:
                _create_empty_partitions(dir_path, timeframe, ticker, cur, end_ms, polling=polling)
            break

        # Audit the chunk for internal gaps (between first and last candle of this response)
        data = _audit_api_chunk_fill_internal_gaps(timeframe=timeframe, candles=data, polling=polling)

        # If the first in this audited chunk starts after 'cur', create empties for that outer gap
        earliest_in_chunk = min(int(c[0]) for c in data)
        if earliest_in_chunk > cur:
            _create_empty_partitions(dir_path, timeframe, ticker, cur, earliest_in_chunk - 1, polling=polling)

        save_candles_to_csv(data, dir_path, ticker, timeframe, polling=polling)

        last_ts = max(int(c[0]) for c in data)
        # Allow equality: when we purposefully refetch the (same) last saved candle
        if last_ts < cur:
            if not polling:
                print(f"{ERROR} Failsafe triggered: last_ts={last_ts} < cur={cur}. Breaking loop.")
            break

        cur = last_ts + 1
        if len(data) < API_LIMIT:
            if end_ms is not None and cur <= end_ms:
                _create_empty_partitions(dir_path, timeframe, ticker, cur, end_ms, polling=polling)
            break

def _fill_missing_partitions_by_group(
    dir_path: str,
    ticker: str,
    timeframe: str,
    groups: List[List[str]],
    last_ts_so_far: Optional[int],
    fill_end_ms: Optional[int],
    *,
    polling: bool = False
) -> Optional[int]:
    """
    For each consecutive run of missing partitions, do a chunk-based fill
    from (last_ts_so_far **inclusive**) or earliest_in_run up to that run's last partition.
    Then stop and move on to the next group.

    Rationale: we *always* include the last candle on record at the start of a fetch
    window to allow refreshing a potentially incomplete last candle.
    """
    for idx, group in enumerate(groups, start=1):
        first_p = group[0]
        last_p  = group[-1]

        if not polling:
            print(f"{WARNING} Missing partition Group {idx} (last candle on record → next candle on record):")
            for mp in group:
                print(f"    - {COLOR_TIMESTAMPS}{mp}{Style.RESET_ALL}")
            print()

        # Convert earliest & last partition in this group to timestamps
        earliest_dt, _ = partition_start_end_dates(timeframe, first_p)
        _, last_dt_end = partition_start_end_dates(timeframe, last_p)

        earliest_ms = int(earliest_dt.timestamp() * 1000)
        last_ms     = int(last_dt_end.timestamp()  * 1000)

        if last_ts_so_far is None:
            start_ms = earliest_ms
        else:
            # Start from whichever is EARLIER between:
            #   - the last candle on record (INCLUSIVE), and
            #   - the earliest partition in this group.
            candidate = last_ts_so_far if ALWAYS_INCLUDE_LAST_ON_RANGE_START else last_ts_so_far + 1
            start_ms = min(candidate, earliest_ms)

        group_end_ms = last_ms if fill_end_ms is None else min(last_ms, fill_end_ms)

        if start_ms > group_end_ms:
            # Nothing to fetch for this group
            continue

        if not polling:
            print(
                f"{INFO} Running fill for Group {idx} from {start_ms} ms → {group_end_ms} ms "
                f"(covering partitions {first_p} .. {last_p})"
            )
        _bulk_fill_missing(dir_path, ticker, timeframe, start_ms, group_end_ms, polling=polling)

        # update last_ts_so_far from newly created/updated files
        files_in_dir = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
        for fcsv in files_in_dir:
            path = os.path.join(dir_path, fcsv)
            df_check = _safe_read_csv(path)
            if not df_check.empty and "timestamp" in df_check.columns:
                ts_series = _to_int_series(df_check["timestamp"])
                if not ts_series.empty:
                    mx = int(ts_series.max())
                    if (last_ts_so_far is None) or (mx > last_ts_so_far):
                        last_ts_so_far = mx

    return last_ts_so_far

def get_last_file_timestamp(dir_path: str, *, polling: bool = False) -> Optional[int]:
    """
    Scans all CSV partitions in dir_path and returns the maximum timestamp (ms)
    observed across all files. Prints details about the file that contained it.
    """
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    if not files:
        return None

    max_ts: Optional[int] = None
    max_file: Optional[str] = None

    for fname in files:
        df = _safe_read_csv(os.path.join(dir_path, fname))
        if df.empty or "timestamp" not in df.columns:
            continue
        ts_series = _to_int_series(df["timestamp"])
        if ts_series.empty:
            continue
        cur_max = int(ts_series.max())
        if max_ts is None or cur_max > max_ts:
            max_ts = cur_max
            max_file = fname

    if max_ts is None:
        if not polling:
            print(f"{WARNING} No valid timestamps found across partitions in {COLOR_DIR}{dir_path}{Style.RESET_ALL}")
        return None

    if not polling:
        human = datetime.fromtimestamp(max_ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"{INFO} Latest recorded timestamp across partitions "
              f"(from {COLOR_FILE}{max_file}{Style.RESET_ALL}): "
              f"{COLOR_TIMESTAMPS}{max_ts}{Style.RESET_ALL} ({human})")
    return max_ts

def synchronize_candle_data(
    exchange: str,
    ticker: str,
    timeframe: str,
    end_date_str: Optional[str] = None,
    verbose: bool = False,
    polling: bool = False
) -> bool:
    """
    Main function to sync historical candles from Bitfinex for a single timeframe.

    1) If no .gotstart => full sync from epoch -> end_date (or now if none).
    2) If .gotstart => find missing partitions, group them by consecutive runs, and fill
       each run separately (avoiding re-fetching the entire range over and over).
    3) Re-check the last partition for completeness up to end_date (or now), and
       ALWAYS re-poll the last saved candle to refresh it.
    4) Each API chunk is audited for internal gaps and missing intervals are synthesized.

    Parameters
    ----------
    exchange : str
    ticker   : str
    timeframe: str
    end_date_str : Optional[str]
        End date (YYYY-MM-DD or YYYY-MM-DD HH:MM). If None, "now" is used.
    verbose : bool
        When True (CLI), prints more context. Ignored when `polling` is True.
    polling : bool
        When True, suppresses normal console output and prints only two compact lines
        per API request:
          - "[candles-sync]: Fetching: <full-url>"
          - "[candles-sync]: API Returned [N elements]: <first> … <last>"
        Default is False (off).
    """
    if timeframe not in VALID_TIMEFRAMES:
        if not polling:
            print(f"{ERROR} Invalid timeframe '{timeframe}'. Must be one of {VALID_TIMEFRAMES}.")
        return False

    if not polling:
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

    dir_path = ensure_directory(exchange, ticker, timeframe, polling=polling)
    gotstart_path = os.path.join(dir_path, GOTSTART_FILE)

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
        if not polling:
            print(f"{WARNING} No '{GOTSTART_FILE}' file found. Starting full historical sync for {timeframe}...")
        start_ms = 0  # epoch
        end_ms   = fill_end_ms

        _bulk_fill_missing(dir_path, ticker, timeframe, start_ms, end_ms, polling=polling)
        # Mark that we got started
        with open(gotstart_path, "w") as _:
            pass
        if not polling:
            print(f"{INFO} Created {COLOR_FILE}{GOTSTART_FILE}{Style.RESET_ALL}")
        return True

    # Otherwise do incremental
    if not polling:
        print(f"{INFO} '{GOTSTART_FILE}' exists for {timeframe}. Checking missing partitions...")

    # If no CSV files but .gotstart is present, treat like full sync
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    if not files:
        if not polling:
            print(f"{WARNING} Marker present but no CSVs. Re-running full sync for {timeframe}.")
        os.remove(gotstart_path)
        return synchronize_candle_data(exchange, ticker, timeframe, end_date_str, verbose, polling=polling)

    # Find earliest existing partition
    earliest_file = files[0][:-4]
    earliest_dt = parse_partition_date(timeframe, earliest_file)

    # Find all missing partitions
    have = set(f[:-4] for f in files if f.endswith(".csv"))
    need = set(generate_partitions(timeframe, earliest_dt, fill_end_date))
    missing = sorted(need - have)

    if missing:
        if not polling:
            print(f"{WARNING} Missing {len(missing)} partition(s). Rebuilding missing partitions:\n")
        # Group them
        grouped = _group_consecutive_partitions(timeframe, missing)

        last_ts_so_far: Optional[int] = None
        # find the maximum known candle so far
        for fcsv in files:
            path = os.path.join(dir_path, fcsv)
            df_existing = _safe_read_csv(path)
            if not df_existing.empty and "timestamp" in df_existing.columns:
                ts_series = _to_int_series(df_existing["timestamp"])
                if not ts_series.empty:
                    mx = int(ts_series.max())
                    if (last_ts_so_far is None) or (mx > last_ts_so_far):
                        last_ts_so_far = mx

        # Fill by group, one group at a time
        last_ts_so_far = _fill_missing_partitions_by_group(
            dir_path, ticker, timeframe, grouped, last_ts_so_far, fill_end_ms, polling=polling
        )
    else:
        if not polling:
            print(f"{INFO} No missing partitions detected. Data seems up to date for {timeframe}.")

    # Now re-check the last partition to ensure it's fully up to date
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    if files:
        last_file = files[-1][:-4]
        if not polling:
            print(f"{WARNING} Re-checking last partition to ensure it's updated: {last_file}")
        lf_start, _lf_end = partition_start_end_dates(timeframe, last_file)

        partition_csv = os.path.join(dir_path, last_file + ".csv")
        df_last = _safe_read_csv(partition_csv)
        if not df_last.empty and "timestamp" in df_last.columns:
            ts_series = _to_int_series(df_last["timestamp"])
            if not ts_series.empty:
                last_ts_in_partition = int(ts_series.max())
                # ALWAYS refresh the very last saved candle:
                # re-poll starting from the last known candle timestamp (inclusive)
                recheck_start_ms = last_ts_in_partition
            else:
                recheck_start_ms = int(lf_start.timestamp() * 1000)
        else:
            recheck_start_ms = int(lf_start.timestamp() * 1000)

        # do a final fill for that partition up to fill_end_ms
        if not polling:
            print(f"{INFO} Updating last partition from {recheck_start_ms} ms to {fill_end_ms} ms")
        _bulk_fill_missing(dir_path, ticker, timeframe, recheck_start_ms, fill_end_ms, polling=polling)

    # Final: show the last known timestamp (across all partitions)
    last_ts_final = get_last_file_timestamp(dir_path, polling=polling)
    if (last_ts_final is not None) and (not polling):
        dt_human = datetime.fromtimestamp(last_ts_final / 1000, timezone.utc)
        print(f"{INFO} Final latest timestamp for {timeframe}: "
              f"{COLOR_TIMESTAMPS}{dt_human:%Y-%m-%d %H:%M:%S UTC}{Style.RESET_ALL}")

    return True

# --------------------------------- CLI ------------------------------------ #

def main() -> int:
    parser = argparse.ArgumentParser(description="Sync candle data from Bitfinex.")
    parser.add_argument("--exchange", required=True, help="Exchange name, e.g. BITFINEX")
    parser.add_argument("--ticker", required=True, help="Ticker symbol, e.g. tBTCUSD")
    parser.add_argument(
        "--timeframe",
        required=False,
        default="1m",
        choices=sorted(VALID_TIMEFRAMES),
        help="Timeframe to sync (1m, 1h, 1D)."
    )
    parser.add_argument("--end", required=False, help="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    args = parser.parse_args()

    # Ensure --exchange is uppercase (as requested)
    exchange = normalize_exchange(args.exchange)

    print(f"{INFO} Synchronizing single timeframe: {args.timeframe}")
    res = synchronize_candle_data(
        exchange=exchange,
        ticker=args.ticker,
        timeframe=args.timeframe,
        end_date_str=args.end,
        verbose=True
        # Note: polling mode is intended for programmatic use; CLI remains verbose.
    )
    if res:
        print(f"\n{SUCCESS} Synchronization completed successfully for timeframe: {args.timeframe}.\n")
        return 0
    return 1

if __name__ == "__main__":
    sys.exit(main())
