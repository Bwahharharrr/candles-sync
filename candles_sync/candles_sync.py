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

# FS scan tracing / tuning (progress vs detailed)
FAST_TAIL_BLOCK_SIZE = 8192              # bytes per backward block when tailing a CSV
LOG_SLOW_FILE_THRESHOLD_MS = 100         # used in 'detailed' mode for [SLOW] tagging (kept for parity)
TRACE_SCAN_SUMMARY_TOP_N = 5             # number of slowest files to list in the summary
FS_PROGRESS_BAR_WIDTH = 28               # characters in the inline progress bar
FS_PROGRESS_EVERY = 64                   # update progress every N files
FS_PROGRESS_MIN_INTERVAL_SEC = 0.08      # minimum interval between progress updates

# Env override for file-scan trace: 'off' | 'progress' | 'detailed'
ENV_FS_TRACE = "CANDLESYNC_FS_TRACE"

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
TRACE   = Fore.CYAN    + "[TRACE]"   + Style.RESET_ALL

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

def _log_trace(msg: str) -> None:
    """Emit a trace line immediately (no buffering)."""
    print(f"{TRACE} {msg}", flush=True)

def _progress_line(prefix: str, i: int, total: int, elapsed_s: float, last_file: str) -> str:
    """Build a single-line, inline-updating textual progress bar."""
    pct = 0.0 if total == 0 else (i / total) * 100.0
    fill = int((FS_PROGRESS_BAR_WIDTH * i) / max(1, total))
    bar = "#" * fill + "-" * (FS_PROGRESS_BAR_WIDTH - fill)
    return (
        f"\r{TRACE} {prefix} "
        f"[{Fore.CYAN}{bar}{Style.RESET_ALL}] "
        f"{i}/{total} ({pct:5.1f}%)  elapsed={elapsed_s:5.2f}s  last={COLOR_FILE}{last_file}{Style.RESET_ALL}"
    )

def normalize_exchange(exchange: str) -> str:
    """Normalize the exchange input. Currently this enforces uppercase."""
    return (exchange or "").upper()

def ensure_directory(exchange: str, ticker: str, timeframe: str, *, polling: bool = False) -> str:
    """Ensures that the data directory for a given timeframe exists."""
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
    """Robust conversion to Decimal from any scalar. Falls back to Decimal(0) on bad input."""
    try:
        return decimal.Decimal(str(value))
    except Exception:
        return decimal.Decimal(0)

def _decimal_to_canonical_str(d: decimal.Decimal) -> str:
    """Canonical number formatting for CSV."""
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s else "0"

def _num_to_canonical_str(x) -> str:
    return _decimal_to_canonical_str(_to_decimal(x))

def _canonicalize_numeric_df(df: pd.DataFrame) -> pd.DataFrame:
    """Apply canonical string formatting to OHLCV; timestamp becomes integer string."""
    out = df.copy()
    out["timestamp"] = pd.to_numeric(out["timestamp"], errors="coerce").astype("Int64").dropna().astype("int64")
    out["timestamp"] = out["timestamp"].apply(lambda v: str(int(v)))
    for c in ["open", "close", "high", "low", "volume"]:
        if c in out.columns:
            out[c] = out[c].apply(_num_to_canonical_str)
        else:
            out[c] = ""
    return out[CSV_COLUMNS]

def _to_int_series(s: pd.Series) -> pd.Series:
    """Convert a string/number series to int64 safely, coercing errors to NaN and dropping them."""
    return pd.to_numeric(s, errors="coerce").dropna().astype("int64")

def _safe_read_csv(path: str) -> pd.DataFrame:
    """Read a CSV as strings. Returns an empty DataFrame with expected columns if malformed."""
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception:
        return pd.DataFrame(columns=CSV_COLUMNS)
    if "timestamp" not in df.columns:
        return pd.DataFrame(columns=CSV_COLUMNS)
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
    for df in (df_old, df_new):
        for col in CSV_COLUMNS:
            if col not in df.columns:
                df[col] = pd.Series(dtype=str)
        df.drop(columns=[c for c in df.columns if c not in CSV_COLUMNS], inplace=True, errors="ignore")

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
        out[c] = merged[n].combine_first(merged[o]).apply(_decimal_to_canonical_str)

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

    When `polling=True`, console output is compact and includes timing.
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
            polling_tag = Fore.MAGENTA + POLLING_PREFIX + Style.RESET_ALL
            now_utc = datetime.now(timezone.utc)
            now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S")
            now_ms = int(now_utc.timestamp() * 1000)
            print(f"{polling_tag}: Now: {now_str} -- {now_ms}", flush=True)
            print(f"{polling_tag}: Fetching: {Fore.YELLOW}{full}{Style.RESET_ALL}", flush=True)
            t0 = time.perf_counter()
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

            delay = BACKOFF_INITIAL_SECONDS

            if polling:
                n = len(data) if data else 0
                polling_tag = Fore.MAGENTA + POLLING_PREFIX + Style.RESET_ALL
                if n == 0:
                    print(f"{polling_tag}: API Returned [0 elements]", flush=True)
                elif n == 1:
                    first = json.dumps(data[0], separators=(",", ":"))
                    print(f"{polling_tag}: API Returned [1 elements]: {first}", flush=True)
                else:
                    count_str = f"{Fore.RED}{n}{Style.RESET_ALL}"
                    first = json.dumps(data[0], separators=(",", ":"))
                    last  = json.dumps(data[-1], separators=(",", ":"))
                    print(f"{polling_tag}: API Returned [{count_str} elements]: {first} … {last}", flush=True)
                elapsed = time.perf_counter() - t0
                print(f"{polling_tag}: Finished in {elapsed:.3f}s", flush=True)
                return data

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
    """Create synthetic rows for missing timestamps inside a chunk."""
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
    return _canonicalize_numeric_df(df)

def _audit_api_chunk_fill_internal_gaps(
    timeframe: str,
    candles: List[List[float]],
    *,
    polling: bool = False
) -> List[List[str]]:
    """Audit the incoming API chunk and synthesize missing intervals inside first..last candle window."""
    if not candles or len(candles) < 2:
        if not candles:
            return []
        df = pd.DataFrame(candles, columns=CSV_COLUMNS)
        return _canonicalize_numeric_df(df).values.tolist()

    df = pd.DataFrame(candles, columns=CSV_COLUMNS)
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
        base_df = _canonicalize_numeric_df(df)
        merged = pd.concat([base_df, add_df], ignore_index=True)
        merged["timestamp"] = pd.to_numeric(merged["timestamp"], errors="coerce").astype("Int64").dropna().astype("int64")
        merged = merged.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
        merged = _canonicalize_numeric_df(merged)
        if not polling:
            print(f"{UPDATE} Chunk audit synthesized "
                  f"{COLOR_ROWS}{total_synth}{Style.RESET_ALL} candle(s) "
                  f"(out of {total_expected} expected across gaps).")
        return merged.values.tolist()

    return _canonicalize_numeric_df(df).values.tolist()

def partition_from_timestamp(timeframe: str, ts: int) -> str:
    """Given timeframe & timestamp, returns the partition name."""
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
    """For a partition name, returns (startDate, endDate) in UTC."""
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
    """Given timeframe, returns a list of partition names covering start_date..end_date."""
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
    """Merge/update rows and write canonicalized CSV; return number of corrected rows."""
    df_old = _safe_read_csv(csv_path)
    out, updated = _merge_candle_frames(df_old, df_new)
    out.to_csv(csv_path, index=False)
    return int(updated)

def save_candles_to_csv(candles: List[List[float]], dir_path: str, symbol: str, timeframe: str, *, polling: bool = False) -> None:
    """
    Partition raw candle data and save into CSV files; merge if file exists.
    Always writes canonical string formatting to CSV (no trailing '.0').
    """
    if not candles:
        return

    t_df_start = time.perf_counter()
    df = pd.DataFrame(candles, columns=CSV_COLUMNS)
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64").dropna().astype("int64")
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    df["partition"] = df["timestamp"].apply(lambda x: partition_from_timestamp(timeframe, int(x)))
    t_df_end = time.perf_counter()
    if not polling:
        print(f"{INFO} DataFrame prep stage took {(t_df_end - t_df_start):.3f}s")

    for part_val, grp in df.groupby("partition"):
        t_part_start = time.perf_counter()
        fname = os.path.join(dir_path, f"{part_val}.csv")
        g = grp.drop(columns=["partition"]).copy()
        g = _canonicalize_numeric_df(g)
        t_canon_end = time.perf_counter()
        if not polling:
            print(f"{INFO} Canonicalize stage for partition {part_val} took {(t_canon_end - t_part_start):.3f}s")

        if os.path.exists(fname):
            t_merge_start = time.perf_counter()
            updated = validate_and_update_candles(g, fname)
            t_merge_end = time.perf_counter()
            if not polling:
                print(f"{INFO} Merge/write stage for {part_val} took {(t_merge_end - t_merge_start):.3f}s")
            if updated > 0 and not polling:
                print(f"{UPDATE} {COLOR_ROWS}{updated}{Style.RESET_ALL} rows corrected in "
                      f"{COLOR_FILE}.../{symbol}/{timeframe}/{part_val}.csv{Style.RESET_ALL}")
        else:
            t_write_start = time.perf_counter()
            g = g.sort_values("timestamp").reset_index(drop=True)
            g.to_csv(fname, index=False)
            t_write_end = time.perf_counter()
            if not polling:
                print(f"{INFO} Write-new-file stage for {part_val} took {(t_write_end - t_write_start):.3f}s")
                start_ts = int(pd.to_numeric(g["timestamp"]).min())
                end_ts = int(pd.to_numeric(g["timestamp"]).max())
                st = datetime.fromtimestamp(start_ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
                et = datetime.fromtimestamp(end_ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
                print(f"{COLOR_NEW}[NEW]{Style.RESET_ALL} {len(g)} candles "
                      f"[{COLOR_TIMESTAMPS}{st} - {et}{Style.RESET_ALL}] → "
                      f"{COLOR_FILE}.../{symbol}/{timeframe}/{part_val}.csv{Style.RESET_ALL}")

def parse_partition_date(tf: str, part_str: str) -> datetime:
    """Inverse of partition_from_timestamp: parse a partition name into a datetime."""
    if tf == "1m":
        return datetime.strptime(part_str, DATE_FMT_DAY).replace(tzinfo=timezone.utc)
    elif tf == "1h":
        return datetime.strptime(part_str, DATE_FMT_MONTH).replace(day=1, tzinfo=timezone.utc)
    else:
        return datetime.strptime(part_str, DATE_FMT_YEAR).replace(month=1, day=1, tzinfo=timezone.utc)

def _is_next_consecutive(tf: str, current_str: str, next_str: str) -> bool:
    """Is next_str the immediate consecutive partition after current_str?"""
    current_dt = parse_partition_date(tf, current_str)
    next_dt    = parse_partition_date(tf, next_str)

    if tf == "1m":
        return (next_dt - current_dt) == timedelta(days=1)
    elif tf == "1h":
        year = current_dt.year + (current_dt.month // 12)
        month = (current_dt.month % 12) + 1
        candidate = current_dt.replace(year=year, month=month, day=1)
        return candidate == next_dt
    else:
        candidate = current_dt.replace(year=current_dt.year + 1, month=1, day=1)
        return candidate == next_dt

def _group_consecutive_partitions(tf: str, missing: List[str]) -> List[List[str]]:
    """Group a sorted list of missing partition strings into consecutive runs."""
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
    """Create empty CSVs for daily/monthly/yearly partitions if they don't exist."""
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

# ----------------- FAST LAST-TIMESTAMP CSV SCAN WITH PROGRESS + SUMMARY --- #

def _fast_last_timestamp_from_csv(path: str) -> Optional[int]:
    """
    Fast path to get the last timestamp from a partition CSV by tail-reading.
    Assumes the file is sorted ascending by 'timestamp' (writer guarantees this).
    Falls back to a pandas scan if parsing fails.
    """
    try:
        filesize = os.path.getsize(path)
        if filesize == 0:
            return None

        with open(path, "rb") as f:
            buf = b""
            pos = f.seek(0, os.SEEK_END)
            while pos > 0:
                read_size = min(FAST_TAIL_BLOCK_SIZE, pos)
                pos -= read_size
                f.seek(pos)
                chunk = f.read(read_size)
                buf = chunk + buf
                if buf.count(b"\n") >= 1:
                    break

            lines = buf.splitlines()
            for raw in reversed(lines):
                line = raw.strip()
                if not line:
                    continue
                if line.lower().startswith(b"timestamp,"):
                    continue
                first = line.split(b",", 1)[0].strip().strip(b'"')
                if not first:
                    continue
                try:
                    return int(first)
                except Exception:
                    try:
                        return int(float(first.decode("utf-8", "ignore")))
                    except Exception:
                        continue
        return None
    except Exception:
        return None

def _fallback_last_timestamp_with_pandas(path: str) -> Optional[int]:
    """Robust fallback for last timestamp extraction using pandas (timestamp column only)."""
    try:
        df = pd.read_csv(path, usecols=["timestamp"], dtype=str)
    except Exception:
        return None
    if df.empty or "timestamp" not in df.columns:
        return None
    ts = pd.to_numeric(df["timestamp"], errors="coerce").dropna()
    if ts.empty:
        return None
    return int(ts.max())

def get_last_file_timestamp(
    dir_path: str,
    *,
    polling: bool = False,
    fs_trace_mode: str = "off"   # 'off' | 'progress' | 'detailed'
) -> Optional[int]:
    """
    Scan all CSV partitions and return the maximum timestamp (ms).

    fs_trace_mode:
      - 'off'      : no per-file output
      - 'progress' : single inline progress bar + final TOP-N summary
      - 'detailed' : per-file timings + final TOP-N summary
    """
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    if not files:
        return None

    if fs_trace_mode not in ("off", "progress", "detailed"):
        fs_trace_mode = "off"

    total = len(files)
    t_total_start = time.perf_counter()
    max_ts: Optional[int] = None
    max_file: Optional[str] = None

    slow_records = []  # (elapsed_ms, filename, method, size_kb)
    last_progress_print = 0.0

    # Optional header for progress mode
    if fs_trace_mode == "progress" and not polling:
        print(f"{TRACE} FS scan: scanning {total} partition file(s) under {COLOR_DIR}{dir_path}{Style.RESET_ALL}", flush=True)
        print(_progress_line("FS scan", 0, total, 0.0, "-"), end="", flush=True)

    for idx, fname in enumerate(files, start=1):
        path = os.path.join(dir_path, fname)
        size_kb = (os.path.getsize(path) // 1024) if os.path.exists(path) else 0

        t0 = time.perf_counter()
        method = "fast"
        ts_val = _fast_last_timestamp_from_csv(path)
        if ts_val is None:
            method = "pandas"
            ts_val = _fallback_last_timestamp_with_pandas(path)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        # Record timing for summary in both 'progress' and 'detailed' modes
        if fs_trace_mode in ("progress", "detailed") and not polling:
            slow_records.append((elapsed_ms, fname, method, size_kb))

        # Per-file lines only in 'detailed'
        if fs_trace_mode == "detailed" and not polling:
            tag = "SLOW" if elapsed_ms >= LOG_SLOW_FILE_THRESHOLD_MS else "ok"
            _log_trace(f"[{idx:04d}/{total:04d}] "
                       f"{COLOR_FILE}{fname}{Style.RESET_ALL} size={size_kb}KB "
                       f"method={method:<6} time={elapsed_ms:.1f}ms "
                       f"{('[SLOW]' if tag == 'SLOW' else '')}")

        # Inline progress updates (throttled)
        if fs_trace_mode == "progress" and not polling:
            now = time.perf_counter()
            if (idx % FS_PROGRESS_EVERY == 0) or ((now - last_progress_print) >= FS_PROGRESS_MIN_INTERVAL_SEC) or (idx == total):
                elapsed_s = now - t_total_start
                print(_progress_line("FS scan", idx, total, elapsed_s, fname), end="", flush=True)
                last_progress_print = now

        if ts_val is not None and (max_ts is None or ts_val > max_ts):
            max_ts = ts_val
            max_file = fname

    # Commit the inline progress line to a newline
    if fs_trace_mode == "progress" and not polling:
        print()  # newline

    total_ms = (time.perf_counter() - t_total_start) * 1000.0

    if max_ts is None:
        if not polling:
            print(f"{WARNING} No valid timestamps found across partitions in {COLOR_DIR}{dir_path}{Style.RESET_ALL}")
        return None

    # Final TOP-N summary for both 'progress' and 'detailed'
    if fs_trace_mode in ("progress", "detailed") and not polling:
        slowest = sorted(slow_records, key=lambda x: x[0], reverse=True)[:TRACE_SCAN_SUMMARY_TOP_N]
        _log_trace(f"Partition scan complete in {total_ms:.1f}ms (files={total}, top-{TRACE_SCAN_SUMMARY_TOP_N} slowest listed below):")
        for rank, (ms, fn, meth, kb) in enumerate(slowest, start=1):
            # Three spaces before the index to match the style you prefer
            _log_trace(f"   {rank}. {COLOR_FILE}{fn}{Style.RESET_ALL} size={kb}KB method={meth} time={ms:.1f}ms")

    if not polling:
        human = datetime.fromtimestamp(max_ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"{INFO} Latest recorded timestamp across partitions "
              f"(from {COLOR_FILE}{max_file}{Style.RESET_ALL}): "
              f"{COLOR_TIMESTAMPS}{max_ts}{Style.RESET_ALL} ({human})")
    return max_ts

# --------------------------------- SYNC ----------------------------------- #

def _bulk_fill_missing(
    dir_path: str,
    ticker: str,
    timeframe: str,
    start_ms: int,
    end_ms: Optional[int],
    *,
    polling: bool = False
) -> None:
    """Fetch candles in chunk(s) from start_ms..end_ms and save them to CSVs."""
    cur = start_ms
    while True:
        if end_ms is not None and cur > end_ms:
            break

        t_fetch_start = time.perf_counter()
        data = fetch_bitfinex_candles(ticker, timeframe, cur, end=end_ms, limit=API_LIMIT, polling=polling)
        t_fetch_end = time.perf_counter()
        if not polling:
            print(f"{INFO} Fetch stage took {(t_fetch_end - t_fetch_start):.3f}s")

        if not data:
            if end_ms is not None and cur <= end_ms:
                _create_empty_partitions(dir_path, timeframe, ticker, cur, end_ms, polling=polling)
            break

        t_audit_start = time.perf_counter()
        data = _audit_api_chunk_fill_internal_gaps(timeframe=timeframe, candles=data, polling=polling)
        t_audit_end = time.perf_counter()
        if not polling:
            print(f"{INFO} Audit stage took {(t_audit_end - t_audit_start):.3f}s")

        earliest_in_chunk = min(int(c[0]) for c in data)
        if earliest_in_chunk > cur:
            _create_empty_partitions(dir_path, timeframe, ticker, cur, earliest_in_chunk - 1, polling=polling)

        t_save_start = time.perf_counter()
        save_candles_to_csv(data, dir_path, ticker, timeframe, polling=polling)
        t_save_end = time.perf_counter()
        if not polling:
            print(f"{INFO} Save stage took {(t_save_end - t_save_start):.3f}s")

        last_ts = max(int(c[0]) for c in data)
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
    """Fill each consecutive run of missing partitions."""
    for idx, group in enumerate(groups, start=1):
        first_p = group[0]
        last_p  = group[-1]

        if not polling:
            print(f"{WARNING} Missing partition Group {idx} (last candle on record → next candle on record):")
            for mp in group:
                print(f"    - {COLOR_TIMESTAMPS}{mp}{Style.RESET_ALL}")
            print()

        earliest_dt, _ = partition_start_end_dates(timeframe, first_p)
        _, last_dt_end = partition_start_end_dates(timeframe, last_p)

        earliest_ms = int(earliest_dt.timestamp() * 1000)
        last_ms     = int(last_dt_end.timestamp()  * 1000)

        if last_ts_so_far is None:
            start_ms = earliest_ms
        else:
            candidate = last_ts_so_far if ALWAYS_INCLUDE_LAST_ON_RANGE_START else last_ts_so_far + 1
            start_ms = min(candidate, earliest_ms)

        group_end_ms = last_ms if fill_end_ms is None else min(last_ms, fill_end_ms)

        if start_ms > group_end_ms:
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

def _utc_now_str() -> str:
    """Return current UTC time formatted for logs."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def _trace_utc(msg: str) -> None:
    """Emit a [TRACE] line with an explicit UTC timestamp prefix."""
    print(f"{TRACE} [{_utc_now_str()} UTC] {msg}", flush=True)


# --- REPLACEMENT: drop this whole function in place of your existing one --- #

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

    Improvements in this version:
    - Eliminates the silent delay between "Missing N partition(s)..." and the first group print
      by (a) replacing an O(N) pandas scan across all CSVs with the fast tail scanner and
      (b) adding explicit, timestamped TRACE logs that bracket each expensive step.
    - All added trace lines use explicit UTC timestamps in 'YYYY-MM-DD HH:MM:SS' format.
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
        with open(gotstart_path, "w") as _:
            pass
        if not polling:
            print(f"{INFO} Created {COLOR_FILE}{GOTSTART_FILE}{Style.RESET_ALL}")
        return True

    # Otherwise do incremental
    if not polling:
        print(f"{INFO} '{GOTSTART_FILE}' exists for {timeframe}. Checking missing partitions...")

    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".csv"))
    if not files:
        if not polling:
            print(f"{WARNING} Marker present but no CSVs. Re-running full sync for {timeframe}.")
        os.remove(gotstart_path)
        return synchronize_candle_data(exchange, ticker, timeframe, end_date_str, verbose, polling=polling)

    earliest_file = files[0][:-4]
    earliest_dt = parse_partition_date(timeframe, earliest_file)

    have = set(f[:-4] for f in files if f.endswith(".csv"))
    need = set(generate_partitions(timeframe, earliest_dt, fill_end_date))
    missing = sorted(need - have)

    if missing:
        if not polling:
            print(f"{WARNING} Missing {len(missing)} partition(s). Rebuilding missing partitions:\n", flush=True)

        # ---- Added, visible TRACE with UTC: bracket the expensive steps in this gap ---- #

        # Step 1: grouping the missing partitions into consecutive runs
        t_grp_start = time.perf_counter()
        _trace_utc(f"Step 1/2: Grouping {len(missing)} missing partition(s) into consecutive runs...")
        grouped = _group_consecutive_partitions(timeframe, missing)
        t_grp_end = time.perf_counter()
        _trace_utc(f"Step 1/2 complete: {len(grouped)} run(s) in {(t_grp_end - t_grp_start):.3f}s.")

        # Step 2: determine the last recorded candle timestamp quickly (fast tail scan)
        # Choose FS scan trace mode: env override, else 'progress' when verbose, else 'off'.
        env_mode = (os.getenv(ENV_FS_TRACE) or "").strip().lower()
        if env_mode in ("off", "progress", "detailed"):
            fs_trace_mode_for_scan = env_mode
        else:
            fs_trace_mode_for_scan = "progress" if (verbose and not polling) else "off"

        t_scan_start = time.perf_counter()
        _trace_utc(
            f"Step 2/2: Scanning existing CSV partitions to find last recorded timestamp "
            f"(dir={dir_path}, files={len(files)}, mode={fs_trace_mode_for_scan})."
        )

        # IMPORTANT: This replaces the previous O(N) pandas read of every CSV.
        # It uses the fast tail scanner + minimal pandas fallback (timestamp col only).
        last_ts_so_far: Optional[int] = get_last_file_timestamp(
            dir_path, polling=polling, fs_trace_mode=fs_trace_mode_for_scan
        )

        t_scan_end = time.perf_counter()
        if last_ts_so_far is None:
            _trace_utc(f"Scan complete in {(t_scan_end - t_scan_start):.3f}s; no existing timestamps found.")
        else:
            last_human = datetime.fromtimestamp(last_ts_so_far / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            _trace_utc(
                f"Scan complete in {(t_scan_end - t_scan_start):.3f}s; "
                f"last_ts={last_ts_so_far} ({last_human})."
            )

        # Now proceed to fill the grouped missing partitions (this prints the Group 1... lines)
        last_ts_so_far = _fill_missing_partitions_by_group(
            dir_path, ticker, timeframe, grouped, last_ts_so_far, fill_end_ms, polling=polling
        )

    else:
        if not polling:
            print(f"{INFO} No missing partitions detected. Data seems up to date for {timeframe}.")

    # Re-check the last partition to ensure it's fully up to date
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
                recheck_start_ms = last_ts_in_partition
            else:
                recheck_start_ms = int(lf_start.timestamp() * 1000)
        else:
            recheck_start_ms = int(lf_start.timestamp() * 1000)

        if not polling:
            print(f"{INFO} Updating last partition from {recheck_start_ms} ms to {fill_end_ms} ms")
        _bulk_fill_missing(dir_path, ticker, timeframe, recheck_start_ms, fill_end_ms, polling=polling)

    # Final: show the last known timestamp (across all partitions)
    # Choose FS scan trace mode: env override, else 'progress' when verbose, else 'off'.
    env_mode = (os.getenv(ENV_FS_TRACE) or "").strip().lower()
    if env_mode in ("off", "progress", "detailed"):
        fs_trace_mode = env_mode
    else:
        fs_trace_mode = "progress" if (verbose and not polling) else "off"

    last_ts_final = get_last_file_timestamp(dir_path, polling=polling, fs_trace_mode=fs_trace_mode)
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
    # Tip: override FS scan trace via env var CANDLESYNC_FS_TRACE=off|progress|detailed
    args = parser.parse_args()

    exchange = normalize_exchange(args.exchange)

    print(f"{INFO} Synchronizing single timeframe: {args.timeframe}")
    res = synchronize_candle_data(
        exchange=exchange,
        ticker=args.ticker,
        timeframe=args.timeframe,
        end_date_str=args.end,
        verbose=True
    )
    if res:
        print(f"\n{SUCCESS} Synchronization completed successfully for timeframe: {args.timeframe}.\n")
        return 0
    return 1

if __name__ == "__main__":
    sys.exit(main())
