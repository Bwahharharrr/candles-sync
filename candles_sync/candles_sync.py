#!/usr/bin/env python3
"""
candles_sync.py

Synchronize historical candle data for a given exchange/ticker and a single timeframe (1m, 1h, 1D).
Safe to run repeatedly; it incrementally fills gaps and refreshes the most recent partition.

Partitions:
    - 1m -> stored daily   (YYYY-MM-DD.csv)
    - 1h -> stored monthly (YYYY-MM.csv)
    - 1D -> stored yearly  (YYYY.csv)

Features preserved:
- First-time full sync from epoch (marks directory with .gotstart).
- Incremental sync that:
  * Detects missing partitions and fills them in consecutive groups.
  * Writes canonical CSVs with stable numeric formatting.
  * Merges new candles into existing files without duplications.
  * Synthesizes internal chunk gaps locally (no extra API calls).
  * Creates empty CSVs for partitions that have no trades.
  * Always re-polls from the last saved candle (inclusive) to refresh the latest partition.
"""

from __future__ import annotations

import argparse
import decimal
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, List, Optional

import pandas as pd
import requests
from urllib.parse import urlencode

try:
    import colorama
    from colorama import Fore, Style
    colorama.init(autoreset=True)
except ImportError:
    class NoColor:
        def __getattr__(self, item):
            return ''
    Fore = Style = NoColor()  # type: ignore[attr-defined]

# ------------------------------ Constants --------------------------------- #

USER_AGENT = "CandleSync/1.0"
HEADERS = {"User-Agent": USER_AGENT}

# Level tags (labels)
INFO    = Fore.GREEN   + "[INFO]"    + Style.RESET_ALL
WARNING = Fore.YELLOW  + "[WARNING]" + Style.RESET_ALL  # kept for compatibility
ERROR   = Fore.RED     + "[ERROR]"   + Style.RESET_ALL
SUCCESS = Fore.GREEN   + "[SUCCESS]" + Style.RESET_ALL
UPDATE  = Fore.MAGENTA + "[UPDATE]"  + Style.RESET_ALL
TRACE   = Fore.CYAN    + "[TRACE]"   + Style.RESET_ALL

# Additional concise tags for uniformity
WARN = Fore.YELLOW + "[WARN]" + Style.RESET_ALL
NEW  = Fore.WHITE  + "[NEW]"  + Style.RESET_ALL

# Inline value colors
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

ROOT_PATH = Path.home() / ".corky"

API_LIMIT = 10_000
HTTP_TIMEOUT_SECONDS = 30
BACKOFF_INITIAL_SECONDS = 30
BACKOFF_MAX_SECONDS = 300

VALID_TIMEFRAMES = {"1m", "1h", "1D"}
CSV_COLUMNS = ["timestamp", "open", "close", "high", "low", "volume"]
GOTSTART_FILE = ".gotstart"

DATE_FMT_DAY = "%Y-%m-%d"
DATE_FMT_MONTH = "%Y-%m"
DATE_FMT_YEAR = "%Y"

INTERVAL_MS = {"1m": 60_000, "1h": 3_600_000, "1D": 86_400_000}

# ------------------------------ Logging helpers --------------------------- #

LABELS = {
    "INFO": INFO,
    "WARN": WARN,
    "ERROR": ERROR,
    "SUCCESS": SUCCESS,
    "UPDATE": UPDATE,
    "TRACE": TRACE,
    "NEW": NEW,
}

def _label(level: str) -> str:
    return LABELS.get(level, INFO)

def log(level: str, message: str) -> None:
    print(f"{_label(level)} {message}")

def log_info(message: str) -> None:
    log("INFO", message)

def log_warn(message: str) -> None:
    log("WARN", message)

def log_error(message: str) -> None:
    log("ERROR", message)

def log_success(message: str) -> None:
    log("SUCCESS", message)

def log_update(message: str) -> None:
    log("UPDATE", message)

def log_trace(message: str) -> None:
    log("TRACE", message)

def log_new(message: str) -> None:
    log("NEW", message)

def c_dir(x: object) -> str:
    return f"{COLOR_DIR}{x}{Style.RESET_ALL}"

def c_file(x: object) -> str:
    return f"{COLOR_FILE}{x}{Style.RESET_ALL}"

def c_ts(x: object) -> str:
    return f"{COLOR_TIMESTAMPS}{x}{Style.RESET_ALL}"

def c_rows(x: object) -> str:
    return f"{COLOR_ROWS}{x}{Style.RESET_ALL}"

def c_var(x: object) -> str:
    return f"{COLOR_VAR}{x}{Style.RESET_ALL}"

def c_type(x: object) -> str:
    return f"{COLOR_TYPE}{x}{Style.RESET_ALL}"

def c_desc(x: object) -> str:
    return f"{COLOR_DESC}{x}{Style.RESET_ALL}"

# Polling tag + helper (compact, lightly colored)
POLL_TAG = Fore.CYAN + "[candles-sync]" + Style.RESET_ALL
def poll_print(message: str) -> None:
    print(f"{POLL_TAG} {message}", flush=True)

# Human-readable timestamp helpers for polling clarity
def _fmt_ts_utc(ms: int) -> str:
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "N/A"

def _fmt_mts_map(ms: int) -> str:
    """Format 'milliseconds -> human time' with coloring."""
    return f"{c_var(int(ms))} → {c_ts(_fmt_ts_utc(int(ms)))}"

def _fmt_ohlcv_inline(row: List[float]) -> str:
    """
    Bitfinex order: [mts, open, close, high, low, volume]
    Return compact inline OHLCV with canonicalized numbers.
    """
    try:
        o = _canonical_num_str(row[1])
        c = _canonical_num_str(row[2])
        h = _canonical_num_str(row[3])
        l = _canonical_num_str(row[4])
        v = _canonical_num_str(row[5])
        return c_desc(f"O:{o} C:{c} H:{h} L:{l} V:{v}")
    except Exception:
        return c_desc("O:?-C:? H:? L:? V:?")

# ------------------------------ Utilities --------------------------------- #

def normalize_exchange(exchange: str) -> str:
    return (exchange or "").upper()

def _safe_join_and_assert_within_root(root: Path, *parts: str) -> Path:
    """Prevent path traversal via user-controlled segments."""
    candidate = root
    for p in parts:
        candidate = candidate / p
    resolved = candidate.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError:
        raise ValueError("Invalid path components: resolved path escapes ROOT_PATH")
    return resolved

def ensure_directory(exchange: str, ticker: str, timeframe: str, *, polling: bool = False) -> str:
    """Create (or confirm) the data directory and return its path as a string."""
    root = ROOT_PATH
    dir_path = _safe_join_and_assert_within_root(root, exchange, "candles", ticker, timeframe)
    dir_path.mkdir(parents=True, exist_ok=True)
    if not polling:
        log_info(f"Using directory: {c_dir(dir_path)}")
    return str(dir_path)

def _get_interval_ms(timeframe: str) -> int:
    tf = to_timeframe_key(timeframe)
    return INTERVAL_MS[tf]

def to_timeframe_key(tf: str) -> str:
    # Guard against accidental whitespace or casing
    key = (tf or "").strip()
    if key not in VALID_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe: {tf}")
    return key

def _first_of_month(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def _first_of_next_month(dt: datetime) -> datetime:
    base = _first_of_month(dt)
    return (base.replace(day=28) + timedelta(days=4)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def _first_of_year(dt: datetime) -> datetime:
    return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

def _first_of_next_year(dt: datetime) -> datetime:
    return _first_of_year(dt).replace(year=dt.year + 1)

# ------------------------------ Partition --------------------------------- #

@dataclass(frozen=True, order=True)
class Partition:
    """
    Tiny value-object to eliminate duplicated date logic.

    name:
      - 1m -> "YYYY-MM-DD" (day)
      - 1h -> "YYYY-MM"    (month)
      - 1D -> "YYYY"       (year)

    start/end are inclusive UTC bounds for the partition.
    """
    timeframe: str
    name: str
    start: datetime  # inclusive
    end: datetime    # inclusive

    @staticmethod
    def _align_start(tf: str, dt: datetime) -> datetime:
        tf = to_timeframe_key(tf)
        if tf == "1m":
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        if tf == "1h":
            return _first_of_month(dt)
        return _first_of_year(dt)

    @staticmethod
    def _next_start(tf: str, start: datetime) -> datetime:
        tf = to_timeframe_key(tf)
        if tf == "1m":
            return start + timedelta(days=1)
        if tf == "1h":
            return _first_of_next_month(start)
        return _first_of_next_year(start)

    @staticmethod
    def from_timestamp(tf: str, ts_ms: int) -> "Partition":
        dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
        start = Partition._align_start(tf, dt)
        next_start = Partition._next_start(tf, start)
        end = next_start - timedelta(seconds=1)
        name = (
            start.strftime(DATE_FMT_DAY) if tf == "1m"
            else start.strftime(DATE_FMT_MONTH) if tf == "1h"
            else start.strftime(DATE_FMT_YEAR)
        )
        return Partition(tf, name, start, end)

    @staticmethod
    def from_name(tf: str, name: str) -> "Partition":
        tf = to_timeframe_key(tf)
        if tf == "1m":
            start = datetime.strptime(name, DATE_FMT_DAY).replace(tzinfo=timezone.utc)
        elif tf == "1h":
            start = datetime.strptime(name, DATE_FMT_MONTH).replace(day=1, tzinfo=timezone.utc)
        else:
            start = datetime.strptime(name, DATE_FMT_YEAR).replace(month=1, day=1, tzinfo=timezone.utc)
        next_start = Partition._next_start(tf, start)
        end = next_start - timedelta(seconds=1)
        return Partition(tf, name, start, end)

    @staticmethod
    def all_between(tf: str, start_dt: datetime, end_dt: datetime) -> List["Partition"]:
        """Generate all partitions covering [start_dt, end_dt], inclusive."""
        cur = Partition._align_start(tf, start_dt)
        out: List[Partition] = []
        while cur <= end_dt:
            part = Partition.from_timestamp(tf, int(cur.timestamp() * 1000))
            out.append(part)
            cur = Partition._next_start(tf, cur)
        return out

    def next(self) -> "Partition":
        return Partition.from_timestamp(self.timeframe, int((self.end + timedelta(seconds=1)).timestamp() * 1000))

# ------------------------------ Canonical numbers ------------------------- #

def _to_decimal(value) -> decimal.Decimal:
    try:
        return decimal.Decimal(str(value))
    except Exception:
        return decimal.Decimal(0)

def _canonical_num_str(x: object) -> str:
    # 10dp is plenty for Bitfinex spot candles yet keeps size modest.
    s = f"{decimal.Decimal(str(x)):.10f}"
    s = s.rstrip("0").rstrip(".")
    return s if s else "0"

# ------------------------------ File helpers ------------------------------ #

def write_partition(df: pd.DataFrame, path: str) -> int:
    """
    Merge new candles into existing file (if any) and write canonical CSV.
    Returns the delta (#rows in merged - #rows in old).
    """
    if os.path.exists(path):
        old = pd.read_csv(path, dtype=str)
    else:
        old = pd.DataFrame(columns=CSV_COLUMNS)

    # Ensure correct columns, keep only expected
    for col in CSV_COLUMNS:
        if col not in df.columns:
            df[col] = pd.Series(dtype=str)
        if col not in old.columns:
            old[col] = pd.Series(dtype=str)
    df = df[CSV_COLUMNS]
    old = old[CSV_COLUMNS]

    merged = (
        pd.concat([old, df], ignore_index=True)
        .assign(timestamp=lambda x: pd.to_numeric(x.timestamp, errors="coerce"))
        .dropna(subset=["timestamp"])
        .astype({"timestamp": int})
        .drop_duplicates(subset="timestamp", keep="last")
        .sort_values("timestamp")
    )

    # Canonical string representation
    merged["timestamp"] = merged["timestamp"].astype(int)
    for col in ["open", "close", "high", "low", "volume"]:
        merged[col] = merged[col].apply(_canonical_num_str)

    merged.to_csv(path, index=False)
    return int(len(merged) - len(old))

def last_timestamp_in_dir(dir_path: str) -> Optional[int]:
    """
    Read only the 'timestamp' column and grab the last row for each CSV.
    Practical, simple and fast enough in practice (O(N) with tiny scans).
    """
    timestamps: List[int] = []
    for f in sorted(os.listdir(dir_path)):
        if not f.endswith(".csv"):
            continue
        full = os.path.join(dir_path, f)
        try:
            if os.path.getsize(full) <= 50:  # header-only or empty
                continue
            ts_col = pd.read_csv(full, usecols=["timestamp"]).iloc[-1, 0]
            ts = int(pd.to_numeric([ts_col], errors="coerce").dropna().astype(int)[0])
            timestamps.append(ts)
        except Exception:
            continue
    return max(timestamps) if timestamps else None

def _group_consecutive(tf: str, names: List[str]) -> List[List[str]]:
    """Group partition names into consecutive runs (per timeframe)."""
    if not names:
        return []
    parts = [Partition.from_name(tf, n) for n in names]
    parts.sort(key=lambda p: p.start)
    groups: List[List[str]] = [[parts[0].name]]
    prev = parts[0]
    for p in parts[1:]:
        if p.start == prev.next().start:
            groups[-1].append(p.name)
        else:
            groups.append([p.name])
        prev = p
    return groups

def _create_empty_partitions(dir_path: str, tf: str, start_ms: int, end_ms: int, *, polling: bool = False) -> None:
    """Create empty CSVs for partitions with no trades in [start_ms, end_ms]."""
    if end_ms < start_ms:
        return
    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
    for p in Partition.all_between(tf, start_dt, end_dt):
        path = os.path.join(dir_path, f"{p.name}.csv")
        if not os.path.exists(path):
            pd.DataFrame(columns=CSV_COLUMNS).to_csv(path, index=False)
            if not polling:
                log_new(f"Empty partition created: {c_file(p.name)}")

# ------------------------------ Bitfinex API ------------------------------ #

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
    Fetch up to `limit` candles from Bitfinex in ascending order (sort=1),
    starting at `start` (inclusive) and up to `end` if provided.

    API rows: [mts, open, close, high, low, volume]
    """
    url = BITFINEX_API_URL.format(to_timeframe_key(timeframe), symbol)
    params = {"limit": int(limit), "sort": 1, "start": int(start)}
    if end is not None:
        params["end"] = int(end)

    delay = BACKOFF_INITIAL_SECONDS
    max_delay = BACKOFF_MAX_SECONDS

    while True:
        full = f"{url}?{urlencode(params)}"
        if polling:
            now_utc = datetime.now(timezone.utc)
            poll_print(f"Now: {c_ts(now_utc.strftime('%Y-%m-%d %H:%M:%S'))} -- {c_var(int(now_utc.timestamp()*1000))}")
            poll_print(f"Fetching: {c_var(full)}")
            t0 = time.perf_counter()
        else:
            log_info(f"GET {c_var(full)}")

        try:
            resp = requests.get(full, headers=HEADERS, timeout=HTTP_TIMEOUT_SECONDS)
        except Exception as e:
            if not polling:
                log_error(f"Network error: {c_desc(e)}. Retrying in {c_var(f'{delay}s')}...")
            time.sleep(delay)
            delay = min(max_delay, delay * 2)
            continue

        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception as e:
                if not polling:
                    log_error(f"Failed to decode JSON response: {c_desc(e)}")
                return []

            if polling:
                n = len(data) if data else 0
                if n == 0:
                    poll_print("API Returned [0 elements]")
                    elapsed = time.perf_counter() - t0
                    poll_print(f"Finished in {c_var(f'{elapsed:.3f}s')}")
                    return data

                # Show human-readable mapping for timestamps (single or range)
                if n == 1:
                    r = data[0]
                    mts = int(r[0])
                    poll_print(f"API Returned [{c_rows(1)} elements]")
                    poll_print(f"  mts: {_fmt_mts_map(mts)}  [{_fmt_ohlcv_inline(r)}]")
                else:
                    first = data[0]
                    last  = data[-1]
                    f_mts = int(first[0])
                    l_mts = int(last[0])
                    poll_print(f"API Returned [{c_rows(n)} elements]")
                    poll_print(f"  first: mts {_fmt_mts_map(f_mts)}  [{_fmt_ohlcv_inline(first)}]")
                    poll_print(f"  last : mts {_fmt_mts_map(l_mts)}  [{_fmt_ohlcv_inline(last)}]")

                elapsed = time.perf_counter() - t0
                poll_print(f"Finished in {c_var(f'{elapsed:.3f}s')}, time now: {datetime.now().isoformat(timespec="microseconds")}")
                
                return data

            # non-polling
            if not data:
                log_warn("API returned 0 candles.")
                return []
            try:
                ts = [int(c[0]) for c in data]
                first_h = datetime.fromtimestamp(min(ts) / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                last_h = datetime.fromtimestamp(max(ts) / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                log_success(f"Received {c_rows(len(data))} candles ({c_ts(first_h)} → {c_ts(last_h)})")
            except Exception:
                log_success(f"Received {c_rows(len(data))} candles")
            return data

        if resp.status_code == 429:
            if not polling:
                log_warn(f"Rate limit (429). Retrying in {c_var(f'{delay}s')}...")
            time.sleep(delay)
            delay = min(max_delay, delay * 2)
            continue

        if not polling:
            log_error(f"HTTP {c_var(resp.status_code)}: {c_desc(resp.text)}")
        return []

# ------------------------------ Gap audit --------------------------------- #

def find_genesis_timestamp(ticker: str, timeframe: str, *, polling: bool = False) -> int:
    """
    Ask Bitfinex for the very first candle ever traded for this symbol/timeframe.
    We request only 1 candle starting from epoch — Bitfinex returns the earliest one available.
    This prevents creating 40+ years of empty partitions.
    """
    if polling:
        poll_print("Discovering genesis candle...")
    raw = fetch_bitfinex_candles(
        ticker, timeframe, start=0, limit=1, polling=polling
    )
    if not raw:
        if not polling:
            log_warn("No candles ever returned — assuming genesis = now")
        return int(datetime.now(timezone.utc).timestamp() * 1000)
    genesis_ms = int(raw[0][0])  # first candle's MTS
    if not polling:
        genesis_dt = datetime.fromtimestamp(genesis_ms / 1000, tz=timezone.utc)
        log_info(f"Genesis candle found: {c_ts(genesis_dt.strftime('%Y-%m-%d %H:%M:%S UTC'))}")
    return genesis_ms

def _synthesize_gap_rows(expected_ts: List[int], prev_close_any) -> pd.DataFrame:
    prev_close = _canonical_num_str(prev_close_any)
    rows = []
    for ts in expected_ts:
        rows.append(
            {
                "timestamp": int(ts),
                "open": prev_close,
                "close": prev_close,
                "high": prev_close,
                "low": prev_close,
                "volume": "0",
            }
        )
    return pd.DataFrame(rows, columns=CSV_COLUMNS)

def _audit_api_chunk_fill_internal_gaps(
    timeframe: str,
    candles: List[List[float]],
    *,
    polling: bool = False
) -> List[List[str]]:
    """
    Audit an API chunk and synthesize missing intervals inside the chunk's first..last window.
    Returns list-of-lists matching CSV_COLUMNS, with canonical strings.
    """
    if not candles:
        return []

    df = pd.DataFrame(candles, columns=CSV_COLUMNS)
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64").dropna().astype("int64")
    df = df.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)

    if len(df) < 2:
        # Canonicalize single row quickly
        for c in ["open", "close", "high", "low", "volume"]:
            df[c] = df[c].apply(_canonical_num_str)
        df["timestamp"] = df["timestamp"].astype(int)
        return df[CSV_COLUMNS].astype(str).values.tolist()

    interval = _get_interval_ms(timeframe)
    inserts: List[pd.DataFrame] = []

    for i in range(len(df) - 1):
        prev_ts = int(df.iloc[i]["timestamp"])
        next_ts = int(df.iloc[i + 1]["timestamp"])
        gap = next_ts - prev_ts
        if gap <= interval:
            continue
        expected_ts = list(range(prev_ts + interval, next_ts, interval))
        prev_close_val = df.iloc[i]["close"]
        inserts.append(_synthesize_gap_rows(expected_ts, prev_close_val))

    if inserts:
        add_df = pd.concat(inserts, ignore_index=True) if inserts else pd.DataFrame(columns=CSV_COLUMNS)
        merged = pd.concat([df[CSV_COLUMNS], add_df], ignore_index=True)
        merged["timestamp"] = pd.to_numeric(merged["timestamp"], errors="coerce").astype("Int64").dropna().astype("int64")
        merged = merged.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)
        df = merged

    # Canonicalize
    df["timestamp"] = df["timestamp"].astype(int)
    for c in ["open", "close", "high", "low", "volume"]:
        df[c] = df[c].apply(_canonical_num_str)
    return df[CSV_COLUMNS].astype(str).values.tolist()

# ------------------------------ Range fetch & save ------------------------ #

def _fetch_and_save_range(
    ticker: str,
    tf: str,
    dir_path: str,
    start_ms: int,
    end_ms: int,
    *,
    polling: bool = False,
    write_fn: Callable[[pd.DataFrame, str], int] = write_partition,
) -> None:
    """Fetch ascending chunks within [start_ms, end_ms] and persist them into partition CSVs."""
    cur = int(start_ms)
    tf = to_timeframe_key(tf)
    interval = _get_interval_ms(tf)

    while cur <= end_ms:
        raw = fetch_bitfinex_candles(ticker, tf, cur, end=end_ms, limit=API_LIMIT, polling=polling)
        if not raw:
            if cur <= end_ms:
                _create_empty_partitions(dir_path, tf, cur, end_ms, polling=polling)
            break

        candles = _audit_api_chunk_fill_internal_gaps(tf, raw, polling=polling)
        if not candles:
            # Nothing came back; avoid infinite loop
            if cur <= end_ms:
                _create_empty_partitions(dir_path, tf, cur, end_ms, polling=polling)
            break

        df = pd.DataFrame(candles, columns=CSV_COLUMNS, dtype=str)
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64").dropna().astype(int)

        # Make empty files for partitions before the first returned candle in this chunk
        earliest = int(df["timestamp"].min())
        if earliest > cur:
            _create_empty_partitions(dir_path, tf, cur, min(earliest - 1, end_ms), polling=polling)

        # Partition and write
        df["partition"] = df["timestamp"].apply(lambda ts: Partition.from_timestamp(tf, int(ts)).name)
        for part_name, sub in df.groupby("partition", sort=True):
            path = os.path.join(dir_path, f"{part_name}.csv")
            write_fn(sub[CSV_COLUMNS], path)

        last_ts = int(df["timestamp"].max())

        # Safety: should never go backwards
        if last_ts < cur:
            if not polling:
                log_error("API returned candles older than start cursor. Stopping.")
            break

        # If we got fewer than limit candles → we are at the end of available data
        if len(raw) < API_LIMIT:
            if not polling:
                last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
                log_info(f"Reached end of available data at {c_ts(last_dt.strftime('%Y-%m-%d %H:%M UTC'))} ({c_rows(len(raw))} candles in last chunk)")
            break  # ← stop after partial chunk

        # Otherwise: advance to next candle after this one
        cur = last_ts + interval

        # If we already covered to end, we can stop
        if cur > end_ms:
            break

# ------------------------------ Synchronization --------------------------- #

def synchronize_candle_data(
    exchange: str,
    ticker: str,
    timeframe: str,
    end_date_str: Optional[str] = None,
    verbose: bool = False,
    polling: bool = False,
) -> bool:
    timeframe = to_timeframe_key(timeframe)
    dir_path = ensure_directory(exchange, ticker, timeframe, polling=polling)
    gotstart = os.path.join(dir_path, GOTSTART_FILE)

    # Parse end date
    if end_date_str:
        fmt = "%Y-%m-%d %H:%M" if " " in end_date_str else "%Y-%m-%d"
        end_dt = datetime.strptime(end_date_str, fmt).replace(tzinfo=timezone.utc)
    else:
        end_dt = datetime.now(timezone.utc)
    end_ms = int(end_dt.timestamp() * 1000)

    # --------------------------------------------------------------
    # 1. FIRST-TIME SYNC: discover real start and fetch everything
    # --------------------------------------------------------------
    has_csvs = any(f.endswith(".csv") for f in os.listdir(dir_path))
    if not os.path.exists(gotstart) or not has_csvs:
        if os.path.exists(gotstart) and not has_csvs:
            if not polling:
                log_warn(f"{GOTSTART_FILE} exists but no data files — recovering with full sync")
            os.remove(gotstart)
        else:
            if not polling:
                log_warn("First-time sync — discovering genesis candle...")

        genesis_ms = find_genesis_timestamp(ticker, timeframe, polling=polling)
        if not polling:
            genesis_dt = datetime.fromtimestamp(genesis_ms / 1000, tz=timezone.utc)
            log_info(f"Genesis candle: {c_ts(genesis_dt.strftime('%Y-%m-%d %H:%M UTC'))} — starting full sync")

        # Only create .gotstart after writing at least one complete partition
        original_write = write_partition

        def safe_write_partition(df: pd.DataFrame, path: str) -> int:
            result = original_write(df, path)
            part_name = Path(path).stem

            # Create .gotstart once a "complete enough" partition is written
            if not os.path.exists(gotstart):
                create_marker = False
                if timeframe == "1h" and part_name >= "2013-04":    # first full month after genesis
                    create_marker = True
                elif timeframe == "1m" and part_name >= "2013-04-01":
                    create_marker = True
                elif timeframe == "1D" and part_name >= "2014":
                    create_marker = True

                if create_marker:
                    Path(gotstart).touch()
                    if not polling:
                        log_info(f"First complete partition written ({c_file(part_name)}) → created {GOTSTART_FILE}")
            return result

        # Dependency-inject the writer to avoid global monkey-patching
        _fetch_and_save_range(ticker, timeframe, dir_path, genesis_ms, end_ms, polling=polling, write_fn=safe_write_partition)

        # Final fallback: if somehow never triggered, create anyway
        if not os.path.exists(gotstart):
            Path(gotstart).touch()
            if not polling:
                log_info(f"Full sync completed → created {GOTSTART_FILE}")

        if not polling:
            log_success("First-time sync finished successfully")
        return True

    # --------------------------------------------------------------
    # 2. INCREMENTAL SYNC: only run if .gotstart exists
    # --------------------------------------------------------------
    if not polling:
        log_info("Incremental sync — checking for gaps and refreshing current partition")

    # Determine real historical start from existing files
    existing_files = [f for f in os.listdir(dir_path) if f.endswith(".csv")]
    existing = {f[:-4] for f in existing_files}

    if existing_files:
        earliest_name = min(existing_files)[:-4]
        real_start_dt = Partition.from_name(timeframe, earliest_name).start
    else:
        # This should never happen — .gotstart exists → files should exist
        real_start_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)

    needed = {p.name for p in Partition.all_between(timeframe, real_start_dt, end_dt)}
    missing = sorted(needed - existing, key=lambda n: Partition.from_name(timeframe, n).start)

    last_ts = last_timestamp_in_dir(dir_path) or 0

    # Fill missing historical gaps
    for group in _group_consecutive(timeframe, missing):
        first = Partition.from_name(timeframe, group[0])
        last = Partition.from_name(timeframe, group[-1])
        start_ms = min(last_ts + 1, int(first.start.timestamp() * 1000))
        group_end_ms = min(int(last.end.timestamp() * 1000), end_ms)
        if start_ms > group_end_ms:
            continue
        if verbose and not polling:
            log_info(f"Filling gap: {c_file(first.name)} → {c_file(last.name)} ({c_rows(len(group))} partitions)")
        _fetch_and_save_range(ticker, timeframe, dir_path, start_ms, group_end_ms, polling=polling)
        last_ts = last_timestamp_in_dir(dir_path) or last_ts

    # Always refresh current partition
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    current_partition = Partition.from_timestamp(timeframe, now_ms)
    current_csv = os.path.join(dir_path, f"{current_partition.name}.csv")

    refresh_start_ms = int(current_partition.start.timestamp() * 1000)
    if os.path.exists(current_csv):
        try:
            latest = pd.read_csv(current_csv, usecols=["timestamp"])["timestamp"].astype(int).max()
            refresh_start_ms = latest  # inclusive re-fetch
        except Exception:
            pass

    if not polling:
        if refresh_start_ms == int(current_partition.start.timestamp() * 1000):
            log_info(f"Refreshing current partition {c_file(current_partition.name)} (first time)")
        else:
            dt = datetime.fromtimestamp(refresh_start_ms / 1000, tz=timezone.utc)
            log_info(f"Refreshing current partition {c_file(current_partition.name)} from {c_ts(dt.strftime('%Y-%m-%d %H:%M'))} → now")

    _fetch_and_save_range(ticker, timeframe, dir_path, refresh_start_ms, end_ms, polling=polling)

    latest = last_timestamp_in_dir(dir_path)
    if latest and not polling:
        dt = datetime.fromtimestamp(latest / 1000, tz=timezone.utc)
        log_info(f"Latest candle: {c_ts(dt.strftime('%Y-%m-%d %H:%M:%S UTC'))}")

    return True


# ------------------------------ CLI --------------------------------------- #

def main() -> int:
    parser = argparse.ArgumentParser(description="Sync candle data from Bitfinex.")
    parser.add_argument("--exchange", required=True, help="Exchange name, e.g. BITFINEX")
    parser.add_argument("--ticker", required=True, help="Ticker symbol, e.g. tBTCUSD")
    parser.add_argument(
        "--timeframe", required=False, default="1m", choices=sorted(VALID_TIMEFRAMES), help="Timeframe to sync (1m, 1h, 1D)"
    )
    parser.add_argument("--end", required=False, help="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM)")
    parser.add_argument("--polling", action="store_true", help="Compact output intended for polling mode")
    parser.add_argument("--verbose", action="store_true", help="More chatty logs")
    args = parser.parse_args()

    exchange = normalize_exchange(args.exchange)
    target = f"{c_type(exchange)}/{c_var(args.ticker)}/{c_type(args.timeframe)}"
    if args.end:
        log_info(f"Synchronizing {target} → {c_ts(args.end)}")
    else:
        log_info(f"Synchronizing {target}")

    ok = synchronize_candle_data(
        exchange=exchange,
        ticker=args.ticker,
        timeframe=args.timeframe,
        end_date_str=args.end,
        verbose=args.verbose,
        polling=args.polling,
    )
    if ok:
        log_success("Synchronization completed.")
        return 0
    log_error("Synchronization failed.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
