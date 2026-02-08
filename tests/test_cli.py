"""CLI regression tests for error handling and sync boundaries."""

import sys
from datetime import datetime, timezone

from candles_sync.adapters.bitfinex import BitfinexAdapter
from candles_sync.candles_sync import Partition
import candles_sync.candles_sync as cs


class TestMainErrorHandling:
    def test_main_unknown_exchange_returns_error_without_traceback(self, monkeypatch, capsys):
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "candles-sync",
                "--exchange", "NOPE",
                "--ticker", "BTCUSD",
                "--timeframe", "1m",
            ],
        )

        rc = cs.main()
        out = capsys.readouterr().out

        assert rc == 1
        assert "No adapter registered for exchange 'NOPE'" in out
        assert "Traceback" not in out

    def test_main_invalid_end_returns_error_without_traceback(self, monkeypatch, capsys, tmp_path):
        monkeypatch.setattr(cs, "ROOT_PATH", tmp_path / ".corky")
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "candles-sync",
                "--exchange", "BITFINEX",
                "--ticker", "tBTCUSD",
                "--timeframe", "1m",
                "--end", "2024-99-99",
            ],
        )

        rc = cs.main()
        out = capsys.readouterr().out

        assert rc == 1
        assert "does not match format '%Y-%m-%d'" in out
        assert "Traceback" not in out


class TestIncrementalRefreshBoundary:
    def test_refresh_start_clamped_to_current_partition_start(self, monkeypatch, tmp_path):
        timeframe = "1m"
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        current_partition = Partition.from_timestamp(timeframe, now_ms)
        current_start_ms = int(current_partition.start.timestamp() * 1000)
        end_dt = datetime.now(timezone.utc)
        end_ms = int(end_dt.timestamp() * 1000)

        csv_path = tmp_path / f"{current_partition.name}.csv"
        csv_path.write_text(
            "timestamp,open,close,high,low,volume\n"
            f"{current_start_ms},1,1,1,1,0\n"
        )

        calls = []

        def _capture_fetch(*args, **kwargs):
            calls.append((args[4], args[5]))

        monkeypatch.setattr(cs, "_fetch_and_save_range", _capture_fetch)

        ok = cs._incremental_sync(
            adapter=BitfinexAdapter(),
            symbol="tBTCUSD",
            timeframe=timeframe,
            dir_path=str(tmp_path),
            end_dt=end_dt,
            end_ms=end_ms,
            verbose=False,
            polling=False,
            debug=False,
        )

        assert ok is True
        assert len(calls) == 1
        refresh_start_ms, _ = calls[0]
        assert refresh_start_ms == current_start_ms
