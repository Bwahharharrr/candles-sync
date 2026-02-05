"""Shared test fixtures for candles-sync tests."""

import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_dir(tmp_path):
    """Provide a temporary directory for test data."""
    return tmp_path


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file with candle data."""
    path = tmp_path / "test.csv"
    path.write_text(
        "timestamp,open,close,high,low,volume\n"
        "1609459200000,29000,29100,29200,28900,100\n"
        "1609545600000,29100,29200,29300,29000,150\n"
    )
    return str(path)


@pytest.fixture
def empty_csv(tmp_path):
    """Create a CSV file with only headers."""
    path = tmp_path / "empty.csv"
    path.write_text("timestamp,open,close,high,low,volume\n")
    return str(path)


@pytest.fixture(autouse=True)
def env_cleanup():
    """Ensure tests don't leak environment variables."""
    old_token = os.environ.get("EODHD_API_TOKEN")
    yield
    if old_token is not None:
        os.environ["EODHD_API_TOKEN"] = old_token
    elif "EODHD_API_TOKEN" in os.environ:
        del os.environ["EODHD_API_TOKEN"]
