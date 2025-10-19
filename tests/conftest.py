import pytest
from pathlib import Path
import os

@pytest.fixture(autouse=True, scope="function")
def sandbox_env(monkeypatch):
    """
    Automatically sets up sandbox directories for pytest runs.
    Makes sure tests use their own data folders instead of the real ones.
    """
    base = Path("tests/resources/data")
    for d in ["processed", "quarantine"]:
        (base / d).mkdir(parents=True, exist_ok=True)

    # Set environment variables so the pipeline can read test data
    monkeypatch.setenv("RAW_DATA_PATH", str((base / "raw").resolve()))
    monkeypatch.setenv("PROCESSED_DATA_PATH", str((base / "processed").resolve()))
    monkeypatch.setenv("QUARANTINE_DATA_PATH", str((base / "quarantine").resolve()))
    yield
