import pytest
import pandas as pd
from pathlib import Path
import os

# ✅ Set test-specific paths before importing the pipeline
os.environ["RAW_DATA_PATH"] = "tests/resources/data/raw"
os.environ["PROCESSED_DATA_PATH"] = "tests/resources/data/processed"
os.environ["QUARANTINE_DATA_PATH"] = "tests/resources/data/quarantine"

from ingestion.ingestion_pipeline import IngestionPipeline


@pytest.fixture(scope="module")
def ingestion_pipeline():
    return IngestionPipeline()


def test_ingestion_valid_file(ingestion_pipeline):
    dfs = ingestion_pipeline.run()

    # ✅ Use env-aware processed directory
    processed_dir = Path(os.getenv("PROCESSED_DATA_PATH"))
    files = list(processed_dir.glob("*.parquet"))
    print(f"DEBUG processed files -> {files}")

    assert any("2025-06-05" in f.name for f in files), "No processed file found!"
    assert isinstance(dfs, list)
    assert all("sensor_id" in df.columns for df in dfs)


def test_ingestion_missing_column():
    # ✅ Use env-aware quarantine directory
    quarantine_dir = Path(os.getenv("QUARANTINE_DATA_PATH"))
    files = list(quarantine_dir.glob("*missing_col*"))
    print(f"DEBUG quarantined files -> {files}")

    assert len(files) > 0, "Missing column file was not quarantined!"


def test_ingestion_outlier_detection():
    processed_dir = Path(os.getenv("PROCESSED_DATA_PATH"))
    files = list(processed_dir.glob("*.parquet"))
    print(f"DEBUG processed for outlier -> {files}")

    assert any("outlier" in f.name for f in files), "Outlier file missing in processed dir!"


def test_ingestion_checkpoint_file_exists():
    # ✅ state/checkpoints.json (your ingestion_logger writes this)
    state_file = Path("state/checkpoints.json")
    print(f"DEBUG checking {state_file.resolve()}")
    assert state_file.exists(), "Checkpoint file not created!"
