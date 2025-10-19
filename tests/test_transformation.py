# tests/test_transformation.py
import pandas as pd
from pathlib import Path
import os
from transformation.transformation_pipeline import TransformationPipeline

# ✅ Keep the environment consistent with production structure
os.environ["PROCESSED_DATA_PATH"] = "tests/resources/data/processed"

tp = TransformationPipeline()

# ---------------------------------------------------------------------
# UNIT TESTS (Your exact version)
# ---------------------------------------------------------------------

def test_cleaning_removes_duplicates():
    df = pd.DataFrame({
        "sensor_id": ["s1", "s1"],
        "timestamp": ["2025-06-05T10:00:00", "2025-06-05T10:00:00"],
        "reading_type": ["temperature", "temperature"],
        "value": [25.5, 25.5],
        "battery_level": [90.0, 90.0]
    })
    cleaned = tp.cleaner.clean(df)
    assert len(cleaned) == 1


def test_calibration_applies_correctly():
    df = pd.DataFrame({
        "sensor_id": ["s1"],
        "timestamp": ["2025-06-05T10:00:00"],
        "reading_type": ["temperature"],
        "value": [25.0],
        "battery_level": [90.0]
    })
    calibrated = tp.calibrator.apply_calibration(df)
    expected_value = 25.0 * 1.02 + 0.5
    assert abs(calibrated.iloc[0]["value"] - expected_value) < 0.001


def test_timestamp_conversion():
    import pandas as pd
    from transformation.transformation_pipeline import TransformationPipeline

    tp = TransformationPipeline()

    df = pd.DataFrame({
        "sensor_id": ["s1", "s2", "s3"],
        "timestamp": ["06/05/2025 10:00 AM", "2025-06-05T10:15:00", "INVALID_TS"],
        "reading_type": ["temperature", "humidity", "temperature"],
        "value": [25.0, 30.0, 50.0],
        "battery_level": [90.0, 85.0, 95.0]
    })

    processed = tp.timeproc.process(df)

    # Column existence checks
    assert "timestamp" in processed.columns, "Missing normalized timestamp column"
    assert "timestamp_ist" in processed.columns, "Missing IST-adjusted timestamp column"

    # ISO format check (T separator)
    assert all("T" in ts for ts in processed["timestamp"]), "Timestamps not ISO formatted"

    # IST conversion check (5h30m shift)
    assert "15:30" in processed.iloc[0]["timestamp_ist"] or "15:45" in processed.iloc[1]["timestamp_ist"]

    # Invalid timestamps dropped
    assert len(processed) == 2, "Invalid timestamps were not dropped correctly"


def test_anomaly_flagging():
    df = pd.DataFrame({
        "sensor_id": ["s1"],
        "timestamp": ["2025-06-05T10:00:00"],
        "reading_type": ["temperature"],
        "value": [200.0],
        "battery_level": [90.0]
    })
    enriched = tp.fe.derive_features(df)
    assert bool(enriched.iloc[0]["anomalous_reading"]) is True


# ---------------------------------------------------------------------
# UPDATED INTEGRATION TEST (processes all files)
# ---------------------------------------------------------------------

def test_pipeline_creates_transformed_files():
    """
    Ensures the full pipeline writes transformed files for all processed inputs.
    Mirrors production flow (loops through all processed files).
    """
    processed_dir = Path(os.getenv("PROCESSED_DATA_PATH", "tests/resources/data/processed"))
    input_files = list(processed_dir.glob("*_processed.parquet"))
    assert input_files, "No processed files found for transformation"

    tp = TransformationPipeline()
    transformed_files = []

    for file in input_files:
        print(f"🚀 Transforming {file.name}")
        tp.run(str(file), save=True)

    # collect outputs
    out_files = list(processed_dir.glob("*_transformed.parquet"))
    print(f"[DEBUG] Transformed output files: {[f.name for f in out_files]}")

    # check: one transformed file per processed file
    assert len(out_files) == len(input_files), (
        f"Expected {len(input_files)} transformed files, found {len(out_files)}"
    )

    # verify the transformed file schema has enrichment columns
    sample = pd.read_parquet(out_files[0])
    expected_cols = {"sensor_id", "timestamp", "value", "battery_level", "anomalous_reading"}
    assert expected_cols.issubset(set(sample.columns)), "Transformed schema missing expected columns"
