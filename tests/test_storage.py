import pytest
import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq
from storage.data_loader import DataLoader


@pytest.fixture(scope="module")
def sample_transformed_file(tmp_path_factory):
    """
    Create a sample transformed parquet file for testing DataLoader.
    """
    base_dir = tmp_path_factory.mktemp("data_loader_test")
    processed_dir = base_dir / "data" / "processed"
    processed_dir.mkdir(parents=True)

    df = pd.DataFrame({
        "sensor_id": ["s1", "s2"],
        "timestamp": ["2025-06-05T10:00:00", "2025-06-05T11:00:00"],
        "reading_type": ["temperature", "humidity"],
        "value": [25.5, 40.1],
        "battery_level": [90.0, 80.0]
    })
    file_path = processed_dir / "2025-06-05_transformed.parquet"
    df.to_parquet(file_path, index=False)

    return file_path, base_dir


def test_data_loader_creates_partitioned_output(sample_transformed_file):
    """
    Test that DataLoader creates partitioned Parquet dataset (date, sensor_id).
    """
    file_path, base_dir = sample_transformed_file
    analytics_dir = base_dir / "data" / "analytics"

    loader = DataLoader()
    loader.transformed_dir = file_path.parent
    loader.analytics_dir = analytics_dir

    loader.run()

    # ✅ Check: Analytics directory created
    assert analytics_dir.exists(), "Analytics folder not created!"

    # ✅ Check: Partition directories exist
    date_dirs = list(analytics_dir.glob("date=*"))
    assert date_dirs, "No date partitions found!"

    sensor_dirs = list(analytics_dir.glob("date=*/sensor_id=*"))
    assert sensor_dirs, "No sensor_id partitions found!"

    # ✅ Check: Parquet files exist
    parquet_files = list(analytics_dir.rglob("*.parquet"))
    assert parquet_files, "No Parquet files written in analytics!"

    # ✅ Check: It’s actually readable Parquet (not corrupt)
    try:
        pf = pq.ParquetFile(parquet_files[0])
    except Exception as e:
        pytest.fail(f"Parquet file unreadable: {e}")

    # ✅ Check: Compression (snappy)
    compression = pf.metadata.row_group(0).column(0).compression
    assert compression.lower() == "snappy", f"Expected snappy compression, got {compression}"

    # ✅ Check: Expected columns present
    cols = pf.schema.names
    for col in ["timestamp", "value", "reading_type", "battery_level"]:
        assert col in cols, f"Missing column {col} in written parquet!"

    folder_path = parquet_files[0].as_posix()
    assert "date=" in folder_path and "sensor_id=" in folder_path, (
        "Partition folders (date, sensor_id) not found in path!"
    )

    print(f"✅ {len(parquet_files)} parquet files written with {compression} compression under {analytics_dir}")
