import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import os


class DataLoader:
    """
    Stage 4: Data Loading & Storage Optimization
    - Reads transformed Parquet files
    - Adds partition columns (date, sensor_id)
    - Writes optimized Parquet dataset with compression & partitioning
    """

    def __init__(self):
        self.transformed_dir = Path(os.getenv("TRANSFORMED_DATA_PATH", "data/processed"))
        self.analytics_dir = Path(os.getenv("ANALYTICS_DATA_PATH", "data/analytics"))
        self.analytics_dir.mkdir(parents=True, exist_ok=True)

    def run(self):
        print("📦 Starting Data Loading & Storage Optimization...")

        transformed_files = list(self.transformed_dir.glob("*_transformed.parquet"))
        if not transformed_files:
            print("⚠️ No transformed files found.")
            return

        for file in transformed_files:
            print(f"\n🔹 Loading {file.name}")
            df = pd.read_parquet(file)

            # Ensure timestamp is parsed
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                df["date"] = df["timestamp"].dt.date
            else:
                print(f"⚠️ Missing timestamp column in {file.name}, skipping date partitioning.")
                df["date"] = "unknown"

            # Convert to Arrow table for optimized write
            table = pa.Table.from_pandas(df)

            # Partition by date and sensor_id
            print("🧩 Writing partitioned Parquet dataset (date, sensor_id)...")
            pq.write_to_dataset(
                table,
                root_path=str(self.analytics_dir),
                partition_cols=["date", "sensor_id"],
                compression="snappy"
            )

        print(f"✅ All transformed data stored under {self.analytics_dir.resolve()}")
