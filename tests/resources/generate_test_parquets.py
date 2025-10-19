# tests/resources/generate_test_parquets.py
import pandas as pd
from pathlib import Path
import numpy as np

raw_dir = Path("tests/resources/data/raw")
raw_dir.mkdir(parents=True, exist_ok=True)

# ✅ 1. valid file
df_valid = pd.DataFrame({
    "sensor_id": ["sensor_1", "sensor_2", "sensor_3"],
    "timestamp": ["2025-06-05T10:00:00", "2025-06-05T11:00:00", "2025-06-05T12:00:00"],
    "reading_type": ["temperature", "humidity", "temperature"],
    "value": [25.5, 60.1, 27.3],
    "battery_level": [90.0, 85.0, 88.5]
})
df_valid.to_parquet(raw_dir / "2025-06-05_valid.parquet")

# ❌ 2. missing column (battery_level)
df_missing_col = df_valid.drop(columns=["battery_level"])
df_missing_col.to_parquet(raw_dir / "2025-06-05_missing_col.parquet")

# ⚠️ 3. outlier values
df_outlier = df_valid.copy()
df_outlier.loc[1, "value"] = 999  # outlier humidity
df_outlier.to_parquet(raw_dir / "2025-06-05_outlier.parquet")

# ⚠️ 4. duplicate rows
df_dup = pd.concat([df_valid, df_valid.iloc[[0]]])
df_dup.to_parquet(raw_dir / "2025-06-05_duplicates.parquet")

print("✅ Test parquet files generated under tests/resources/data/raw/")
