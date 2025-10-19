import json
from pathlib import Path
import numpy as np
import pandas as pd

class DataCleaner:
    def __init__(self, config_path="config/sensor_config.json"):
        # Load min/max thresholds from JSON config
        self.thresholds = json.loads(Path(config_path).read_text())

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans sensor data:
        1. Drop duplicates & missing rows
        2. Fill numeric NaNs
        3. Detect and correct outliers:
           • if enough data → Z-score
           • else → config-range fallback
        """
        df = df.drop_duplicates(subset=["sensor_id", "timestamp", "reading_type"])
        df = df.dropna(subset=["sensor_id", "timestamp", "reading_type"])

        for col in ["value", "battery_level"]:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].mean())

        # --- Outlier detection ---
        for rt in df["reading_type"].unique():
            mask = df["reading_type"] == rt
            vals = df.loc[mask, "value"]

            # Get range limits from config
            limits = self.thresholds.get(rt, {"min": -np.inf, "max": np.inf})
            low, high = limits["min"], limits["max"]

            if len(vals) < 5:
                # Too few points → range-based correction
                outliers = (vals < low) | (vals > high)
                if outliers.any():
                    df.loc[mask & outliers, "value"] = np.clip(vals, low, high)
                    print(f"⚠️ {rt}: small sample fallback; clipped {outliers.sum()} values.")
                continue

            # Normal case → Z-score
            z = (vals - vals.mean()) / vals.std(ddof=0)
            outliers = abs(z) > 3
            if outliers.any():
                df.loc[mask & outliers, "value"] = vals.median()
                print(f"🔧 {rt}: corrected {outliers.sum()} outliers via Z-score >3")

        print("✅ Cleaning complete.")
        return df