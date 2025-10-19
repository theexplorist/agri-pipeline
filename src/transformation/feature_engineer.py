# src/transformation/feature_engineer.py
import pandas as pd
import json
from pathlib import Path

class FeatureEngineer:
    def __init__(self, config_path="config/sensor_config.json"):
        self.config = json.loads(Path(config_path).read_text())

    def derive_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["date"] = pd.to_datetime(df["timestamp"]).dt.date

        # 1️⃣ Daily average
        daily_avg = (
            df.groupby(["sensor_id", "reading_type", "date"])["value"]
              .mean()
              .reset_index()
              .rename(columns={"value": "daily_avg"})
        )
        df = df.merge(daily_avg, on=["sensor_id", "reading_type", "date"], how="left")

        # 2️⃣ 7-day rolling average
        df = df.sort_values(["sensor_id", "timestamp"])
        df["rolling_7d_avg"] = (
            df.groupby(["sensor_id", "reading_type"])["value"]
              .rolling(window=7, min_periods=1)
              .mean()
              .reset_index(level=[0,1], drop=True)
        )

        # 3️⃣ Anomalous flag (from config)
        def is_anomalous(row):
            cfg = self.config.get(row["reading_type"], {})
            low, high = cfg.get("min", float("-inf")), cfg.get("max", float("inf"))
            return not (low <= row["value"] <= high)

        df["anomalous_reading"] = df.apply(is_anomalous, axis=1)

        print("✅ Derived features with config-driven thresholds.")
        return df