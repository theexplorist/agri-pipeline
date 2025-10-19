# src/transformation/calibration_service.py
import pandas as pd
import json
from pathlib import Path

class CalibrationService:
    def __init__(self, config_path="config/sensor_config.json"):
        self.config = json.loads(Path(config_path).read_text())

    def apply_calibration(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies per-sensor calibration dynamically from config.
        """
        df = df.copy()
        for rt in df["reading_type"].unique():
            if rt not in self.config:
                continue
            params = self.config[rt].get("calibration", {})
            m, b = params.get("multiplier", 1.0), params.get("offset", 0.0)
            df.loc[df["reading_type"] == rt, "value"] = (
                df.loc[df["reading_type"] == rt, "value"] * m + b
            )
            print(f"⚙️ Applied calibration for {rt}: value = value * {m} + {b}")
        return df