# src/transformation/timestamp_processor.py
import pandas as pd
from datetime import timedelta

class TimestampProcessor:
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize timestamp to ISO 8601
        Adjust to UTC+5:30 (IST)
        Drop invalid timestamps safely
        """

        df = df.copy()

        def try_parse(ts):
            # Try ISO first
            try:
                return pd.to_datetime(ts, errors="raise")
            except Exception:
                # Try common alternate format: e.g., 06/05/2025 10:00 AM
                try:
                    return pd.to_datetime(ts, format="%m/%d/%Y %I:%M %p", errors="raise")
                except Exception:
                    return pd.NaT

        # Apply custom parser row-wise
        df["timestamp"] = df["timestamp"].apply(try_parse)

        # Drop invalid timestamps
        invalid_rows = df["timestamp"].isna().sum()
        if invalid_rows > 0:
            print(f"[WARN] Dropping {invalid_rows} rows with invalid timestamps.")
            df = df.dropna(subset=["timestamp"])

        # Convert to IST
        df["timestamp_ist"] = (
            df["timestamp"] + timedelta(hours=5, minutes=30)
        ).dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Normalize to ISO 8601 string
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")

        print(f"🕓 {len(df)} valid timestamps normalized successfully.")
        return df
