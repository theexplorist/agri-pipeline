# src/transformation/transformation_pipeline.py
import pandas as pd
from pathlib import Path
import os
from transformation.data_cleaner import DataCleaner
from transformation.calibration_service import CalibrationService
from transformation.timestamp_processor import TimestampProcessor
from transformation.feature_engineer import FeatureEngineer

class TransformationPipeline:
    """
    Modular Transformation Pipeline (Production-Grade)
    --------------------------------------------------
    Cleans and deduplicates data
    Applies calibration logic (multiplier + offset)
    Converts timestamps to IST (UTC+5:30)
    Derives features: daily averages, rolling avg, anomaly flags
    Writes transformed Parquet back to data/processed/
    """

    def __init__(self):
        self.cleaner = DataCleaner()
        self.calibrator = CalibrationService()
        self.timeproc = TimestampProcessor()
        self.fe = FeatureEngineer()

        # Use environment variable for flexibility
        self.processed_dir = Path(os.getenv("PROCESSED_DATA_PATH", "data/processed"))
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def run(self, input_path: str, save: bool = True):
        print(f"🚀 Starting transformation for {input_path}")
        df = pd.read_parquet(input_path)

        # --- Step Cleaning ---
        df = self.cleaner.clean(df)

        # --- Step Calibration ---
        df = self.calibrator.apply_calibration(df)

        # --- Step Timestamp Conversion ---
        df = self.timeproc.process(df)

        # --- Step Feature Engineering ---
        df = self.fe.derive_features(df)

        # --- Step Save Output ---
        if save:
            out_path = self.processed_dir / (
                Path(input_path).stem.replace("_processed", "_transformed") + ".parquet"
            )
            df.to_parquet(out_path, index=False)
            print(f"[SUCCESS] Transformed file written to {out_path}")

        return df
