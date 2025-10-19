# src/ingestion/ingestion_pipeline.py
import time
import duckdb
import os
import shutil
from pathlib import Path
from ingestion.file_scanner import FileScanner
from ingestion.schema_validator import SchemaValidator
from ingestion.data_reader import DataReader
from ingestion.ingestion_logger import IngestionLogger
from ingestion.data_profiler import DataProfiler


class IngestionPipeline:
    """
    Production-grade ingestion pipeline:
    - Scans for new Parquet files
    - Validates schema
    - Profiles data (summary, missing, outliers)
    - Reads into DataFrame
    - Logs ingestion metadata
    """

    def __init__(self):
        # Environment-aware folders (pytest fixture will override these)
        self.raw_dir = Path(os.getenv("RAW_DATA_PATH", "data/raw"))
        self.processed_dir = Path(os.getenv("PROCESSED_DATA_PATH", "data/processed"))
        self.quarantine_dir = Path(os.getenv("QUARANTINE_DATA_PATH", "data/quarantine"))

        # Ensure folders exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

        # Core components
        self.scanner = FileScanner(self.raw_dir)
        self.validator = SchemaValidator()
        self.reader = DataReader()
        self.logger = IngestionLogger()
        self.profiler = DataProfiler()

    # ---------------------------------------------------------------------- #
    def run(self):
        print(f"DEBUG PATHS -> raw {self.raw_dir}, processed {self.processed_dir}, quarantine {self.quarantine_dir}")
        files = self.scanner.list_new_files()
        print(f"Found {len(files)} new file(s) to ingest.")
        results = []

        for file_path in files:
            print(f"\n--- Processing {file_path} ---")
            start = time.time()

            # Schema Validation
            ok, details = self.validator.inspect_schema(file_path)
            if not ok:
                duration = round(time.time() - start, 2)
                print(f"[SCHEMA ERROR] {details.get('error')}")
                # Move file to quarantine
                dest = self.quarantine_dir / Path(file_path).name
                shutil.move(str(file_path), dest)
                print(f"[QUARANTINED] {file_path} -> {dest}")
                self.logger.update_checkpoint_and_log(file_path, 0, "schema_error", str(details), duration)
                continue

            if details.get("missing"):
                duration = round(time.time() - start, 2)
                print(f"[SCHEMA MISMATCH] missing columns: {details['missing']}")
                # Move file to quarantine
                dest = self.quarantine_dir / Path(file_path).name
                shutil.move(str(file_path), dest)
                print(f"[QUARANTINED] {file_path} -> {dest}")
                self.logger.update_checkpoint_and_log(file_path, 0, "schema_mismatch", f"missing:{details['missing']}", duration)
                continue

            # Profiling
            summary = self.profiler.profile_file(file_path)
            if "error" in summary:
                duration = round(time.time() - start, 2)
                print(f"[PROFILING ERROR] {summary['error']}")
                self.logger.update_checkpoint_and_log(file_path, 0, "profiling_error", summary["error"], duration)
                continue

            # Print summary
            if "reading_summary" in summary:
                print("Ingestion summary (by reading_type):")
                for row in summary["reading_summary"]:
                    print(f"  - {row['reading_type']}: count={row['record_count']}, "
                          f"avg={row['avg_value']}, min={row['min_value']}, "
                          f"max={row['max_value']}, avg_batt={row['avg_battery']}")

            # Read data
            df, error = self.reader.read_with_duckdb(file_path)
            duration = round(time.time() - start, 2)
            if error:
                print(f"[READ ERROR] {error} -> moved to quarantine.")
                dest = self.quarantine_dir / Path(file_path).name
                shutil.move(str(file_path), dest)
                print(f"[QUARANTINED] {file_path} -> {dest}")
                self.logger.update_checkpoint_and_log(file_path, 0, "read_error", error, duration)
                continue

            # Basic content validation
            missing_sensor = int(df["sensor_id"].isnull().sum()) if "sensor_id" in df.columns else 0
            missing_ts = int(df["timestamp"].isnull().sum()) if "timestamp" in df.columns else 0
            missing_value = int(df["value"].isnull().sum()) if "value" in df.columns else 0

            if missing_sensor or missing_ts or missing_value:
                warn_msg = f"missing sensor:{missing_sensor}, ts:{missing_ts}, value:{missing_value}"
                print(f"[DATA QUALITY WARNING] {warn_msg}")
                self.logger.update_checkpoint_and_log(file_path, len(df), "dq_warning", warn_msg, duration, extra_summary=summary)
            else:
                self.logger.update_checkpoint_and_log(file_path, len(df), "success", None, duration, extra_summary=summary)

            # Save cleaned file
            processed_path = self.processed_dir / f"{Path(file_path).stem}_processed.parquet"
            df.to_parquet(processed_path, index=False)
            print(f"[SAVED] Clean file written to {processed_path}")
            print(f"[SUCCESS] Ingested {len(df)} rows in {duration}s")

            results.append(df)

        return results