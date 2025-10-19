# src/ingestion/data_reader.py
import duckdb
import shutil
from pathlib import Path

QUARANTINE_DIR = "data/quarantine"

class DataReader:
    def __init__(self):
        Path(QUARANTINE_DIR).mkdir(parents=True, exist_ok=True)

    def read_with_duckdb(self, file_path):
        """
        Read Parquet into Pandas via DuckDB. On error move file to quarantine.
        Returns (df, error_str_or_None)
        """
        try:
            df = duckdb.query(f"SELECT * FROM read_parquet('{file_path}')").df()
            return df, None
        except Exception as e:
            # move file to quarantine for later inspection
            dest = Path(QUARANTINE_DIR) / Path(file_path).name
            shutil.move(file_path, dest)
            return None, str(e)