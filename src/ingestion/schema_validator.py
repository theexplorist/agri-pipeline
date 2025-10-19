# src/ingestion/schema_validator.py
import duckdb

REQUIRED_COLUMNS = {"sensor_id", "timestamp", "reading_type", "value", "battery_level"}

class SchemaValidator:
    def __init__(self):
        pass

    def inspect_schema(self, file_path):
        """
        Use DuckDB to read Parquet metadata quickly and return column names.
        Returns: (valid: bool, details: dict)
        """
        try:
            # DESCRIBE SELECT * FROM read_parquet(...) returns metadata about columns
            meta = duckdb.query(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").df()
            cols = [c for c in meta["column_name"].tolist()]
            missing = list(REQUIRED_COLUMNS - set(cols))
            extra = list(set(cols) - REQUIRED_COLUMNS)
            return True, {"columns": cols, "missing": missing, "extra": extra}
        except Exception as e:
            return False, {"error": str(e)}