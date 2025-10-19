# src/ingestion/data_profiler.py
import duckdb
from pathlib import Path

class DataProfiler:
    def __init__(self):
        pass

    def profile_file(self, file_path):
        """
        Run extended profiling queries on a Parquet file using DuckDB.
        Returns a dict that can be saved as JSON.
        """
        try:
            con = duckdb.connect()

            # Summary by reading_type (existing one)
            summary_query = f"""
                SELECT
                    reading_type,
                    COUNT(*) AS record_count,
                    ROUND(AVG(value), 2) AS avg_value,
                    ROUND(MIN(value), 2) AS min_value,
                    ROUND(MAX(value), 2) AS max_value,
                    ROUND(AVG(battery_level), 2) AS avg_battery,
                    ROUND(MIN(battery_level), 2) AS min_battery,
                    ROUND(MAX(battery_level), 2) AS max_battery
                FROM read_parquet('{file_path}')
                GROUP BY reading_type
            """
            reading_summary = con.execute(summary_query).df().to_dict(orient="records")

            # Missing value summary (column-wise null %)
            missing_query = f"""
                SELECT
                    SUM(sensor_id IS NULL)::DOUBLE / COUNT(*) AS sensor_id_missing_ratio,
                    SUM(timestamp IS NULL)::DOUBLE / COUNT(*) AS timestamp_missing_ratio,
                    SUM(reading_type IS NULL)::DOUBLE / COUNT(*) AS reading_type_missing_ratio,
                    SUM(value IS NULL)::DOUBLE / COUNT(*) AS value_missing_ratio,
                    SUM(battery_level IS NULL)::DOUBLE / COUNT(*) AS battery_missing_ratio
                FROM read_parquet('{file_path}')
            """
            missing_summary = con.execute(missing_query).fetchone()
            missing_cols = ["sensor_id_missing_ratio", "timestamp_missing_ratio", "reading_type_missing_ratio", "value_missing_ratio", "battery_missing_ratio"]
            missing_data = dict(zip(missing_cols, missing_summary))

            # Distinct sensor and reading type count
            distinct_query = f"""
                SELECT
                    COUNT(DISTINCT sensor_id) AS distinct_sensors,
                    COUNT(DISTINCT reading_type) AS distinct_reading_types
                FROM read_parquet('{file_path}')
            """
            distinct_data = con.execute(distinct_query).fetchdf().to_dict(orient="records")[0]

            # Out-of-range detection (based on business rules)
            outlier_query = f"""
                SELECT
                    SUM(value < 0 OR value > 100) AS value_out_of_range,
                    SUM(battery_level < 0 OR battery_level > 100) AS battery_out_of_range
                FROM read_parquet('{file_path}')
            """
            outlier_data = con.execute(outlier_query).fetchdf().to_dict(orient="records")[0]

            # Combine all results
            profile = {
                "file_name": Path(file_path).name,
                "reading_summary": reading_summary,
                "missing_data_ratio": missing_data,
                "distinct_counts": distinct_data,
                "outlier_counts": outlier_data,
            }

            return profile

        except Exception as e:
            return {"error": str(e)}