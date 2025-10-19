import duckdb
import pandas as pd
from pathlib import Path
import json
import os
import math


class DataQualityValidator:
    """
    Performs comprehensive data quality checks on transformed sensor data.
    Uses DuckDB for SQL-based validation at scale.

    ✅ Checks performed:
      - Type validation (value and timestamp correctness)
      - Range validation (based on config/sensor_config.json)
      - Missing value percentage per reading_type
      - Time coverage gap detection (hourly, per sensor)
      - Writes aggregated report to metadata/data_quality_report.csv
    """

    def __init__(self):
        # Directory containing transformed files
        self.transformed_dir = Path(os.getenv("TRANSFORMED_DATA_PATH", "data/processed"))

        # Output report file
        self.report_path = Path("metadata/data_quality_report.csv")

        # Load dynamic sensor configuration
        config_path = Path("config/sensor_config.json")
        if config_path.exists():
            self.sensor_config = json.loads(config_path.read_text())
            print(f"✅ Loaded sensor config from {config_path}")
        else:
            print("⚠️ sensor_config.json not found, using empty config.")
            self.sensor_config = {}

        # Reusable DuckDB connection
        self.conn = duckdb.connect()

    # ------------------------------------------------------
    # Run validation for all transformed parquet files
    # ------------------------------------------------------
    def run(self):
        print("🔍 Starting Data Quality Validation...")

        files = list(self.transformed_dir.glob("*_transformed.parquet"))
        if not files:
            print("⚠️ No transformed files found. Run transformation first.")
            return

        reports = []
        for file_path in files:
            print(f"\n📄 Validating {file_path.name} ...")
            report = self.validate_file(file_path)
            report["file_name"] = file_path.name
            reports.append(report)

        # Save the consolidated report
        df_report = pd.DataFrame(reports)
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        df_report.to_csv(self.report_path, index=False)
        print(f"✅ Data quality report written → {self.report_path.resolve()}")
        return df_report

    # ------------------------------------------------------
    # Validate a single transformed file
    # ------------------------------------------------------
    def validate_file(self, file_path):
        try:
            # --- Total Record Count ---
            total_records = self.conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{file_path}')"
            ).fetchone()[0]

            # --- Type Validation ---
            type_check = self.conn.execute(f"""
                SELECT
                    SUM(typeof(value) != 'DOUBLE') AS invalid_value_type,
                    SUM(try_cast(timestamp AS TIMESTAMP) IS NULL) AS invalid_timestamp
                FROM read_parquet('{file_path}');
            """).fetchdf().iloc[0].to_dict()

            # --- Identify which reading_types exist in this file ---
            existing_types = [
                r[0] for r in self.conn.execute(
                    f"SELECT DISTINCT reading_type FROM read_parquet('{file_path}')"
                ).fetchall() if r[0] is not None
            ]

            # --- Range Validation (only for sensors present in the file) ---
            outlier_summary = {}
            for rt in existing_types:
                cfg = self.sensor_config.get(rt, {})
                min_v, max_v = cfg.get("min", -math.inf), cfg.get("max", math.inf)

                result = self.conn.execute(f"""
                    SELECT
                        COUNT(*) FILTER (WHERE reading_type = '{rt}') AS total,
                        COUNT(*) FILTER (
                            WHERE reading_type = '{rt}' AND (value < {min_v} OR value > {max_v})
                        ) AS out_of_range
                    FROM read_parquet('{file_path}');
                """).fetchdf().iloc[0].to_dict()

                total = result.get("total", 0)
                outliers = result.get("out_of_range", 0)
                pct = 0 if not total else round((outliers / total) * 100, 2)
                outlier_summary[rt] = pct

            # --- Missing Value Percentage (per reading_type) ---
            missing_check = self.conn.execute(f"""
                SELECT
                    reading_type,
                    SUM(value IS NULL) AS missing_values,
                    ROUND(100.0 * SUM(value IS NULL) / COUNT(*), 2) AS missing_pct
                FROM read_parquet('{file_path}')
                GROUP BY reading_type;
            """).fetchdf()

            missing_summary = {
                row.reading_type: row.missing_pct for _, row in missing_check.iterrows()
            } if not missing_check.empty else {}

            # --- Time Coverage Gaps (hourly) ---
            gap_check_sql = f"""
                WITH base AS (
                    SELECT
                        sensor_id,
                        MIN(try_cast(timestamp AS TIMESTAMP)) AS min_ts,
                        MAX(try_cast(timestamp AS TIMESTAMP)) AS max_ts
                    FROM read_parquet('{file_path}')
                    GROUP BY sensor_id
                ),
                base_non_null AS (
                    SELECT sensor_id, min_ts, max_ts
                    FROM base
                    WHERE min_ts IS NOT NULL AND max_ts IS NOT NULL AND min_ts < max_ts
                ),
                expected AS (
                    SELECT
                        sensor_id,
                        UNNEST(generate_series(min_ts, max_ts, INTERVAL 1 HOUR)) AS expected_ts
                    FROM base_non_null
                ),
                actual AS (
                    SELECT sensor_id, try_cast(timestamp AS TIMESTAMP) AS ts
                    FROM read_parquet('{file_path}')
                    WHERE try_cast(timestamp AS TIMESTAMP) IS NOT NULL
                ),
                missing_per_sensor AS (
                    SELECT e.sensor_id, COUNT(*) AS missing_hours
                    FROM expected e
                    LEFT JOIN actual a
                      ON e.sensor_id = a.sensor_id AND e.expected_ts = a.ts
                    WHERE a.ts IS NULL
                    GROUP BY e.sensor_id
                )
                SELECT
                    COALESCE(COUNT(*), 0) AS sensors_with_gaps,
                    COALESCE(SUM(missing_hours), 0) AS total_missing_hours
                FROM missing_per_sensor;
            """
            gap_check = self.conn.execute(gap_check_sql).fetchdf().iloc[0].to_dict()

            # --- Build Final Report ---
            report = {
                "total_records": int(total_records),
                "invalid_value_type": int(type_check["invalid_value_type"]),
                "invalid_timestamp": int(type_check["invalid_timestamp"]),
                "outlier_%": json.dumps(outlier_summary),
                "missing_%": json.dumps(missing_summary),
                "sensors_with_gaps": int(gap_check.get("sensors_with_gaps", 0)),
                "total_missing_hours": int(gap_check.get("total_missing_hours", 0)),
            }

            print(f"✅ Validation summary for {file_path.name}:")
            print(json.dumps(report, indent=2))
            return report

        except Exception as e:
            print(f"❌ Validation failed for {file_path.name}: {str(e)}")
            return {"error": str(e), "file_name": Path(file_path).name}