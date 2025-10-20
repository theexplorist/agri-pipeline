# Data Quality Validation (Using DuckDB)

The validation stage acts as a quality gate after transformation.  
It ensures that data written to `data/processed/` meets expected quality standards in terms of type correctness, value ranges, completeness, and temporal consistency.

All validations are performed using **DuckDB SQL queries**, which enable efficient in-memory analytics without loading large datasets into Python.

---

## FR1 – Validate types (e.g., `value` is float, `timestamp` is ISO format)
```
Code Reference: src/validation/data_quality_validator.py
type_check = self.conn.execute(f"""
    SELECT
        SUM(typeof(value) != 'DOUBLE') AS invalid_value_type,
        SUM(try_cast(timestamp AS TIMESTAMP) IS NULL) AS invalid_timestamp
    FROM read_parquet('{file_path}');
""").fetchdf().iloc[0].to_dict()
```
### Explanation
```
This query verifies that:

All value fields are stored as floating-point (DOUBLE) types.

All timestamps can be cast into valid TIMESTAMP format.

DuckDB’s try_cast() safely handles invalid timestamp values (e.g., malformed strings) without breaking the pipeline.

Example:

timestamp	value	validation
2025-06-05T10:00:00	25.5	valid
invalid_date	20.1	invalid_timestamp
2025-06-05T12:00:00	"abc"	invalid_value_type

Outcome: Invalid timestamps and values are counted, but processing continues.
```

# Data Quality Validation (Using DuckDB)

The validation stage acts as a quality gate after transformation.  
It ensures that data written to `data/processed/` meets expected quality standards in terms of type correctness, value ranges, completeness, and temporal consistency.

All validations are performed using **DuckDB SQL queries**, which enable efficient in-memory analytics without loading large datasets into Python.

---

## FR2 – Check expected value ranges per reading_type
```
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
```
### Explanation
```
The system loads thresholds dynamically from config/sensor_config.json, e.g.:

{
  "temperature": { "min": 0, "max": 50 },
  "humidity": { "min": 0, "max": 100 }
}


For each sensor type present in the file, DuckDB counts how many readings fall outside the configured range.

The result is stored in the outlier_% column of the report.

Example:
If humidity values are expected between 0–100, and a file has 5 outliers among 200 readings →
outlier_% = 2.5

Outcome: Reports out-of-range readings as percentage per sensor type.
```

## FR3 – Detect gaps in hourly data (using DuckDB generate_series)
```
gap_check_sql = f"""
WITH base AS (
    SELECT
        sensor_id,
        MIN(try_cast(timestamp AS TIMESTAMP)) AS min_ts,
        MAX(try_cast(timestamp AS TIMESTAMP)) AS max_ts
    FROM read_parquet('{file_path}')
    GROUP BY sensor_id
),
expected AS (
    SELECT sensor_id,
           UNNEST(generate_series(min_ts, max_ts, INTERVAL 1 HOUR)) AS expected_ts
    FROM base
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

```
### Explanation
```
This query:

Computes each sensor’s time range (min_ts, max_ts).

Uses generate_series() to simulate hourly expected readings.

Joins actual timestamps against expected ones to detect missing hours.

Example:

sensor_id	expected hours	missing hours
s1	24	2
s2	24	0

Outcome:
Summarized as:

sensors_with_gaps = 1
total_missing_hours = 2


This quantifies temporal completeness.
```

## FR4 – Profile missing values per reading_type
```
missing_check = self.conn.execute(f"""
    SELECT
        reading_type,
        SUM(value IS NULL) AS missing_values,
        ROUND(100.0 * SUM(value IS NULL) / COUNT(*), 2) AS missing_pct
    FROM read_parquet('{file_path}')
    GROUP BY reading_type;
""").fetchdf()
```
### Explanation
```
Calculates percentage of missing numeric readings (value IS NULL) for each sensor type.

Produces a dictionary such as:

{ "temperature": 0.0, "humidity": 3.25 }


Example Output:
If 3 out of 100 humidity readings are null → missing_% = 3.0

Outcome: Quantifies data completeness for each measurement type.
```

## FR5 – Generate anomaly and range summary (outlier and missing%)
```
report = {
    "total_records": int(total_records),
    "invalid_value_type": int(type_check["invalid_value_type"]),
    "invalid_timestamp": int(type_check["invalid_timestamp"]),
    "outlier_%": json.dumps(outlier_summary),
    "missing_%": json.dumps(missing_summary),
    "sensors_with_gaps": int(gap_check.get("sensors_with_gaps", 0)),
    "total_missing_hours": int(gap_check.get("total_missing_hours", 0)),
}
```
### Explanation
```
Consolidates all validation metrics into a single structured report.

Each record in the final CSV represents one validated file, summarizing:

Data type errors

Range violations

Missing percentages

Time coverage gaps

Example:

file_name	total_records	invalid_value_type	invalid_timestamp	outlier_%	missing_%	sensors_with_gaps	total_missing_hours
2025-06-05_transformed.parquet	1200	0	0	{"temperature": 0.0, "humidity": 1.5}	{"temperature": 0.0, "humidity": 2.0}	1	3

Outcome: Unified report for downstream review or dashboarding.
```

## FR6 – Save validation output as data_quality_report.csv
```
df_report = pd.DataFrame(reports)
self.report_path.parent.mkdir(parents=True, exist_ok=True)
df_report.to_csv(self.report_path, index=False)
```
### Explanation
```
After validating all transformed files, the results are concatenated into a single DataFrame and saved as a CSV file under:

metadata/data_quality_report.csv


Example CSV Content:

file_name,total_records,invalid_value_type,invalid_timestamp,outlier_%,missing_%,sensors_with_gaps,total_missing_hours
2025-06-05_transformed.parquet,1200,0,0,"{""temperature"":0.0,""humidity"":1.5}","{""temperature"":0.0,""humidity"":2.0}",1,3


Outcome:
The validation output serves as a data quality dashboard, allowing easy tracking of data consistency and completeness across days.
```
