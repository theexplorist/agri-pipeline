# Data Transfomation Flow — FR by FR Breakdown

The transformation stage processes cleaned Parquet files produced by the ingestion phase.  
It focuses on cleaning, calibration, timestamp normalization, feature derivation, and anomaly detection.

The main driver of this stage is `TransformationPipeline`, which orchestrates all transformation submodules.
## FR1 – Data Cleaning: Drop duplicates

```
**Code Reference:** src/transformation/data_cleaner.py
df = df.drop_duplicates(subset=["sensor_id", "timestamp", "reading_type"])
```
### Explanation:
```
Ensures each unique sensor reading (sensor_id, timestamp, reading_type) appears only once.
Prevents duplicate sensor records caused by ingestion retries or device duplication.
Example:
Before:

sensor_id	timestamp	reading_type	value
s1	2025-06-05T10:00:00	temperature	25.0
s1	2025-06-05T10:00:00	temperature	25.0

After cleaning → only one record retained.
```

## FR2 – Handle missing values (fill or drop)
```
Code Reference: DataCleaner.clean()

df = df.dropna(subset=["sensor_id", "timestamp", "reading_type"])
for col in ["value", "battery_level"]:
    df[col] = df[col].fillna(df[col].mean())
```
### Explanation
```
Drops rows missing key identifiers (sensor_id, timestamp, or reading_type).

Replaces missing numeric fields (value, battery_level) with column means.

Example:
Before:

sensor_id	timestamp	value	battery_level
s1	2025-06-05T10:00:00	NaN	90
s2	2025-06-05T10:15:00	22.5	NaN

After:

sensor_id	timestamp	value	battery_level
s1	2025-06-05T10:00:00	22.5	90
s2	2025-06-05T10:15:00	22.5	90
```

## FR3 – Detect and correct outliers (Z-score > 3 or range-based)
```
Code Reference: DataCleaner.clean()

z = (vals - vals.mean()) / vals.std(ddof=0)
outliers = abs(z) > 3
df.loc[mask & outliers, "value"] = vals.median()
```
### Explanation
```
For each reading type (e.g., temperature, humidity):

Calculates the Z-score to find outliers (|z| > 3).

Replaces those outliers with the median value.

For small datasets (fewer than 5 samples), applies config-based safe clipping instead of z-score.

Example:
Before:

sensor_id	reading_type	value
s1	temperature	25.0
s2	temperature	26.0
s3	temperature	999.0

After correction:

sensor_id	reading_type	value
s1	temperature	25.0
s2	temperature	26.0
s3	temperature	25.5
```

## FR4 – Normalize using calibration logic
```
Code Reference: src/transformation/calibration_service.py

for rt in df["reading_type"].unique():
    params = self.config[rt].get("calibration", {})
    m, b = params.get("multiplier", 1.0), params.get("offset", 0.0)
    df.loc[df["reading_type"] == rt, "value"] = df.loc[df["reading_type"] == rt, "value"] * m + b
```
### Explanation
```
Reads calibration constants dynamically from config/sensor_config.json.

Adjusts each reading based on per-sensor calibration parameters:

value = raw_value * multiplier + offset


Example:
If calibration for temperature is { "multiplier": 1.02, "offset": 0.5 }:

Raw value = 25.0 → Calibrated value = 25.0 * 1.02 + 0.5 = 26.0


Outcome: Adjusted readings reflect sensor-specific calibration.
```

## FR5 – Timestamp processing (convert to ISO 8601)
```
Code Reference: src/transformation/timestamp_processor.py

df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
```
### Explanation
```
Converts timestamps into a standardized ISO 8601 format (YYYY-MM-DDTHH:MM:SS).

Handles alternate formats such as "06/05/2025 10:00 AM" gracefully.

Invalid timestamps are dropped from the dataset.

Example:
Before:

timestamp
06/05/2025 10:00 AM

After:

timestamp
2025-06-05T10:00:00
```

## FR6 – Adjust timestamps to UTC+5:30 (IST)
```
Code Reference: TimestampProcessor.process()

df["timestamp_ist"] = (
    df["timestamp"] + timedelta(hours=5, minutes=30)
).dt.strftime("%Y-%m-%dT%H:%M:%S")
```
### Explanation
```
Adds 5 hours 30 minutes to convert UTC time into Indian Standard Time.

Stores result in a new column timestamp_ist.

Example:
If timestamp = 2025-06-05T10:00:00 (UTC),
then timestamp_ist = 2025-06-05T15:30:00.
```

## FR6 – Adjust timestamps to UTC+5:30 (IST)
```
Code Reference: TimestampProcessor.process()

df["timestamp_ist"] = (
    df["timestamp"] + timedelta(hours=5, minutes=30)
).dt.strftime("%Y-%m-%dT%H:%M:%S")
```
### Explanation
```
Adds 5 hours 30 minutes to convert UTC time into Indian Standard Time.

Stores result in a new column timestamp_ist.

Example:
If timestamp = 2025-06-05T10:00:00 (UTC),
then timestamp_ist = 2025-06-05T15:30:00.
```

## FR7 – Derived fields: Daily average per sensor and reading type
```
Code Reference: src/transformation/feature_engineer.py

daily_avg = (
    df.groupby(["sensor_id", "reading_type", "date"])["value"]
      .mean()
      .reset_index()
      .rename(columns={"value": "daily_avg"})
)
df = df.merge(daily_avg, on=["sensor_id", "reading_type", "date"], how="left")
```
### Explanation
```
Calculates the daily average value for each (sensor_id, reading_type) combination.

Merges the average back into the main DataFrame as a new column daily_avg.

Example:

sensor_id	reading_type	date	value	daily_avg
s1	temperature	2025-06-05	26.5	26.0
s1	temperature	2025-06-05	25.5	26.0
```

## FR8 – Derived fields: 7-day rolling average
```
Code Reference: FeatureEngineer.derive_features()

df["rolling_7d_avg"] = (
    df.groupby(["sensor_id", "reading_type"])["value"]
      .rolling(window=7, min_periods=1)
      .mean()
      .reset_index(level=[0,1], drop=True)
)
```
### Explanation
```
Calculates rolling 7-day mean for each sensor and reading type.

Handles partial data (minimum of 1 record) to avoid missing early-day values.

Example:

date	value	rolling_7d_avg
2025-06-01	24	24.0
2025-06-02	26	25.0
2025-06-03	25	25.0
```

## FR9 – Anomaly detection flag
```
Code Reference: FeatureEngineer.derive_features()

def is_anomalous(row):
    cfg = self.config.get(row["reading_type"], {})
    low, high = cfg.get("min", float("-inf")), cfg.get("max", float("inf"))
    return not (low <= row["value"] <= high)

df["anomalous_reading"] = df.apply(is_anomalous, axis=1)
```
### Explanation
```
Fetches expected value ranges from config/sensor_config.json.

Flags readings outside configured min/max as anomalies.

Example Config:

"temperature": { "min": 0, "max": 50 }


Example Output:

value	reading_type	anomalous_reading
25.0	temperature	False
80.0	temperature	True
```

## FR10 – Pipeline Orchestration
```
Code Reference: src/transformation/transformation_pipeline.py

df = self.cleaner.clean(df)
df = self.calibrator.apply_calibration(df)
df = self.timeproc.process(df)
df = self.fe.derive_features(df)
```
### Explanation
```
The TransformationPipeline class coordinates all transformation steps in order:

Cleans data (DataCleaner)

Applies calibration (CalibrationService)

Normalizes timestamps (TimestampProcessor)

Generates features and anomaly flags (FeatureEngineer)

Output:
Transformed Parquet files are written under data/processed/ with _transformed suffix.

Example:

data/processed/2025-06-05_transformed.parquet

Example Transformation Flow

Input:

sensor_id	timestamp	reading_type	value	battery_level
s1	2025-06-05T10:00:00	temperature	25.0	90
s2	2025-06-05T11:00:00	humidity	999.0	85

Output:

sensor_id	timestamp	reading_type	value	battery_level	timestamp_ist	daily_avg	rolling_7d_avg	anomalous_reading
s1	2025-06-05T10:00:00	temperature	26.0	90	2025-06-05T15:30:00	26.0	26.0	False
s2	2025-06-05T11:00:00	humidity	980.0	85	2025-06-05T16:30:00	980.0	980.0	True
```
