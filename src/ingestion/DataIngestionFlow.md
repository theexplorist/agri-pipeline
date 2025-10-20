# Data Ingestion Flow — FR by FR Breakdown

Read and validate daily sensor data from Parquet files in data/raw/, profile and log it, and produce clean files under data/processed/.

## FR 1️⃣ — Reads daily Parquet files from data/raw/

```
File Path Example: data/raw/2025-06-05.parquet
Code Reference: src/ingestion/file_scanner.py

class FileScanner:
    def list_new_files(self):
        all_files = sorted([p.name for p in self.raw_dir.glob("*.parquet")])
        state = self.load_state()
        processed = set(state.get("processed_files", {}).keys())
        new_files = [str(self.raw_dir / f) for f in all_files if f not in processed]
        return new_files

```
### Explanation:

* The FileScanner lists all .parquet files in data/raw/.
* It filters out already processed ones using state/checkpoints.json.
* This ensures only new daily files are picked for ingestion.

## FR 2️⃣ — Supports incremental loading (timestamp-based checkpointing)

```
Code Reference: src/ingestion/ingestion_logger.py
STATE_FILE = "state/checkpoints.json"

state.setdefault("processed_files", {})[filename] = {
    "checksum": checksum,
    "rows": rows,
    "status": status,
    "processed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
}
Path(STATE_FILE).write_text(json.dumps(state, indent=2))
```
### Explanation:
* Each processed file (e.g. 2025-06-05.parquet) is recorded in checkpoints.json with metadata:
```
{
  "processed_files": {
    "2025-06-05.parquet": {
      "checksum": "abc123...",
      "rows": 500,
      "status": "success",
      "processed_at": "2025-06-05T12:30:00Z"
    }
  }
}
```
* On the next run, FileScanner uses this checkpoint to skip already processed files.

## FR 3️⃣ — Uses DuckDB to inspect schema quickly

```
Code Reference: src/ingestion/schema_validator.py
meta = duckdb.query(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").df()
cols = meta["column_name"].tolist()
missing = list(REQUIRED_COLUMNS - set(cols))
```
### Explanation:
```
The schema validator runs a DESCRIBE command using DuckDB to extract column metadata without loading the full file.

It checks if required columns exist:

REQUIRED_COLUMNS = {"sensor_id", "timestamp", "reading_type", "value", "battery_level"}

Missing or extra columns are identified instantly.
Example:
If 2025-06-05_missing_col.parquet is missing battery_level,
then output:

[SCHEMA MISMATCH] missing columns: ['battery_level']
```

## FR 4️⃣ — Run validation queries (e.g., missing columns, data ranges)

```
Code Reference: src/ingestion/data_profiler.py
summary_query = """
SELECT reading_type, COUNT(*) AS record_count,
       ROUND(AVG(value), 2) AS avg_value,
       ROUND(MIN(value), 2) AS min_value,
       ROUND(MAX(value), 2) AS max_value,
       ROUND(AVG(battery_level), 2) AS avg_battery
FROM read_parquet('{file_path}')
GROUP BY reading_type
"""
reading_summary = con.execute(summary_query).df().to_dict(orient="records")
```
### Explanation:
```
Profiles each file with DuckDB aggregations per reading_type.

Detects invalid or extreme ranges for value and battery_level.

Example Output:

Ingestion summary (by reading_type):
  - temperature: count=3, avg=26.1, min=25.5, max=27.3, avg_batt=89.5
  - humidity: count=1, avg=60.1, min=60.1, max=60.1, avg_batt=85.0
```

## FR 5️⃣ — Handles corrupt/unreadable files gracefully

```
Code Reference: src/ingestion/data_reader.py
try:
    df = duckdb.query(f"SELECT * FROM read_parquet('{file_path}')").df()
    return df, None
except Exception as e:
    dest = Path(QUARANTINE_DIR) / Path(file_path).name
    shutil.move(file_path, dest)
    return None, str(e)

```
### Explanation:
```
If DuckDB fails to read a file (corrupted or invalid Parquet), it:

Moves it to data/quarantine/

Returns the error string for logging

Example:

[READ ERROR] Parquet file not readable -> moved to quarantine.
```

## FR 6️⃣ — Logs ingestion summary & stats

```
Code Reference: src/ingestion/ingestion_logger.py
df = pd.DataFrame([{
    "filename": filename,
    "rows": rows,
    "status": status,
    "error": error,
    "duration_sec": duration,
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
}])
df.to_csv("metadata/ingest_log.csv", mode="a", index=False, header=header)

```
### Explanation:
```
Every file ingestion is logged to metadata/ingest_log.csv
Example:

filename	rows	status	error	duration_sec	timestamp
2025-06-05.parquet	1200	success	null	3.12	2025-06-05T12:35Z
```

## FR 7️⃣ — Handles schema mismatches, missing values

```
Code Reference: src/ingestion/ingestion_pipeline.py
if details.get("missing"):
    dest = self.quarantine_dir / Path(file_path).name
    shutil.move(str(file_path), dest)
    print(f"[QUARANTINED] {file_path} -> {dest}")


```
### Explanation:
```
If schema is missing required columns (battery_level, value, etc.),
the file is moved to quarantine instead of processed further.
```

## FR 8️⃣ — Logs number of files, records processed, skipped

```
print(f"Found {len(files)} new file(s) to ingest.")
...
print(f"[SUCCESS] Ingested {len(df)} rows in {duration}s")
```
### Explanation:
```
Found 4 new file(s) to ingest.
[SUCCESS] Ingested 3 rows in 0.02s

```
