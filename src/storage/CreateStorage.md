## Data Loading & Storage

This stage is responsible for the final persistence of validated and transformed sensor data.  
The goal is to store the data in a way that is **optimized for analytical workloads** (e.g., dashboards, queries, machine learning feature extraction).

The process reads all transformed Parquet files, enriches them with partitioning metadata (date and sensor_id), and writes them to a columnar dataset under `data/analytics/`.

---

### FR1 – Store cleaned and transformed data in Parquet format under `data/processed/`

```
Code Reference: src/loading/data_loader.py
transformed_files = list(self.transformed_dir.glob("*_transformed.parquet"))
for file in transformed_files:
    df = pd.read_parquet(file)
```

### Explanation:
```
The DataLoader scans the directory data/processed/ (defined via TRANSFORMED_DATA_PATH) for all _transformed.parquet files.

Each file is loaded into memory as a Pandas DataFrame for post-processing.

This ensures that only the final, cleaned, and validated data produced by the transformation pipeline is used for analytics.

Example:

data/processed/
├── 2025-06-05_transformed.parquet
├── 2025-06-06_transformed.parquet

Each of these files is read and prepared for analytical storage.
```

### FR2 – Optimize for analytical queries using columnar Parquet format

```
table = pa.Table.from_pandas(df)
pq.write_to_dataset(
    table,
    root_path=str(self.analytics_dir),
    partition_cols=["date", "sensor_id"],
    compression="snappy"
)
```

### Explanation:
```
The code converts Pandas DataFrames into Apache Arrow Tables, which are columnar in-memory structures.

These are written to disk as Parquet — a columnar storage format optimized for analytical query engines such as DuckDB, Athena, and Spark.

The dataset is physically partitioned by date and sensor_id, improving query efficiency and parallelism.

Outcome:
The stored structure allows queries like:

SELECT AVG(value)
FROM parquet_scan('data/analytics/')
WHERE date = '2025-06-05' AND reading_type = 'temperature';


to scan only the relevant partitions, significantly improving performance.
```

### FR3 – Partition data by date (and optionally by sensor_id)

```
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df["date"] = df["timestamp"].dt.date
pq.write_to_dataset(
    table,
    root_path=str(self.analytics_dir),
    partition_cols=["date", "sensor_id"],
    compression="snappy"
)
```

### Explanation:
```
Each record’s timestamp is converted into a date column.

The pyarrow.parquet.write_to_dataset() function automatically creates subdirectories for each partition.

This improves analytical filtering and incremental refresh capabilities.

Example:
After running the loader, the output folder structure looks like this:

data/analytics/
├── date=2025-06-05/
│   ├── sensor_id=s1/
│   │   └── part-0.parquet
│   ├── sensor_id=s2/
│   │   └── part-0.parquet
├── date=2025-06-06/
│   ├── sensor_id=s1/
│   │   └── part-0.parquet


This allows analytical engines to skip entire directories when filtering by date or sensor_id.
```

### FR4 – Apply compression (Snappy or Gzip)

```
pq.write_to_dataset(
    table,
    root_path=str(self.analytics_dir),
    partition_cols=["date", "sensor_id"],
    compression="snappy"
)
```

### Explanation:
```
The Parquet files are written using Snappy compression by default.

Snappy offers a balance between fast compression/decompression and minimal storage overhead.

This reduces disk usage and improves query performance during reads.

Example:

Uncompressed size: ~200 MB

With Snappy: ~70–80 MB

Outcome:
The dataset is efficiently stored and optimized for both speed and cost.
```

### FR5 – Handle missing timestamps gracefully

```
if "timestamp" in df.columns:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = df["timestamp"].dt.date
else:
    print(f"⚠️ Missing timestamp column in {file.name}, skipping date partitioning.")
    df["date"] = "unknown"
```

### Explanation:
```
If a file lacks a timestamp column, the system doesn’t fail.

It assigns the partition column date = 'unknown' to ensure the file is still stored but logically separated.

Outcome:
All transformed files are saved, even if partially incomplete, preserving traceability.
```

### FR6 – Persist the final dataset under data/analytics/

```
self.analytics_dir = Path(os.getenv("ANALYTICS_DATA_PATH", "data/analytics"))
self.analytics_dir.mkdir(parents=True, exist_ok=True)
```

### Explanation:
```
The analytics directory (data/analytics/) acts as the single source of truth for downstream use.

It’s automatically created if missing.

Once written, analysts and other systems can query this dataset directly using analytical engines like DuckDB, Athena, or PySpark.

Example Output Path:

data/analytics/date=2025-06-05/sensor_id=s1/part-0.parquet
```
