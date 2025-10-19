# src/ingestion/ingestion_logger.py
import json
import hashlib
import time
from pathlib import Path
import pandas as pd

STATE_FILE = "state/checkpoints.json"
LOG_FILE = "metadata/ingest_log.csv"

class IngestionLogger:
    def __init__(self):
        Path("metadata").mkdir(parents=True, exist_ok=True)
        Path("state").mkdir(parents=True, exist_ok=True)
        if not Path(STATE_FILE).exists():
            Path(STATE_FILE).write_text(json.dumps({"processed_files": {}}, indent=2))

    def _checksum(self, path):
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def update_checkpoint_and_log(self, file_path, rows, status, error=None, duration=0.0, extra_summary=None):
        """
        Persist checkpoint and append a CSV log row.
        extra_summary can hold DuckDB aggregated summary (pandas DF or dict)
        """
        filename = Path(file_path).name
        checksum = None
        try:
            checksum = self._checksum(file_path)
        except Exception:
            # If file moved to quarantine previously, checksum may fail; that's ok.
            checksum = None

        # update checkpoints.json
        state = json.loads(Path(STATE_FILE).read_text())
        state.setdefault("processed_files", {})[filename] = {
            "checksum": checksum,
            "rows": rows,
            "status": status,
            "error": error,
            "processed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "duration_sec": duration
        }
        Path(STATE_FILE).write_text(json.dumps(state, indent=2))

        # append a CSV row
        df = pd.DataFrame([{
            "filename": filename,
            "checksum": checksum,
            "rows": rows,
            "status": status,
            "error": error,
            "duration_sec": duration,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }])
        header = not Path(LOG_FILE).exists()
        df.to_csv(LOG_FILE, mode="a", index=False, header=header)

        # optionally return or persist extra_summary
        if extra_summary is not None:
            # store per-file summary as JSON sidecar if provided
            try:
                summary_path = Path("metadata") / f"{filename}.summary.json"
                summary_path.write_text(json.dumps(extra_summary, indent=2, default=str))
            except Exception:
                pass