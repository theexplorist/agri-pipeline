# src/ingestion/file_scanner.py
import os
import json
from pathlib import Path

STATE_FILE = "state/checkpoints.json"

class FileScanner:
    def __init__(self, raw_dir="data/raw"):
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        Path("state").mkdir(parents=True, exist_ok=True)
        if not Path(STATE_FILE).exists():
            Path(STATE_FILE).write_text(json.dumps({"processed_files": {}}, indent=2))

    def load_state(self):
        return json.loads(Path(STATE_FILE).read_text())

    def save_state(self, state):
        Path(STATE_FILE).write_text(json.dumps(state, indent=2))

    def list_new_files(self):
        """Return list of full paths for parquet files not yet processed."""
        all_files = sorted([p.name for p in self.raw_dir.glob("*.parquet")])
        state = self.load_state()
        processed = set(state.get("processed_files", {}).keys())
        new_files = [str(self.raw_dir / f) for f in all_files if f not in processed]
        return new_files