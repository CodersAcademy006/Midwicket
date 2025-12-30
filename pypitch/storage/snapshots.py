import json
import time
from pathlib import Path
from typing import List, Dict

class SnapshotManager:
    def __init__(self, data_dir: str):
        self.meta_path = Path(data_dir) / "snapshots.json"
        self._load()

    def _load(self):
        if self.meta_path.exists():
            with open(self.meta_path, 'r') as f:
                self.history = json.load(f)
        else:
            self.history = {"snapshots": []}

    def create_snapshot(self, tag: str, description: str = ""):
        """Records a new immutable state of the database."""
        snapshot = {
            "id": tag,
            "timestamp": time.time(),
            "description": description,
            "schema_version": "1.0.0"
        }
        self.history["snapshots"].append(snapshot)
        self._save()

    def _save(self):
        with open(self.meta_path, 'w') as f:
            json.dump(self.history, f, indent=2)

    def get_latest(self) -> str:
        if not self.history["snapshots"]:
            return "initial"
        return self.history["snapshots"][-1]["id"]
