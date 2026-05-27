import json
import time
from pathlib import Path
from typing import List, Dict, Any, cast

class SnapshotManager:
    def __init__(self, data_dir: str):
        self.meta_path = Path(data_dir) / "snapshots.json"
        self.history: Dict[str, List[Dict[str, Any]]] = {"snapshots": []}
        self._load()

    def _load(self) -> None:
        if self.meta_path.exists():
            with open(self.meta_path, 'r') as f:
                self.history = json.load(f)
        else:
            self.history = {"snapshots": []}

    def create_snapshot(self, tag: str, description: str = "") -> None:
        """Records a new immutable state of the database."""
        snapshot = {
            "id": tag,
            "timestamp": time.time(),
            "description": description,
            "schema_version": "1.0.0"
        }
        self.history["snapshots"].append(snapshot)
        self._save()

    def _save(self) -> None:
        import os
        import tempfile
        with tempfile.NamedTemporaryFile(
            mode="w", dir=self.meta_path.parent,
            suffix=".tmp", delete=False,
        ) as tmp:
            json.dump(self.history, tmp, indent=2)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp.name, self.meta_path)

    def get_latest(self) -> str:
        if not self.history["snapshots"]:
            return "initial"
        return str(self.history["snapshots"][-1]["id"])
