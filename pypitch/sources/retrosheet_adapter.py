"""
RetrosheetAdapter: Pre-built adapter for Retrosheet baseball data (example for extensibility).
"""



from pathlib import Path
import csv
from typing import List, Dict, Any, Optional, DefaultDict
from collections import defaultdict

FILE_PRIORITY = [".csv", ".tsv", ".evn", ".evt"]
SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".evn", ".evt"}


from .adapters.base import BaseAdapter
from .adapters.registry import AdapterRegistry

class RetrosheetAdapter:
    AdapterRegistry.register("retrosheet", RetrosheetAdapter)
    """
    Adapter for Retrosheet event and tabular files. Handles discovery, parsing, and normalization.
    """
    def __init__(self, data_dir: str = "./data/retrosheet"):
        self.data_dir = Path(data_dir)

    def get_match_ids(self) -> List[str]:
        """
        Returns sorted, deduplicated match IDs for all supported files.
        """
        return sorted({
            f.stem for f in self.data_dir.iterdir()
            if f.suffix.lower() in SUPPORTED_EXTENSIONS
        })

    def get_match_data(self, match_id: str) -> Dict[str, Any]:
        """
        Loads match data for the given match_id, dispatching by file type.
        Returns a normalized dict with match_id, format, info, events.
        """
        for ext in FILE_PRIORITY:
            path = self.data_dir / f"{match_id}{ext}"
            if path.exists():
                if ext in {".csv", ".tsv"}:
                    return self._load_tabular(path, ext)
                else:
                    return self._load_retrosheet_raw(path)
        raise FileNotFoundError(f"No data found for match_id={match_id}")

    def _load_tabular(self, path: Path, ext: str) -> Dict[str, Any]:
        delimiter = "\t" if ext == ".tsv" else ","
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            rows = list(reader)
        if not rows:
            raise ValueError(f"No events found in {path}")
        return {
            "match_id": path.stem,
            "format": "tabular",
            "info": {},
            "events": rows
        }

    @staticmethod
    def parse_retrosheet_event_file(file_path: Path) -> Dict[str, Any]:
        """
        Parses a Retrosheet event file (.EVN/.EVT) into a normalized dict with typed event buckets.
        """
        game: Dict[str, Any] = {
            "match_id": None,
            "info": {},
            "events": defaultdict(list)  # type: DefaultDict[str, list]
        }
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(',')
                record_type = parts[0]
                if record_type == "id":
                    game["match_id"] = parts[1]
                elif record_type == "info":
                    key, value = parts[1], parts[2]
                    game["info"][key] = value
                else:
                    game["events"][record_type].append(parts)
        if game["match_id"] is None:
            raise ValueError("Missing game id record")
        # Convert events to dict for output contract
        return {
            "match_id": game["match_id"],
            "format": "retrosheet_raw",
            "info": game["info"],
            "events": dict(game["events"])
        }

    def _load_retrosheet_raw(self, path: Path) -> Dict[str, Any]:
        data = self.parse_retrosheet_event_file(path)
        if not data["events"]:
            raise ValueError(f"No events found in {path}")
        return data
