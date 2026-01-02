"""
CricsheetLoader: Zero-config loader for Cricsheet cricket data.
"""

from pathlib import Path
import json
from typing import List, Dict, Any
from .adapters.base import BaseAdapter
from .adapters.registry import AdapterRegistry

class CricsheetLoader(BaseAdapter):
    """
    Zero-config loader for Cricsheet JSON data. Discovers and loads matches from a directory.
    Usage:
        loader = CricsheetLoader()
        match_ids = loader.get_match_ids()
        match_data = loader.get_match_data(match_ids[0])
    """
    def __init__(self, data_dir: str = "./data/raw/ipl"):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Cricsheet data directory not found: {self.data_dir}")

    def get_match_ids(self) -> List[str]:
        """
        Returns sorted, deduplicated match IDs for all JSON files in the directory.
        """
        return sorted({f.stem for f in self.data_dir.glob("*.json")})

    def get_match_data(self, match_id: str) -> Dict[str, Any]:
        """
        Loads and normalizes match data for the given match_id from Cricsheet JSON.
        Returns a dict with keys: match_id, format, info, events, raw.
        """
        file_path = self.data_dir / f"{match_id}.json"
        if not file_path.exists():
            raise FileNotFoundError(f"Match file not found: {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        # Minimal normalization: extract info and events
        info = raw.get("info", {})
        events = raw.get("innings", [])
        return {
            "match_id": match_id,
            "format": "cricsheet",
            "info": info,
            "events": events,
            "raw": raw
        }

# Register with AdapterRegistry
AdapterRegistry.register("cricsheet", CricsheetLoader)
