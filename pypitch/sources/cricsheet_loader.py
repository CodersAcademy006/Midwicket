"""
CricsheetLoader: Zero-config loader for Cricsheet cricket data.
"""

from pathlib import Path
import json
from typing import List, Dict, Any, Optional
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
    def __init__(self, data_dir: str = "./data/raw/ipl",
                 competition: Optional[str] = None,
                 season: Optional[int] = None):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Cricsheet data directory not found: {self.data_dir}")
        self.competition = competition.lower() if competition else None
        self.season = season

    def _match_passes_filter(self, path: Path) -> bool:
        """Return True if file matches competition/season filters (or no filters set)."""
        if self.competition is None and self.season is None:
            return True
        try:
            with open(path, "r", encoding="utf-8") as f:
                info = json.load(f).get("info", {})
        except (OSError, ValueError):
            return False
        if self.competition is not None:
            event_name = (info.get("event", {}).get("name", "") or "").lower()
            # Direct substring match OR abbreviation match (e.g. "ipl" → "Indian Premier League")
            abbrev = "".join(w[0] for w in event_name.split() if w)
            if self.competition not in event_name and self.competition != abbrev:
                return False
        if self.season is not None:
            dates = info.get("dates", [])
            if not dates:
                return False
            try:
                match_year = int(str(dates[0])[:4])
            except (ValueError, IndexError):
                return False
            if match_year != self.season:
                return False
        return True

    def get_match_ids(self) -> List[str]:
        """
        Returns sorted, deduplicated match IDs for all JSON files in the directory,
        optionally filtered by competition and season.
        """
        return sorted(
            f.stem for f in self.data_dir.glob("*.json")
            if self._match_passes_filter(f)
        )

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
