import zipfile
import logging
from pathlib import Path
import json
from typing import List, Dict, Any, Optional
from .adapters.base import BaseAdapter
from .adapters.registry import AdapterRegistry

logger = logging.getLogger(__name__)

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
        self.is_memory = (data_dir == ":memory:")
        self.competition = competition.lower() if competition else None
        self.season = season
        
        if self.is_memory:
            self.data_dir = Path(":memory:")
            self.zip_path = Path(__file__).parent.parent / "data" / "ipl_json.zip"
            self.in_memory_data: Dict[str, Any] = {}
            self._load_zip_to_memory()
        else:
            self.data_dir = Path(data_dir)
            if not self.data_dir.exists():
                raise FileNotFoundError(f"Cricsheet data directory not found: {self.data_dir}")
            self.in_memory_data = {}  # not used in file-backed mode

    def _load_zip_to_memory(self) -> None:
        """Loads and parses all JSON files from the bundled ZIP directly into RAM."""
        if not self.zip_path.exists():
            # Fallback paths
            possible_paths = [
                self.zip_path,
                Path("./data/ipl_json.zip"),
                Path.home() / ".midwicket_data" / "ipl_json.zip"
            ]
            for path in possible_paths:
                if path.exists():
                    self.zip_path = path
                    break
            else:
                logger.warning("Bundled dataset ZIP not found in CricsheetLoader. Running with empty RAM cache.")
                return

        with zipfile.ZipFile(self.zip_path, "r") as z:
            for member in z.namelist():
                if member.endswith(".json") and "/" not in member and "\\" not in member:
                    try:
                        content = z.read(member)
                        match_id = Path(member).stem
                        self.in_memory_data[match_id] = json.loads(content.decode("utf-8"))
                    except Exception as e:
                        logger.warning("Failed to load in-memory match %s: %s", member, e)

    def _match_passes_filter(self, path_or_data: Any) -> bool:
        """Return True if match passes competition/season filters (or no filters set)."""
        if self.competition is None and self.season is None:
            return True
        try:
            if isinstance(path_or_data, dict):
                info = path_or_data.get("info", {})
            else:
                with open(path_or_data, "r", encoding="utf-8") as f:
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
        if self.is_memory:
            return sorted(
                match_id for match_id, data in self.in_memory_data.items()
                if self._match_passes_filter(data)
            )
        return sorted(
            f.stem for f in self.data_dir.glob("*.json")
            if self._match_passes_filter(f)
        )

    def get_match_data(self, match_id: str) -> Dict[str, Any]:
        """
        Loads and normalizes match data for the given match_id from Cricsheet JSON.
        Returns a dict with keys: match_id, format, info, events, raw.
        """
        safe_id = Path(match_id).name
        if safe_id != match_id or "/" in match_id or "\\" in match_id:
            raise ValueError(f"Invalid match_id: {match_id!r}")
            
        if self.is_memory:
            if safe_id not in self.in_memory_data:
                raise FileNotFoundError(f"Match file not found in RAM: {match_id}")
            raw = self.in_memory_data[safe_id]
        else:
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
