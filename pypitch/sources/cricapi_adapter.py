"""
CricAPIAdapter: Pre-built adapter for CricAPI cricket data (example for extensibility).
"""


import requests

from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter, Retry
from .adapters.base import BaseAdapter
from .adapters.registry import AdapterRegistry

class CricAPIAdapter(BaseAdapter):
    AdapterRegistry.register("cricapi", CricAPIAdapter)
    """
    Adapter for CricAPI cricket data. Normalizes output, validates responses, and matches the PyPitch adapter contract.
    """
    def __init__(self, api_key: str, base_url: str = "https://cricapi.com/api/"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/") + "/"
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    def get_match_ids(self) -> List[str]:
        """
        Fetches a list of match IDs from CricAPI.
        Returns a list of match_id strings.
        """
        url = self.base_url + "matches"
        params = {"apikey": self.api_key}
        resp = self.session.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if "matches" not in data:
            raise ValueError("Invalid CricAPI response: missing 'matches'")
        match_ids = []
        for match in data["matches"]:
            match_id = str(match.get("unique_id"))
            if match_id:
                match_ids.append(match_id)
        return match_ids

    def get_match_data(self, match_id: str) -> Dict[str, Any]:
        """
        Fetches and normalizes detailed match data for a given match_id from CricAPI.
        Returns a dict with keys: match_id, format, info, events, raw.
        """
        url = self.base_url + "cricketScore"
        params = {"apikey": self.api_key, "unique_id": match_id}
        resp = self.session.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        # Validate and normalize
        info = {}
        events = []
        # Extract metadata if present
        if "stat" in data:
            info["stat"] = data["stat"]
        if "score" in data:
            info["score"] = data["score"]
        if "description" in data:
            info["description"] = data["description"]
        # Improved event normalization
        if "score" in data or "stat" in data:
            events.append({
                "type": "match_state",
                "score": data.get("score"),
                "stat": data.get("stat"),
                "description": data.get("description")
            })
        if not info and not events:
            raise ValueError(f"Invalid CricAPI response for match_id={match_id}")
        return {
            "match_id": match_id,
            "format": "cricapi",
            "info": info,
            "events": events,
            "raw": data
        }
