"""
PyPitch API Client SDK
"""

import requests
from typing import Any
from urllib.parse import urljoin
import json
from requests.exceptions import ConnectionError

class PyPitchClient:
    """Client for interacting with PyPitch API servers."""

    def __init__(self, base_url: str = "http://localhost:8000", api_key: str | None = None, timeout: float = 30.0) -> None:
        """
        Initialize the PyPitch API client.

        Args:
            base_url: Base URL of the PyPitch API server
            api_key: API key for authentication (if required)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()

        if api_key:
            self.session.headers.update({"X-API-Key": api_key})

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a GET request to the API."""
        url = urljoin(self.base_url + '/', endpoint.lstrip('/'))
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _post(self, endpoint: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a POST request to the API."""
        url = urljoin(self.base_url + '/', endpoint.lstrip('/'))
        response = self.session.post(url, json=data, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def health_check(self) -> dict[str, Any]:
        """Check API health status."""
        return self._get("/health")

    def get_metrics(self) -> dict[str, Any]:
        """Get API and system metrics."""
        return self._get("/v1/metrics")

    def list_matches(self) -> list[dict[str, Any]]:
        """List all available matches."""
        return self._get("/matches")

    def get_match(self, match_id: str) -> dict[str, Any]:
        """Get details for a specific match."""
        return self._get(f"/matches/{match_id}")

    def get_player_stats(self, player_id: str) -> dict[str, Any]:
        """Get statistics for a specific player."""
        return self._get(f"/players/{player_id}")

    def predict_win_probability(self, venue: str, target: int, current_score: int,
                              wickets_down: int, overs_remaining: float) -> dict[str, Any]:
        """Predict win probability for a match situation."""
        params = {
            "venue": venue,
            "target": target,
            "current_score": current_score,
            "wickets_down": wickets_down,
            "overs_remaining": overs_remaining
        }
        return self._get("/win_probability", params=params)

    def analyze_custom(self, query: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Run custom analysis query."""
        data = {"query": query}
        if params:
            data["params"] = params
        return self._post("/analyze", data)

    def register_live_match(self, match_id: str, source: str,
                           metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        """Register a match for live ingestion."""
        data = {
            "match_id": match_id,
            "source": source
        }
        if metadata:
            data["metadata"] = metadata
        return self._post("/live/register", data)

    def ingest_live_delivery(self, match_id: str, inning: int, over: int, ball: int,
                           runs_total: int, wickets_fallen: int, target: int | None = None,
                           venue: str | None = None, timestamp: float | None = None) -> dict[str, Any]:
        """Ingest live delivery data."""
        data = {
            "match_id": match_id,
            "inning": inning,
            "over": over,
            "ball": ball,
            "runs_total": runs_total,
            "wickets_fallen": wickets_fallen
        }
        if target is not None:
            data["target"] = target
        if venue is not None:
            data["venue"] = venue
        if timestamp is not None:
            data["timestamp"] = timestamp
        return self._post("/live/ingest", data)

    def get_live_matches(self) -> list[dict[str, Any]]:
        """Get list of active live matches."""
        return self._get("/live/matches")

# Convenience functions for quick access
def connect(base_url: str = "http://localhost:8000", api_key: str | None = None, timeout: float = 30.0) -> PyPitchClient:
    """Create a PyPitch API client connection."""
    return PyPitchClient(base_url, api_key, timeout)

def quick_health_check(base_url: str = "http://localhost:8000", api_key: str | None = None, timeout: float = 30.0) -> bool:
    """Quick health check - returns True if API is healthy."""
    try:
        client = PyPitchClient(base_url, api_key, timeout)
        health = client.health_check()
        return health.get("status") == "healthy"
    except (requests.RequestException, ValueError, ConnectionError):
        return False