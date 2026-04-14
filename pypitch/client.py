"""
PyPitch API Client SDK
"""

import requests
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin
from requests.exceptions import ConnectionError


class PyPitchClient:
    """Client for interacting with PyPitch API servers."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
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
            self.session.headers.update({
                "X-API-Key": api_key,
                "Authorization": f"Bearer {api_key}",
            })

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a GET request to the API."""
        url = urljoin(self.base_url + '/', endpoint.lstrip('/'))
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def _post(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a POST request to the API."""
        url = urljoin(self.base_url + '/', endpoint.lstrip('/'))
        response = self.session.post(url, json=data, timeout=self.timeout)
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def health_check(self) -> Dict[str, Any]:
        """Check API health status."""
        return self._get("/health")

    def get_metrics(self) -> Dict[str, Any]:
        """Get API and system metrics."""
        return self._get("/v1/metrics")

    def list_matches(self) -> List[Dict[str, Any]]:
        """List all available matches."""
        return self._get("/matches")  # type: ignore[return-value]

    def get_match(self, match_id: str) -> Dict[str, Any]:
        """Get details for a specific match."""
        return self._get(f"/matches/{match_id}")

    def get_player_stats(self, player_id: str) -> Dict[str, Any]:
        """Get statistics for a specific player."""
        return self._get(f"/players/{player_id}")

    def predict_win_probability(
        self,
        target: int,
        current_runs: int,
        wickets_down: int,
        overs_done: float,
        venue: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Predict win probability for a match situation.

        Args:
            target: Target score to chase.
            current_runs: Runs scored so far in the chase.
            wickets_down: Wickets lost so far.
            overs_done: Overs completed so far.
            venue: Optional venue name (informational only).
        """
        query_params: Dict[str, Any] = {
            "target": target,
            "current_runs": current_runs,
            "wickets_down": wickets_down,
            "overs_done": overs_done,
        }
        if venue is not None:
            query_params["venue"] = venue
        return self._get("/win_probability", params=query_params)

    def analyze_custom(
        self, sql: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Run a read-only SELECT query against the ball_events table.

        Args:
            sql: A SELECT statement to execute.
            params: Reserved for future parameterised query support.
        """
        data: Dict[str, Any] = {"sql": sql}
        if params:
            data["params"] = params
        return self._post("/analyze", data)

    def register_live_match(
        self,
        match_id: str,
        source: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Register a match for live ingestion."""
        data: Dict[str, Any] = {"match_id": match_id, "source": source}
        if metadata:
            data["metadata"] = metadata
        return self._post("/live/register", data)

    def ingest_live_delivery(
        self,
        match_id: str,
        inning: int,
        over: int,
        ball: int,
        runs_total: int,
        wickets_fallen: int,
        target: Optional[int] = None,
        venue: Optional[str] = None,
        timestamp: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Ingest live delivery data."""
        data: Dict[str, Any] = {
            "match_id": match_id,
            "inning": inning,
            "over": over,
            "ball": ball,
            "runs_total": runs_total,
            "wickets_fallen": wickets_fallen,
        }
        if target is not None:
            data["target"] = target
        if venue is not None:
            data["venue"] = venue
        if timestamp is not None:
            data["timestamp"] = timestamp
        return self._post("/live/ingest", data)

    def get_live_matches(self) -> List[Dict[str, Any]]:
        """Get list of active live matches."""
        return self._get("/live/matches")  # type: ignore[return-value]


# Convenience functions for quick access
def connect(
    base_url: str = "http://localhost:8000",
    api_key: Optional[str] = None,
    timeout: float = 30.0,
) -> PyPitchClient:
    """Create a PyPitch API client connection."""
    return PyPitchClient(base_url, api_key, timeout)


def quick_health_check(
    base_url: str = "http://localhost:8000",
    api_key: Optional[str] = None,
    timeout: float = 30.0,
) -> bool:
    """Quick health check - returns True if API is healthy."""
    try:
        client = PyPitchClient(base_url, api_key, timeout)
        health = client.health_check()
        return health.get("status") == "healthy"
    except (requests.RequestException, ValueError, ConnectionError):
        return False
