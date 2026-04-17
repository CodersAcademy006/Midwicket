"""
PyPitch API Client SDK
"""

import contextlib
import requests
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urljoin
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
        self._closed = False

        if api_key:
            self.session.headers.update({
                "X-API-Key": api_key,
                "Authorization": f"Bearer {api_key}",
            })

    def close(self) -> None:
        """Close the underlying requests session and release pooled sockets."""
        self._close_once()

    def _close_once(self) -> None:
        """Close the session exactly once, even across multiple callers."""
        if self._closed:
            return
        self._closed = True
        self.session.close()

    def __del__(self) -> None:
        """Best-effort cleanup when caller forgets to close explicitly."""
        with contextlib.suppress(Exception):
            self._close_once()

    def __enter__(self) -> "PyPitchClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a GET request to the API."""
        url = urljoin(self.base_url + '/', endpoint.lstrip('/'))
        with self.session.get(url, params=params, timeout=self.timeout) as response:
            response.raise_for_status()
            return response.json()  # type: ignore[no-any-return]

    def _post(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a POST request to the API."""
        url = urljoin(self.base_url + '/', endpoint.lstrip('/'))
        with self.session.post(url, json=data, timeout=self.timeout) as response:
            response.raise_for_status()
            return response.json()  # type: ignore[no-any-return]

    @staticmethod
    def _clean_params(params: Dict[str, Any]) -> Dict[str, Any]:
        """Drop query params that were not explicitly provided."""
        return {key: value for key, value in params.items() if value is not None}

    @staticmethod
    def _path(*segments: Any) -> str:
        """Build a URL-safe path from potentially unsafe user-provided segments."""
        encoded = [quote(str(segment), safe="") for segment in segments]
        return "/" + "/".join(encoded)

    def health_check(self) -> Dict[str, Any]:
        """Check API health status."""
        return self._get("/health")

    def health_check_v1(self) -> Dict[str, Any]:
        """Check API health via the versioned endpoint."""
        return self._get("/v1/health")

    def get_metrics(self) -> Dict[str, Any]:
        """Get API and system metrics."""
        return self._get("/v1/metrics")

    def list_matches(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        venue: Optional[str] = None,
        team: Optional[str] = None,
        sort_by: str = "match_id",
        order: str = "asc",
        page: int = 1,
        page_size: int = 50,
    ) -> Dict[str, Any]:
        """List matches with optional filters and pagination."""
        query_params = self._clean_params(
            {
                "date_from": date_from,
                "date_to": date_to,
                "venue": venue,
                "team": team,
                "sort_by": sort_by,
                "order": order,
                "page": page,
                "page_size": page_size,
            }
        )
        return self._get("/matches", params=query_params)

    def get_match(self, match_id: str) -> Dict[str, Any]:
        """Get details for a specific match."""
        return self._get(self._path("matches", match_id))

    def get_player_stats(self, player_id: str) -> Dict[str, Any]:
        """Get statistics for a specific player."""
        return self._get(self._path("players", player_id))

    def get_team_stats(self, team_id: str) -> Dict[str, Any]:
        """Get statistics for a specific team."""
        return self._get(self._path("teams", team_id))

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
        self, sql: str, params: Optional[List[Any]] = None
    ) -> Dict[str, Any]:
        """Run a read-only SELECT query against the ball_events table.

        Args:
            sql: A SELECT statement to execute.
            params: Positional SQL parameters.
        """
        data: Dict[str, Any] = {"sql": sql}
        if params is not None:
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

    def get_audit_log(self, limit: int = 100) -> Dict[str, Any]:
        """Fetch recent audited analyze queries."""
        return self._get("/v1/audit", params={"limit": limit})

    def resolve_player(self, name: str, match_date: str) -> Dict[str, Any]:
        """Resolve a player name to canonical ID for a specific date."""
        return self._get(
            "/v1/players/resolve",
            params={"name": name, "match_date": match_date},
        )

    def resolve_venue(self, name: str, match_date: str) -> Dict[str, Any]:
        """Resolve a venue name to canonical ID for a specific date."""
        return self._get(
            "/v1/venues/resolve",
            params={"name": name, "match_date": match_date},
        )

    def search_players(self, query: str, limit: int = 10) -> Dict[str, Any]:
        """Search players by alias/name substring."""
        return self._get("/v1/players/search", params={"q": query, "limit": limit})

    def list_venues(self, page: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """List venues with pagination."""
        return self._get("/v1/venues", params={"page": page, "page_size": page_size})

    def get_venue(self, venue_id: int) -> Dict[str, Any]:
        """Get detail payload for one venue."""
        return self._get(self._path("v1", "venues", venue_id))

    def get_matchup(self, batter: str, bowler: str, match_date: str) -> Dict[str, Any]:
        """Get batter vs bowler matchup for a specific date context."""
        return self._get(
            "/v1/matchup",
            params={"batter": batter, "bowler": bowler, "match_date": match_date},
        )

    def get_player_batting(self, player_name: str) -> Dict[str, Any]:
        """Get batting analytics bundle for a player."""
        return self._get(self._path("v1", "players", player_name, "batting"))

    def get_player_bowling(self, player_name: str) -> Dict[str, Any]:
        """Get bowling analytics bundle for a player."""
        return self._get(self._path("v1", "players", player_name, "bowling"))

    def get_player_milestones(self, player_name: str) -> Dict[str, Any]:
        """Get milestone and streak analytics for a player."""
        return self._get(self._path("v1", "players", player_name, "milestones"))

    def get_player_fantasy(self, player_name: str, season: Optional[str] = None) -> Dict[str, Any]:
        """Get fantasy points estimate for a player."""
        return self._get(
            self._path("v1", "players", player_name, "fantasy"),
            params=self._clean_params({"season": season}),
        )

    def get_venue_fantasy(self, venue_name: str) -> Dict[str, Any]:
        """Get venue fantasy cheat sheet and bias."""
        return self._get(self._path("v1", "venues", venue_name, "fantasy"))

    def get_player_vs_team(self, player_name: str, team_name: str) -> Dict[str, Any]:
        """Get a player's batting and bowling output against one team."""
        return self._get(self._path("v1", "players", player_name, "vs-team", team_name))

    def compare_players(self, player_one: str, player_two: str) -> Dict[str, Any]:
        """Compare two players side-by-side."""
        return self._get("/v1/players/compare", params={"p1": player_one, "p2": player_two})

    def batting_leaderboard(
        self,
        sort_by: str = "runs",
        top_n: int = 10,
        min_balls: int = 30,
    ) -> Any:
        """Get batting leaderboard."""
        return self._get(
            "/v1/players/leaderboard/batting",
            params={"sort_by": sort_by, "top_n": top_n, "min_balls": min_balls},
        )

    def bowling_leaderboard(
        self,
        sort_by: str = "wickets",
        top_n: int = 10,
        min_balls: int = 30,
    ) -> Any:
        """Get bowling leaderboard."""
        return self._get(
            "/v1/players/leaderboard/bowling",
            params={"sort_by": sort_by, "top_n": top_n, "min_balls": min_balls},
        )


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
    client = PyPitchClient(base_url, api_key, timeout)
    try:
        health = client.health_check()
        return health.get("status") == "healthy"
    except (requests.RequestException, ValueError, ConnectionError):
        return False
    finally:
        try:
            client.close()
        except Exception:  # nosec B110 - best effort cleanup for health checks
            pass
