"""Client SDK endpoint wiring tests."""

from typing import Any, Dict, Optional

import pytest

from pypitch.client import PyPitchClient


def test_list_matches_builds_expected_query_params(monkeypatch: pytest.MonkeyPatch) -> None:
    client = PyPitchClient()
    captured: Dict[str, Any] = {}

    def fake_get(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        captured["endpoint"] = endpoint
        captured["params"] = params
        return {"items": [], "total": 0, "page": 2, "page_size": 25}

    monkeypatch.setattr(client, "_get", fake_get)

    payload = client.list_matches(
        date_from="2024-01-01",
        date_to="2024-12-31",
        venue="Wankhede",
        team="MI",
        sort_by="date",
        order="desc",
        page=2,
        page_size=25,
    )

    assert captured["endpoint"] == "/matches"
    assert captured["params"] == {
        "date_from": "2024-01-01",
        "date_to": "2024-12-31",
        "venue": "Wankhede",
        "team": "MI",
        "sort_by": "date",
        "order": "desc",
        "page": 2,
        "page_size": 25,
    }
    assert payload["page"] == 2


def test_analyze_custom_uses_positional_params(monkeypatch: pytest.MonkeyPatch) -> None:
    client = PyPitchClient()
    captured: Dict[str, Any] = {}

    def fake_post(endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        captured["endpoint"] = endpoint
        captured["data"] = data
        return {"rows": 0, "data": []}

    monkeypatch.setattr(client, "_post", fake_post)

    client.analyze_custom("SELECT * FROM ball_events WHERE match_id = ?", params=["1234"])

    assert captured["endpoint"] == "/analyze"
    assert captured["data"] == {
        "sql": "SELECT * FROM ball_events WHERE match_id = ?",
        "params": ["1234"],
    }


@pytest.mark.parametrize(
    ("method_name", "kwargs", "expected_endpoint", "expected_params"),
    [
        ("get_audit_log", {"limit": 50}, "/v1/audit", {"limit": 50}),
        (
            "resolve_player",
            {"name": "V Kohli", "match_date": "2024-05-18"},
            "/v1/players/resolve",
            {"name": "V Kohli", "match_date": "2024-05-18"},
        ),
        (
            "resolve_venue",
            {"name": "Wankhede Stadium", "match_date": "2024-05-18"},
            "/v1/venues/resolve",
            {"name": "Wankhede Stadium", "match_date": "2024-05-18"},
        ),
        (
            "search_players",
            {"query": "koh", "limit": 5},
            "/v1/players/search",
            {"q": "koh", "limit": 5},
        ),
        (
            "list_venues",
            {"page": 3, "page_size": 20},
            "/v1/venues",
            {"page": 3, "page_size": 20},
        ),
        (
            "get_matchup",
            {"batter": "V Kohli", "bowler": "JJ Bumrah", "match_date": "2024-05-18"},
            "/v1/matchup",
            {"batter": "V Kohli", "bowler": "JJ Bumrah", "match_date": "2024-05-18"},
        ),
        (
            "compare_players",
            {"player_one": "V Kohli", "player_two": "RG Sharma"},
            "/v1/players/compare",
            {"p1": "V Kohli", "p2": "RG Sharma"},
        ),
        (
            "batting_leaderboard",
            {"sort_by": "average", "top_n": 15, "min_balls": 100},
            "/v1/players/leaderboard/batting",
            {"sort_by": "average", "top_n": 15, "min_balls": 100},
        ),
        (
            "bowling_leaderboard",
            {"sort_by": "economy", "top_n": 20, "min_balls": 120},
            "/v1/players/leaderboard/bowling",
            {"sort_by": "economy", "top_n": 20, "min_balls": 120},
        ),
    ],
)
def test_v1_wrappers_call_expected_routes(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    kwargs: Dict[str, Any],
    expected_endpoint: str,
    expected_params: Dict[str, Any],
) -> None:
    client = PyPitchClient()
    captured: Dict[str, Any] = {}

    def fake_get(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        captured["endpoint"] = endpoint
        captured["params"] = params
        return {"ok": True}

    monkeypatch.setattr(client, "_get", fake_get)

    method = getattr(client, method_name)
    result = method(**kwargs)

    assert captured["endpoint"] == expected_endpoint
    assert captured["params"] == expected_params
    assert result == {"ok": True}


def test_path_based_player_and_venue_wrappers(monkeypatch: pytest.MonkeyPatch) -> None:
    client = PyPitchClient()
    seen_endpoints: list[str] = []
    seen_params: list[Optional[Dict[str, Any]]] = []

    def fake_get(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        seen_endpoints.append(endpoint)
        seen_params.append(params)
        return {"ok": True}

    monkeypatch.setattr(client, "_get", fake_get)

    client.get_venue(7)
    client.get_player_batting("V Kohli")
    client.get_player_bowling("V Kohli")
    client.get_player_milestones("V Kohli")
    client.get_player_fantasy("V Kohli")
    client.get_player_fantasy("V Kohli", season="2024")
    client.get_venue_fantasy("Wankhede Stadium")
    client.get_player_vs_team("V Kohli", "Chennai Super Kings")

    assert seen_endpoints == [
        "/v1/venues/7",
        "/v1/players/V%20Kohli/batting",
        "/v1/players/V%20Kohli/bowling",
        "/v1/players/V%20Kohli/milestones",
        "/v1/players/V%20Kohli/fantasy",
        "/v1/players/V%20Kohli/fantasy",
        "/v1/venues/Wankhede%20Stadium/fantasy",
        "/v1/players/V%20Kohli/vs-team/Chennai%20Super%20Kings",
    ]
    assert seen_params == [
        None,
        None,
        None,
        None,
        {},
        {"season": "2024"},
        None,
        None,
    ]


def test_path_wrappers_encode_reserved_characters(monkeypatch: pytest.MonkeyPatch) -> None:
    client = PyPitchClient()
    seen_endpoints: list[str] = []

    def fake_get(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        seen_endpoints.append(endpoint)
        return {"ok": True}

    monkeypatch.setattr(client, "_get", fake_get)

    client.get_match("ipl/2024?semi")
    client.get_player_vs_team("A/B", "KKR?A")

    assert seen_endpoints == [
        "/matches/ipl%2F2024%3Fsemi",
        "/v1/players/A%2FB/vs-team/KKR%3FA",
    ]
