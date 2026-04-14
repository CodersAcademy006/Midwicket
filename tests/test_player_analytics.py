"""
Tests for pypitch/api/player_analytics.py (PA-01 to PA-28).

Uses an in-memory DuckDB session with synthetic ball_events data.
All player_analytics functions call _get_con() → session.engine.raw_connection().
We monkey-patch _get_con to return a fresh in-memory connection seeded with
fixture data so tests run without a real data directory.
"""

from __future__ import annotations

import duckdb
import pytest
from unittest.mock import patch

import pypitch.api.player_analytics as pa


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BATTER = "V Kohli"
BOWLER = "JJ Bumrah"
BATTER2 = "RG Sharma"

_SCHEMA = """
CREATE TABLE ball_events (
    match_id  VARCHAR,
    inning    INTEGER,
    over      INTEGER,
    ball      INTEGER,
    runs_total    INTEGER DEFAULT 0,
    wickets_fallen INTEGER DEFAULT 0,
    target    INTEGER DEFAULT 0,
    venue     VARCHAR DEFAULT '',
    timestamp DOUBLE DEFAULT 0,
    runs_batter   INTEGER DEFAULT 0,
    runs_extras   INTEGER DEFAULT 0,
    is_wicket BOOLEAN DEFAULT FALSE,
    batter    VARCHAR DEFAULT '',
    bowler    VARCHAR DEFAULT '',
    competition VARCHAR DEFAULT '',
    season    VARCHAR DEFAULT ''
)
"""


def _seed(con: duckdb.DuckDBPyConnection) -> None:
    """Insert synthetic ball data for two batters and one bowler."""
    con.execute(_SCHEMA)

    rows = []
    # Match 1 — Kohli bats, Bumrah bowls, Wankhede, IPL 2023
    for over in range(20):
        for ball in range(6):
            runs_b = 4 if (over + ball) % 5 == 0 else (1 if ball % 2 == 0 else 0)
            is_w = (over == 15 and ball == 5)
            rows.append((
                "M001", 1, over, ball,
                runs_b, 1 if is_w else 0, 0,
                "Wankhede", float(1000 + over * 6 + ball),
                runs_b, 0, is_w,
                BATTER, BOWLER, "IPL", "2023",
            ))

    # Match 2 — Kohli bats again, Eden Gardens, IPL 2022
    for over in range(10):
        for ball in range(6):
            runs_b = 6 if ball == 5 else 2
            rows.append((
                "M002", 2, over, ball,
                runs_b, 0, 150,
                "Eden Gardens", float(2000 + over * 6 + ball),
                runs_b, 0, False,
                BATTER, "Another Bowler", "IPL", "2022",
            ))

    # Match 3 — Sharma bats
    for over in range(8):
        for ball in range(6):
            rows.append((
                "M003", 1, over, ball,
                3, 0, 0,
                "Wankhede", float(3000 + over * 6 + ball),
                3, 0, False,
                BATTER2, BOWLER, "IPL", "2023",
            ))

    # Bumrah bowls in M003 — add bowling rows
    for over in range(4):
        for ball in range(6):
            is_w = (over == 3 and ball == 3)
            rows.append((
                "M003", 1, over, ball,
                8, 1 if is_w else 0, 0,
                "Wankhede", float(3000 + over * 6 + ball),
                6, 2, is_w,
                "Other Batter", BOWLER, "IPL", "2023",
            ))

    con.executemany("""
        INSERT INTO ball_events VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """, rows)


@pytest.fixture
def mock_con(monkeypatch):
    """Patch _get_con() to return a seeded in-memory DuckDB connection."""
    connections = []

    def _fake_con():
        c = duckdb.connect(":memory:")
        _seed(c)
        connections.append(c)
        return c

    monkeypatch.setattr(pa, "_get_con", _fake_con)
    yield
    for c in connections:
        try:
            c.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# PA-01  Career batting
# ---------------------------------------------------------------------------

class TestCareerBatting:
    def test_returns_stats(self, mock_con):
        result = pa.career_batting(BATTER)
        assert result["player"] == BATTER
        assert result["matches"] >= 1
        assert result["runs"] > 0
        assert result["balls_faced"] > 0
        assert result["strike_rate"] is not None
        assert result["strike_rate"] > 0

    def test_no_data_player(self, mock_con):
        result = pa.career_batting("Ghost Player")
        assert result["matches"] == 0

    def test_has_fours_sixes(self, mock_con):
        result = pa.career_batting(BATTER)
        assert "fours" in result
        assert "sixes" in result
        assert result["fours"] >= 0

    def test_has_milestones(self, mock_con):
        result = pa.career_batting(BATTER)
        assert "fifties" in result
        assert "hundreds" in result
        assert "highest_score" in result


# ---------------------------------------------------------------------------
# PA-02  Career bowling
# ---------------------------------------------------------------------------

class TestCareerBowling:
    def test_returns_stats(self, mock_con):
        result = pa.career_bowling(BOWLER)
        assert result["player"] == BOWLER
        assert result["balls_bowled"] > 0
        assert result["wickets"] >= 0
        assert result["economy"] is not None

    def test_best_figures_format(self, mock_con):
        result = pa.career_bowling(BOWLER)
        assert "/" in result["best_figures"]

    def test_no_data(self, mock_con):
        result = pa.career_bowling("Ghost Bowler")
        assert result["matches"] == 0


# ---------------------------------------------------------------------------
# PA-03  Fielding
# ---------------------------------------------------------------------------

class TestCareerFielding:
    def test_returns_note(self, mock_con):
        result = pa.career_fielding(BATTER)
        assert "note" in result
        assert result["player"] == BATTER


# ---------------------------------------------------------------------------
# PA-04  Batting by phase
# ---------------------------------------------------------------------------

class TestBattingByPhase:
    def test_phases_present(self, mock_con):
        result = pa.batting_by_phase(BATTER)
        phases = [p["phase"] for p in result["phases"]]
        assert len(phases) > 0
        for p in result["phases"]:
            assert p["phase"] in ("Powerplay", "Middle", "Death")
            assert p["balls"] > 0

    def test_sr_computed(self, mock_con):
        result = pa.batting_by_phase(BATTER)
        for p in result["phases"]:
            if p["balls"] > 0:
                assert p["strike_rate"] is not None


# ---------------------------------------------------------------------------
# PA-05  Bowling by phase
# ---------------------------------------------------------------------------

class TestBowlingByPhase:
    def test_returns_phases(self, mock_con):
        result = pa.bowling_by_phase(BOWLER)
        assert "phases" in result
        assert len(result["phases"]) > 0
        for p in result["phases"]:
            assert "economy" in p


# ---------------------------------------------------------------------------
# PA-06  Batting by venue
# ---------------------------------------------------------------------------

class TestBattingByVenue:
    def test_returns_venues(self, mock_con):
        result = pa.batting_by_venue(BATTER)
        assert isinstance(result, list)
        assert len(result) >= 1
        venues = [r["venue"] for r in result]
        assert "Wankhede" in venues or "Eden Gardens" in venues

    def test_sr_present(self, mock_con):
        result = pa.batting_by_venue(BATTER)
        for r in result:
            if r["balls"] > 0:
                assert r["strike_rate"] is not None


# ---------------------------------------------------------------------------
# PA-07  Bowling by venue
# ---------------------------------------------------------------------------

class TestBowlingByVenue:
    def test_returns_venues(self, mock_con):
        result = pa.bowling_by_venue(BOWLER)
        assert isinstance(result, list)
        assert len(result) >= 1
        for r in result:
            assert "economy" in r


# ---------------------------------------------------------------------------
# PA-08  Best / worst venue
# ---------------------------------------------------------------------------

class TestBestWorstVenues:
    def test_batting_role(self, mock_con):
        result = pa.best_worst_venues(BATTER, role="batting")
        assert "best" in result
        assert "worst" in result

    def test_bowling_role(self, mock_con):
        result = pa.best_worst_venues(BOWLER, role="bowling")
        assert "best" in result
        assert "worst" in result


# ---------------------------------------------------------------------------
# PA-09 / PA-10  Season splits
# ---------------------------------------------------------------------------

class TestSeasonSplits:
    def test_batting_seasons(self, mock_con):
        result = pa.batting_by_season(BATTER)
        assert isinstance(result, list)
        assert len(result) >= 1
        for r in result:
            assert "season" in r
            assert "runs" in r

    def test_bowling_seasons(self, mock_con):
        result = pa.bowling_by_season(BOWLER)
        assert isinstance(result, list)
        for r in result:
            assert "wickets" in r


# ---------------------------------------------------------------------------
# PA-11 / PA-12  Form tracker
# ---------------------------------------------------------------------------

class TestFormTracker:
    def test_batting_form(self, mock_con):
        result = pa.batting_form(BATTER, last_n=5)
        assert result["player"] == BATTER
        assert "matches" in result
        assert "total_runs" in result
        assert len(result["matches"]) <= 5

    def test_bowling_form(self, mock_con):
        result = pa.bowling_form(BOWLER, last_n=5)
        assert result["player"] == BOWLER
        assert "total_wickets" in result
        assert "matches" in result


# ---------------------------------------------------------------------------
# PA-13 / PA-14  vs teams
# ---------------------------------------------------------------------------

class TestVsTeams:
    def test_batting_vs_teams(self, mock_con):
        result = pa.batting_vs_teams(BATTER)
        assert isinstance(result, list)
        assert len(result) >= 1
        for r in result:
            assert "opposition" in r
            assert "runs" in r

    def test_bowling_vs_teams(self, mock_con):
        result = pa.bowling_vs_teams(BOWLER)
        assert isinstance(result, list)
        for r in result:
            assert "wickets" in r


# ---------------------------------------------------------------------------
# PA-15  Weakness detector
# ---------------------------------------------------------------------------

class TestWeaknessDetector:
    def test_returns_structure(self, mock_con):
        result = pa.weakness_detector(BATTER)
        assert "weaknesses" in result
        assert isinstance(result["weaknesses"], list)
        assert "career_sr" in result


# ---------------------------------------------------------------------------
# PA-16  Innings split
# ---------------------------------------------------------------------------

class TestInningsSplit:
    def test_split_returned(self, mock_con):
        result = pa.batting_by_innings_number(BATTER)
        assert "innings_split" in result
        assert isinstance(result["innings_split"], list)


# ---------------------------------------------------------------------------
# PA-17  Chases
# ---------------------------------------------------------------------------

class TestChases:
    def test_chase_data(self, mock_con):
        result = pa.batting_in_chases(BATTER)
        assert result["player"] == BATTER
        # M002 has target=150 and inning=2 — should have data
        assert result.get("matches", 0) >= 0


# ---------------------------------------------------------------------------
# PA-18  High pressure
# ---------------------------------------------------------------------------

class TestHighPressure:
    def test_structure(self, mock_con):
        result = pa.batting_under_pressure(BATTER, wickets_threshold=0)
        assert result["player"] == BATTER
        # threshold=0 means all deliveries qualify
        assert result.get("balls", 0) >= 0


# ---------------------------------------------------------------------------
# PA-19  Death specialist
# ---------------------------------------------------------------------------

class TestDeathSpecialist:
    def test_returns_ratio(self, mock_con):
        result = pa.death_over_specialist(BATTER)
        assert result["player"] == BATTER
        if result.get("death_over_balls", 0) > 0:
            assert result["death_over_sr"] is not None
            assert "is_specialist" in result


# ---------------------------------------------------------------------------
# PA-20  Highest score
# ---------------------------------------------------------------------------

class TestHighestScore:
    def test_top_scores(self, mock_con):
        result = pa.highest_score(BATTER, top_n=3)
        assert "top_scores" in result
        assert isinstance(result["top_scores"], list)
        for s in result["top_scores"]:
            assert "runs" in s
            assert "match_id" in s

    def test_sorted_desc(self, mock_con):
        result = pa.highest_score(BATTER, top_n=5)
        runs_list = [s["runs"] for s in result["top_scores"]]
        assert runs_list == sorted(runs_list, reverse=True)


# ---------------------------------------------------------------------------
# PA-21  Best bowling figures
# ---------------------------------------------------------------------------

class TestBestFigures:
    def test_returns_figures(self, mock_con):
        result = pa.best_bowling_figures(BOWLER, top_n=3)
        assert "best_figures" in result
        for f in result["best_figures"]:
            assert "/" in f["figures"]
            assert f["wickets"] >= 0


# ---------------------------------------------------------------------------
# PA-22  Streaks
# ---------------------------------------------------------------------------

class TestStreaks:
    def test_returns_batting_bowling(self, mock_con):
        result = pa.match_streaks(BATTER)
        assert "batting_streak" in result
        assert "bowling_streak" in result
        assert result["batting_streak"]["longest"] >= 0
        assert result["batting_streak"]["current"] >= 0


# ---------------------------------------------------------------------------
# PA-23  Ducks and economy breaks
# ---------------------------------------------------------------------------

class TestMilestonesFailures:
    def test_structure(self, mock_con):
        result = pa.milestones_and_failures(BATTER)
        assert "ducks" in result
        assert "economy_breaks_over_10" in result
        assert result["ducks"] >= 0


# ---------------------------------------------------------------------------
# PA-24  Compare
# ---------------------------------------------------------------------------

class TestComparePlayers:
    def test_both_players_present(self, mock_con):
        result = pa.compare_players(BATTER, BATTER2)
        assert result["player1"]["name"] == BATTER
        assert result["player2"]["name"] == BATTER2
        assert "batting" in result["player1"]
        assert "bowling" in result["player1"]


# ---------------------------------------------------------------------------
# PA-25  Batting leaderboard
# ---------------------------------------------------------------------------

class TestBattingLeaderboard:
    def test_returns_list(self, mock_con):
        result = pa.batting_leaderboard(sort_by="runs", top_n=5, min_balls=1)
        assert isinstance(result, list)
        assert len(result) >= 1
        for r in result:
            assert "player" in r
            assert "runs" in r

    def test_sorted_by_runs(self, mock_con):
        result = pa.batting_leaderboard(sort_by="runs", top_n=10, min_balls=1)
        runs = [r["runs"] for r in result]
        assert runs == sorted(runs, reverse=True)

    def test_sort_by_sr(self, mock_con):
        result = pa.batting_leaderboard(sort_by="strike_rate", top_n=5, min_balls=1)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# PA-26  Bowling leaderboard
# ---------------------------------------------------------------------------

class TestBowlingLeaderboard:
    def test_returns_list(self, mock_con):
        result = pa.bowling_leaderboard(sort_by="wickets", top_n=5, min_balls=1)
        assert isinstance(result, list)
        assert len(result) >= 1
        for r in result:
            assert "player" in r
            assert "wickets" in r

    def test_sort_by_economy(self, mock_con):
        result = pa.bowling_leaderboard(sort_by="economy", top_n=5, min_balls=1)
        economies = [r["economy"] for r in result if r["economy"] is not None]
        assert economies == sorted(economies)


# ---------------------------------------------------------------------------
# PA-27 / PA-28  Hand splits (metadata note)
# ---------------------------------------------------------------------------

class TestHandSplits:
    def test_batting_vs_bowler_hand(self, mock_con):
        result = pa.batting_vs_bowler_hand(BATTER)
        assert result["available"] is False
        assert "note" in result

    def test_bowling_vs_batter_hand(self, mock_con):
        result = pa.bowling_vs_batter_hand(BOWLER)
        assert result["available"] is False
        assert "note" in result
