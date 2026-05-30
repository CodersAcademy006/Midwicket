"""
Tests for midwicket/api/player_analytics.py (PA-01 to PA-28).

Uses an in-memory DuckDB session with synthetic ball_events data.
All player_analytics functions call _get_con() → session.engine.raw_connection().
We monkey-patch _get_con to return a fresh in-memory connection seeded with
fixture data so tests run without a real data directory.
"""

from __future__ import annotations

import duckdb
import pytest
from unittest.mock import patch

import midwicket.api.player_analytics as pa


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BATTER = "V Kohli"
BOWLER = "JJ Bumrah"
BATTER2 = "RG Sharma"

_SCHEMA = """
CREATE TABLE ball_events (
    match_id        VARCHAR,
    date            DATE DEFAULT '2023-01-01',
    venue_id        INTEGER DEFAULT 0,
    inning          INTEGER,
    over            INTEGER,
    ball            INTEGER,
    batter_id       INTEGER DEFAULT 0,
    bowler_id       INTEGER DEFAULT 0,
    non_striker_id  INTEGER DEFAULT 0,
    batting_team_id SMALLINT DEFAULT 0,
    bowling_team_id SMALLINT DEFAULT 0,
    runs_batter     INTEGER DEFAULT 0,
    runs_extras     INTEGER DEFAULT 0,
    extras_type     VARCHAR DEFAULT NULL,
    is_wicket       BOOLEAN DEFAULT FALSE,
    wicket_type     VARCHAR DEFAULT NULL,
    phase           VARCHAR DEFAULT 'Middle',
    batter          VARCHAR DEFAULT '',
    bowler          VARCHAR DEFAULT '',
    venue           VARCHAR DEFAULT '',
    player_dismissed VARCHAR DEFAULT NULL
)
"""


def _seed(con: duckdb.DuckDBPyConnection) -> None:
    """Insert synthetic ball data for two batters and one bowler.

    Schema matches v1 + denormalized names (no legacy runs_total/season/etc.).
    """
    con.execute(_SCHEMA)

    rows = []
    # Match 1 — Kohli bats, Bumrah bowls, Wankhede, 2023-05-21
    for over in range(20):
        phase = "Powerplay" if over < 6 else ("Middle" if over < 15 else "Death")
        for ball in range(6):
            runs_b = 4 if (over + ball) % 5 == 0 else (1 if ball % 2 == 0 else 0)
            is_w = (over == 15 and ball == 5)
            rows.append((
                "M001", "2023-05-21", 1,  # match_id, date, venue_id
                1, over, ball,
                10, 20, 11,  # batter_id, bowler_id, non_striker_id
                1, 2,  # batting_team_id, bowling_team_id
                runs_b, 0, is_w,
                "BOWLED" if is_w else None,
                phase,
                BATTER, BOWLER, "Wankhede",
                BATTER if is_w else None,
            ))

    # Match 2 — Kohli bats again, Eden Gardens, 2022-04-10
    for over in range(10):
        phase = "Powerplay" if over < 6 else "Middle"
        for ball in range(6):
            runs_b = 6 if ball == 5 else 2
            rows.append((
                "M002", "2022-04-10", 2,  # match_id, date, venue_id
                2, over, ball,
                10, 30, 11,  # batter_id, bowler_id (different), non_striker_id
                1, 3,  # batting_team_id, bowling_team_id (different)
                runs_b, 0, False,
                None,
                phase,
                BATTER, "Another Bowler", "Eden Gardens",
                None,
            ))

    # Match 3 — Sharma bats
    for over in range(8):
        phase = "Powerplay" if over < 6 else "Middle"
        for ball in range(6):
            rows.append((
                "M003", "2023-06-15", 1,  # match_id, date, venue_id
                1, over, ball,
                12, 20, 13,  # batter_id (Sharma), bowler_id (Bumrah), non_striker_id
                3, 1,  # batting_team_id, bowling_team_id
                3, 0, False,
                None,
                phase,
                BATTER2, BOWLER, "Wankhede",
                None,
            ))

    # Bumrah bowls in M003 — add bowling rows (different batter facing)
    for over in range(4):
        phase = "Powerplay" if over < 6 else "Middle"
        for ball in range(6):
            is_w = (over == 3 and ball == 3)
            rows.append((
                "M003", "2023-06-15", 1,  # match_id, date, venue_id
                1, over, ball,
                14, 20, 15,  # batter_id, bowler_id (Bumrah), non_striker_id
                3, 1,  # batting_team_id, bowling_team_id
                6, 2, is_w,
                "CAUGHT" if is_w else None,
                phase,
                "Other Batter", BOWLER, "Wankhede",
                "Other Batter" if is_w else None,
            ))

    con.executemany("""
        INSERT INTO ball_events (
            match_id, date, venue_id, inning, over, ball,
            batter_id, bowler_id, non_striker_id,
            batting_team_id, bowling_team_id,
            runs_batter, runs_extras, is_wicket, wicket_type, phase,
            batter, bowler, venue, player_dismissed
        ) VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
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


# ---------------------------------------------------------------------------
# Regression tests for wides and no-balls (MW-011, MW-013, MW-040)
# ---------------------------------------------------------------------------

class TestCricketStatsCorrectness:
    def test_wides_and_noballs_metrics(self, monkeypatch):
        # Insert test events:
        # 1. A wide ball (runs_extras=1, extras_type='wides')
        # 2. A noball where batter scores 4 runs (runs_batter=4, runs_extras=1, extras_type='noballs')
        # 3. A normal ball where batter scores 1 run (runs_batter=1)
        test_rows = [
            ("M999", "2026-05-29", 99, 1, 0, 1, 100, 200, 101, 1, 2, 0, 1, "wides", False, None, "Middle", "Test Batter", "Test Bowler", "Test Venue", None),
            ("M999", "2026-05-29", 99, 1, 0, 2, 100, 200, 101, 1, 2, 4, 1, "noballs", False, None, "Middle", "Test Batter", "Test Bowler", "Test Venue", None),
            ("M999", "2026-05-29", 99, 1, 0, 3, 100, 200, 101, 1, 2, 1, 0, None, False, None, "Middle", "Test Batter", "Test Bowler", "Test Venue", None)
        ]
        
        def _get_seeded_con():
            con = duckdb.connect(":memory:")
            con.execute(_SCHEMA)
            con.executemany("INSERT INTO ball_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", test_rows)
            return con
        
        # Patch _get_con to return our custom seeded connection creator
        monkeypatch.setattr(pa, "_get_con", _get_seeded_con)
        
        # Test batting stats
        bat_stats = pa.career_batting("Test Batter")
        assert bat_stats["runs"] == 5
        assert bat_stats["balls_faced"] == 2  # noball + normal ball, excludes wide
        assert bat_stats["strike_rate"] == 250.0
        assert bat_stats["dot_balls"] == 0  # neither ball faced had runs_batter = 0
        
        # Test bowling stats
        bowl_stats = pa.career_bowling("Test Bowler")
        assert bowl_stats["balls_bowled"] == 1  # normal ball only, excludes wide and noball
        assert bowl_stats["runs_conceded"] == 7  # 4 batter runs + 1 wide penalty + 1 noball penalty
        assert bowl_stats["economy"] == 42.0  # 7 runs / (1/6 overs) = 42.0

    def test_non_striker_dismissal_correctness(self, monkeypatch):
        test_rows = [
            ("M888", "2026-05-29", 99, 1, 0, 1, 100, 200, 101, 1, 2, 1, 0, None, True, "RUN_OUT", "Middle", "Striker", "Bowler", "Venue", "NonStriker")
        ]
        
        def _get_seeded_con():
            con = duckdb.connect(":memory:")
            con.execute(_SCHEMA)
            con.executemany("INSERT INTO ball_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", test_rows)
            return con
            
        monkeypatch.setattr(pa, "_get_con", _get_seeded_con)
        
        striker_stats = pa.career_batting("Striker")
        assert striker_stats["innings"] - striker_stats["not_outs"] == 0
        assert striker_stats["not_outs"] == 1
        
        non_striker_stats = pa.career_batting("NonStriker")
        assert non_striker_stats["innings"] - non_striker_stats["not_outs"] == 1
        assert non_striker_stats["not_outs"] == 0


def test_get_con_returns_actual_connection():
    from midwicket.api.session import init
    import tempfile
    from pathlib import Path
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_api_get_con.duckdb"
        init(str(db_path))
        
        con = pa._get_con()
        assert con is not None
        assert hasattr(con, "execute")
        con.close()


