"""
Tests for pypitch.api.fantasy — fantasy_score, cheat_sheet, venue_bias.

All tests use a mocked session/engine so no real DuckDB data is needed.
Covers:
  - Milestone counting correctness (50s/100s from per-match aggregates)
  - Economy bonus tiers (lt7, lt8, none)
  - Empty / no-data safe path
  - Custom scoring weight override
  - cheat_sheet structure
  - venue_bias structure and verdict
"""

from __future__ import annotations

import pytest
from datetime import date
from unittest.mock import MagicMock, patch
from typing import Any, Dict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pydict(**cols: list) -> Dict[str, list]:
    """Build a mock to_pydict() return value."""
    return cols


def _engine_returning(pydict: Dict[str, list]) -> MagicMock:
    """Return a mock engine whose execute_sql().to_pydict() returns pydict."""
    result = MagicMock()
    result.to_pydict.return_value = pydict
    engine = MagicMock()
    engine.execute_sql.return_value = result
    return engine


def _session_with(engine: MagicMock) -> MagicMock:
    session = MagicMock()
    session.engine = engine
    return session


# ---------------------------------------------------------------------------
# fantasy_score — milestone correctness
# ---------------------------------------------------------------------------

class TestFantasyScoreMilestones:
    """Per-match aggregate SQL must count 50s/100s correctly."""

    def _bat_pydict(self, matches=5, runs=350, fours=20, sixes=10,
                    fifties=2, hundreds=1) -> Dict[str, list]:
        return _pydict(
            matches=[matches],
            runs=[runs],
            fours=[fours],
            sixes=[sixes],
            fifties=[fifties],
            hundreds=[hundreds],
        )

    def _bowl_pydict(self, matches=5, balls=120, wickets=8,
                     runs_conceded=160) -> Dict[str, list]:
        return _pydict(
            matches=[matches],
            balls=[balls],
            wickets=[wickets],
            runs_conceded=[runs_conceded],
        )

    def test_basic_score_calculation(self):
        engine = MagicMock()
        bat_res = MagicMock()
        bat_res.to_pydict.return_value = self._bat_pydict(
            matches=10, runs=500, fours=40, sixes=20, fifties=5, hundreds=2
        )
        bowl_res = MagicMock()
        bowl_res.to_pydict.return_value = self._bowl_pydict(
            matches=10, balls=180, wickets=15, runs_conceded=300
        )
        engine.execute_sql.side_effect = [bat_res, bowl_res]

        session = _session_with(engine)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import fantasy_score
            result = fantasy_score("Test Player")

        # batting: 500*1 + 40*1 + 20*2 + 5*30 + 2*50 = 500+40+40+150+100 = 830
        assert result["batting_pts"] == pytest.approx(830.0)
        assert result["matches"] == 10
        assert result["player"] == "Test Player"

    def test_fifties_and_hundreds_from_match_aggregates(self):
        """50s = match scores in [50,100); 100s = match scores ≥ 100."""
        engine = MagicMock()
        bat_res = MagicMock()
        bat_res.to_pydict.return_value = self._bat_pydict(
            matches=4, runs=280, fours=12, sixes=5, fifties=2, hundreds=1
        )
        bowl_res = MagicMock()
        bowl_res.to_pydict.return_value = self._bowl_pydict(
            matches=4, balls=60, wickets=3, runs_conceded=80
        )
        engine.execute_sql.side_effect = [bat_res, bowl_res]

        session = _session_with(engine)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import fantasy_score
            result = fantasy_score("Batter X")

        breakdown = result["batting_breakdown"]
        assert breakdown["fifties"] == 2
        assert breakdown["hundreds"] == 1
        # Points: 2*30 + 1*50 = 60 + 50 = 110 milestone pts
        assert result["batting_pts"] == pytest.approx(
            280 * 1 + 12 * 1 + 5 * 2 + 2 * 30 + 1 * 50
        )

    def test_zero_fifties_zero_hundreds(self):
        engine = MagicMock()
        bat_res = MagicMock()
        bat_res.to_pydict.return_value = self._bat_pydict(
            matches=3, runs=90, fours=5, sixes=2, fifties=0, hundreds=0
        )
        bowl_res = MagicMock()
        bowl_res.to_pydict.return_value = self._bowl_pydict(
            matches=3, balls=30, wickets=1, runs_conceded=40
        )
        engine.execute_sql.side_effect = [bat_res, bowl_res]

        session = _session_with(engine)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import fantasy_score
            result = fantasy_score("Steady Player")

        assert result["batting_breakdown"]["fifties"] == 0
        assert result["batting_breakdown"]["hundreds"] == 0


# ---------------------------------------------------------------------------
# fantasy_score — economy bonus
# ---------------------------------------------------------------------------

class TestFantasyScoreEconomyBonus:

    def _make_call(self, balls: int, runs_conceded: int) -> Dict[str, Any]:
        engine = MagicMock()
        bat_res = MagicMock()
        bat_res.to_pydict.return_value = _pydict(
            matches=[5], runs=[100], fours=[10], sixes=[5], fifties=[1], hundreds=[0]
        )
        bowl_res = MagicMock()
        bowl_res.to_pydict.return_value = _pydict(
            matches=[5], balls=[balls], wickets=[4], runs_conceded=[runs_conceded]
        )
        engine.execute_sql.side_effect = [bat_res, bowl_res]

        session = _session_with(engine)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import fantasy_score
            import importlib
            import pypitch.api.fantasy as _m
            importlib.reload(_m)
            return _m.fantasy_score("Economy Bowler")

    def test_economy_lt7_gives_bonus_10(self):
        # 6 overs, 36 runs → economy = 6.0 < 7 → +10
        from pypitch.api.fantasy import _DEFAULT_SCORING
        engine = MagicMock()
        bat_res = MagicMock()
        bat_res.to_pydict.return_value = _pydict(
            matches=[3], runs=[60], fours=[5], sixes=[2], fifties=[0], hundreds=[0]
        )
        bowl_res = MagicMock()
        bowl_res.to_pydict.return_value = _pydict(
            matches=[3], balls=[36], wickets=[2], runs_conceded=[36]
        )
        engine.execute_sql.side_effect = [bat_res, bowl_res]
        session = _session_with(engine)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import fantasy_score
            result = fantasy_score("Econ Bowler A")
        # economy = 36/(36/6) = 36/6 = 6.0 < 7 → bonus 10
        assert result["bowling_breakdown"]["economy_bonus"] == _DEFAULT_SCORING["economy_bonus_lt7"]

    def test_economy_lt8_gives_bonus_5(self):
        from pypitch.api.fantasy import _DEFAULT_SCORING
        engine = MagicMock()
        bat_res = MagicMock()
        bat_res.to_pydict.return_value = _pydict(
            matches=[3], runs=[60], fours=[5], sixes=[2], fifties=[0], hundreds=[0]
        )
        bowl_res = MagicMock()
        bowl_res.to_pydict.return_value = _pydict(
            matches=[3], balls=[36], wickets=[2], runs_conceded=[42]
        )
        engine.execute_sql.side_effect = [bat_res, bowl_res]
        session = _session_with(engine)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import fantasy_score
            result = fantasy_score("Econ Bowler B")
        # economy = 42/6 = 7.0 → not < 7, is < 8 → bonus 5
        assert result["bowling_breakdown"]["economy_bonus"] == _DEFAULT_SCORING["economy_bonus_lt8"]

    def test_economy_ge8_gives_no_bonus(self):
        engine = MagicMock()
        bat_res = MagicMock()
        bat_res.to_pydict.return_value = _pydict(
            matches=[3], runs=[60], fours=[5], sixes=[2], fifties=[0], hundreds=[0]
        )
        bowl_res = MagicMock()
        bowl_res.to_pydict.return_value = _pydict(
            matches=[3], balls=[36], wickets=[2], runs_conceded=[54]
        )
        engine.execute_sql.side_effect = [bat_res, bowl_res]
        session = _session_with(engine)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import fantasy_score
            result = fantasy_score("Expensive Bowler")
        # economy = 54/6 = 9.0 → no bonus
        assert result["bowling_breakdown"]["economy_bonus"] == 0

    def test_insufficient_balls_skips_economy(self):
        """< 6 balls bowled — economy calculation is skipped."""
        engine = MagicMock()
        bat_res = MagicMock()
        bat_res.to_pydict.return_value = _pydict(
            matches=[1], runs=[20], fours=[1], sixes=[0], fifties=[0], hundreds=[0]
        )
        bowl_res = MagicMock()
        bowl_res.to_pydict.return_value = _pydict(
            matches=[1], balls=[3], wickets=[1], runs_conceded=[5]
        )
        engine.execute_sql.side_effect = [bat_res, bowl_res]
        session = _session_with(engine)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import fantasy_score
            result = fantasy_score("Part Timer")
        assert result["bowling_breakdown"]["economy"] is None
        assert result["bowling_breakdown"]["economy_bonus"] == 0


# ---------------------------------------------------------------------------
# fantasy_score — empty / no-data path
# ---------------------------------------------------------------------------

class TestFantasyScoreEmptyData:

    def test_empty_data_returns_safe_defaults(self):
        """Engine raises RuntimeError — result should still be a valid dict."""
        engine = MagicMock()
        engine.execute_sql.side_effect = RuntimeError("table does not exist")

        session = _session_with(engine)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import fantasy_score
            result = fantasy_score("Ghost Player")

        assert result["player"] == "Ghost Player"
        assert result["matches"] == 0
        assert result["total_pts"] == 0.0
        assert result["per_match_avg"] == 0.0

    def test_zero_matches_gives_zero_per_match_avg(self):
        engine = MagicMock()
        bat_res = MagicMock()
        bat_res.to_pydict.return_value = _pydict(
            matches=[0], runs=[0], fours=[0], sixes=[0], fifties=[0], hundreds=[0]
        )
        bowl_res = MagicMock()
        bowl_res.to_pydict.return_value = _pydict(
            matches=[0], balls=[0], wickets=[0], runs_conceded=[0]
        )
        engine.execute_sql.side_effect = [bat_res, bowl_res]
        session = _session_with(engine)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import fantasy_score
            result = fantasy_score("New Player")

        assert result["per_match_avg"] == 0.0
        assert result["total_pts"] == 0.0


# ---------------------------------------------------------------------------
# fantasy_score — custom scoring weights
# ---------------------------------------------------------------------------

class TestFantasyScoreCustomWeights:

    def test_custom_weights_applied(self):
        engine = MagicMock()
        bat_res = MagicMock()
        bat_res.to_pydict.return_value = _pydict(
            matches=[4], runs=[200], fours=[10], sixes=[5], fifties=[2], hundreds=[0]
        )
        bowl_res = MagicMock()
        bowl_res.to_pydict.return_value = _pydict(
            matches=[4], balls=[60], wickets=[5], runs_conceded=[80]
        )
        engine.execute_sql.side_effect = [bat_res, bowl_res]
        session = _session_with(engine)

        custom = {"run": 2, "six": 5, "wicket": 20, "fifty": 40, "hundred": 80,
                  "four": 0, "economy_bonus_lt7": 0, "economy_bonus_lt8": 0}
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import fantasy_score
            result = fantasy_score("Power Hitter", scoring=custom)

        # batting: 200*2 + 10*0 + 5*5 + 2*40 + 0*80 = 400+0+25+80 = 505
        expected_bat = 200 * 2 + 10 * 0 + 5 * 5 + 2 * 40 + 0 * 80
        assert result["batting_pts"] == pytest.approx(float(expected_bat))

        # bowling: 5*20 + 0 economy bonus
        expected_bowl = 5 * 20
        assert result["bowling_pts"] == pytest.approx(float(expected_bowl))


# ---------------------------------------------------------------------------
# venue_bias — structure and verdict
# ---------------------------------------------------------------------------

class TestVenueBias:

    def _make_session(self, inning_rows, run_rows) -> MagicMock:
        engine = MagicMock()
        inning_res = MagicMock()
        inning_res.to_pydict.return_value = inning_rows
        run_res = MagicMock()
        run_res.to_pydict.return_value = run_rows
        engine.execute_sql.side_effect = [inning_res, run_res]
        session = _session_with(engine)
        return session

    def test_bat_first_verdict(self):
        inning_rows = _pydict(inning=[1, 2], matches=[5, 5])
        run_rows = _pydict(
            match_id=["m1", "m1", "m2", "m2"],
            inning=[1, 2, 1, 2],
            total_runs=[180, 150, 170, 160],
        )
        session = self._make_session(inning_rows, run_rows)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import venue_bias
            result = venue_bias("Wankhede")

        assert result["verdict"] == "BAT FIRST"
        assert result["total_matches_analysed"] == 2
        assert result["win_bat_first_pct"] == 100.0

    def test_chase_verdict(self):
        inning_rows = _pydict(inning=[1, 2], matches=[3, 3])
        run_rows = _pydict(
            match_id=["m1", "m1", "m2", "m2"],
            inning=[1, 2, 1, 2],
            total_runs=[140, 145, 130, 160],
        )
        session = self._make_session(inning_rows, run_rows)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import venue_bias
            result = venue_bias("Eden Gardens")

        assert result["verdict"] == "CHASE"
        assert result["win_chase_pct"] == 100.0

    def test_no_data_returns_neutral(self):
        engine = MagicMock()
        engine.execute_sql.side_effect = RuntimeError("no data")
        session = _session_with(engine)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import venue_bias
            result = venue_bias("Unknown Venue")

        assert result["verdict"] == "INSUFFICIENT DATA"
        assert result["win_bat_first_pct"] == 50.0
        assert result["win_chase_pct"] == 50.0

    def test_result_keys_present(self):
        inning_rows = _pydict(inning=[1, 2], matches=[4, 4])
        run_rows = _pydict(
            match_id=["m1", "m1"],
            inning=[1, 2],
            total_runs=[160, 155],
        )
        session = self._make_session(inning_rows, run_rows)
        with patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import venue_bias
            result = venue_bias("Chinnaswamy")

        for key in ("venue", "total_matches_analysed", "win_bat_first_pct",
                    "win_chase_pct", "verdict"):
            assert key in result


# ---------------------------------------------------------------------------
# cheat_sheet — structure
# ---------------------------------------------------------------------------

class TestCheatSheet:

    def test_returns_dataframe_or_empty(self):
        """cheat_sheet must always return a DataFrame (possibly empty)."""
        import pandas as pd
        engine = MagicMock()
        engine.execute_sql.side_effect = RuntimeError("no data")
        session = _session_with(engine)

        with patch("pypitch.api.fantasy.get_executor", side_effect=RuntimeError("no exec")), \
             patch("pypitch.api.fantasy.get_registry", side_effect=RuntimeError("no reg")), \
             patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import cheat_sheet
            df = cheat_sheet("Nowhere Stadium")

        assert isinstance(df, pd.DataFrame)

    def test_fallback_dataframe_has_player_column(self):
        """When ball_events fallback succeeds, DataFrame has a 'player' column."""
        import pandas as pd

        result_mock = MagicMock()
        result_mock.to_pandas.return_value = pd.DataFrame({
            "player": ["Alice", "Bob"],
            "matches": [5, 4],
            "runs": [200, 150],
            "strike_rate": [140.0, 125.0],
        })
        engine = MagicMock()
        engine.execute_sql.return_value = result_mock
        session = _session_with(engine)

        with patch("pypitch.api.fantasy.get_executor", side_effect=RuntimeError), \
             patch("pypitch.api.fantasy.get_registry", side_effect=RuntimeError), \
             patch("pypitch.api.fantasy.get_session", return_value=session):
            from pypitch.api.fantasy import cheat_sheet
            df = cheat_sheet("Test Venue")

        assert isinstance(df, pd.DataFrame)
        assert "player" in df.columns
