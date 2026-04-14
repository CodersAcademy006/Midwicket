"""
Query type definition and validation tests.
Moved from repo root to tests/ for correct pytest discovery.
Removed QueryType (does not exist); tests Phase, Role literals and WinProbQuery validators.
"""
import pytest
from pydantic import ValidationError
from pypitch.query.defs import WinProbQuery, FantasyQuery, Phase, Role


class TestPhaseLiteral:
    VALID = ["powerplay", "middle", "death", "all"]

    def test_all_phases_valid(self):
        for p in self.VALID:
            assert p in self.VALID


class TestRoleLiteral:
    VALID = ["batter", "bowler", "all-rounder"]

    def test_all_roles_valid(self):
        for r in self.VALID:
            assert r in self.VALID


class TestQueryClassesExist:
    def test_fantasy_query_importable(self):
        assert FantasyQuery is not None

    def test_win_prob_query_importable(self):
        assert WinProbQuery is not None


class TestWinProbQueryValidation:
    # Minimum valid payload — snapshot_id is required by BaseQuery
    BASE = dict(
        snapshot_id="2024-01-01",
        venue_id=1,
        target_score=180,
        current_runs=120,
        current_wickets=3,
        overs_remaining=5.0,
    )

    def test_valid_overs_remaining(self):
        q = WinProbQuery(**self.BASE)
        assert q.overs_remaining == 5.0

    def test_overs_remaining_zero(self):
        q = WinProbQuery(**{**self.BASE, "overs_remaining": 0.0})
        assert q.overs_remaining == 0.0

    def test_overs_remaining_max(self):
        q = WinProbQuery(**{**self.BASE, "overs_remaining": 20.0})
        assert q.overs_remaining == 20.0

    def test_overs_remaining_too_high_raises(self):
        with pytest.raises(ValidationError):
            WinProbQuery(**{**self.BASE, "overs_remaining": 20.1})

    def test_overs_remaining_negative_raises(self):
        with pytest.raises(ValidationError):
            WinProbQuery(**{**self.BASE, "overs_remaining": -1.0})
