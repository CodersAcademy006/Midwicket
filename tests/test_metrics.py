"""
Metric function unit tests — validates pure compute functions (Agent 5).
Moved from repo root to tests/ for correct pytest discovery.
Only imports functions that are confirmed to exist in the current codebase.
"""
import pytest
import pyarrow as pa
import pyarrow.compute as pc

# ── Batting ──────────────────────────────────────────────────────────────────
from pypitch.compute.metrics.batting import calculate_strike_rate

# ── Bowling ───────────────────────────────────────────────────────────────────
from pypitch.compute.metrics.bowling import calculate_economy, calculate_pressure_index

# ── Partnership ───────────────────────────────────────────────────────────────
from pypitch.compute.metrics.partnership import (
    calculate_partnership_run_rate,
    calculate_partnership_contribution,
)

# ── Team ──────────────────────────────────────────────────────────────────────
from pypitch.compute.metrics.team import calculate_team_win_rate, calculate_team_run_rate


# ---------------------------------------------------------------------------
# Batting
# ---------------------------------------------------------------------------

class TestStrikeRate:
    def test_basic(self):
        runs = pa.array([50, 30])
        balls = pa.array([40, 30])
        sr = calculate_strike_rate(runs, balls)
        assert sr[0].as_py() == pytest.approx(125.0)
        assert sr[1].as_py() == pytest.approx(100.0)

    def test_zero_balls_returns_zero(self):
        sr = calculate_strike_rate(pa.array([0]), pa.array([0]))
        assert sr[0].as_py() == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Bowling
# ---------------------------------------------------------------------------

class TestEconomy:
    def test_basic(self):
        econ = calculate_economy(pa.array([30]), pa.array([24]))  # 4 overs
        assert econ[0].as_py() == pytest.approx(7.5)

    def test_zero_balls_returns_zero(self):
        econ = calculate_economy(pa.array([0]), pa.array([0]))
        assert econ[0].as_py() == pytest.approx(0.0)


class TestPressureIndex:
    def test_basic(self):
        pi = calculate_pressure_index(pa.array([12]), pa.array([24]))
        assert pi[0].as_py() == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# Partnership  — zero-guard verification (PR #12 fixes)
# ---------------------------------------------------------------------------

class TestPartnershipRunRate:
    def test_basic(self):
        rr = calculate_partnership_run_rate(pa.array([45]), pa.array([30]))
        assert rr[0].as_py() == pytest.approx(9.0)

    def test_zero_balls_returns_zero(self):
        """balls == 0 must return 0.0, not Inf."""
        rr = calculate_partnership_run_rate(pa.array([0]), pa.array([0]))
        result = rr[0].as_py()
        assert result == pytest.approx(0.0), f"Expected 0.0, got {result}"

    def test_multiple_values(self):
        rr = calculate_partnership_run_rate(
            pa.array([45, 67, 23, 0]),
            pa.array([30, 42, 18, 0]),
        )
        assert rr[0].as_py() == pytest.approx(9.0)
        assert rr[3].as_py() == pytest.approx(0.0)


class TestPartnershipContribution:
    def test_basic(self):
        pct = calculate_partnership_contribution(pa.array([30]), pa.array([60]))
        assert pct[0].as_py() == pytest.approx(50.0)

    def test_zero_partnership_returns_zero(self):
        """partnership_runs == 0 must return 0.0, not Inf."""
        pct = calculate_partnership_contribution(pa.array([0]), pa.array([0]))
        result = pct[0].as_py()
        assert result == pytest.approx(0.0), f"Expected 0.0, got {result}"

    def test_full_contribution(self):
        pct = calculate_partnership_contribution(pa.array([60]), pa.array([60]))
        assert pct[0].as_py() == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Team  — zero-guard verification (PR #12 fixes)
# ---------------------------------------------------------------------------

class TestTeamWinRate:
    def test_basic(self):
        wr = calculate_team_win_rate(pa.array([15]), pa.array([20]))
        assert wr[0].as_py() == pytest.approx(75.0)

    def test_zero_matches_returns_zero(self):
        """matches == 0 must return 0.0, not Inf."""
        wr = calculate_team_win_rate(pa.array([0]), pa.array([0]))
        result = wr[0].as_py()
        assert result == pytest.approx(0.0), f"Expected 0.0, got {result}"

    def test_multiple_values(self):
        wr = calculate_team_win_rate(
            pa.array([15, 12, 8]),
            pa.array([20, 18, 14]),
        )
        assert wr[0].as_py() == pytest.approx(75.0)
        assert wr[1].as_py() == pytest.approx(pytest.approx(66.6667, rel=1e-3))


class TestTeamRunRate:
    def test_basic(self):
        rr = calculate_team_run_rate(pa.array([180]), pa.array([20]))
        assert rr[0].as_py() == pytest.approx(9.0)

    def test_zero_overs_returns_zero(self):
        """overs == 0 must return 0.0, not Inf."""
        rr = calculate_team_run_rate(pa.array([0]), pa.array([0]))
        result = rr[0].as_py()
        assert result == pytest.approx(0.0), f"Expected 0.0, got {result}"
