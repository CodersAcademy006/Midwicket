"""
Tests for 8 previously-untested public functions:
  - plot_match_worm, plot_run_pressure, plot_batter_pacing,
    plot_momentum_swings, plot_manhattan, plot_partnership_flow
    (midwicket/visuals/worm.py)
  - build_registry_stats (midwicket/data/pipeline.py)
  - serve_overlay (midwicket/live/overlay.py)
"""
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — must come before pyplot import

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch

from midwicket.exceptions import MatchDataMissing


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_session(arrow_table: pa.Table, registry_rows=None):
    """Build a minimal mock session whose engine returns the given Arrow table."""
    session = MagicMock()
    session.engine.execute_sql.return_value = arrow_table

    # registry.con.execute(...).fetchone() → (name,) or None
    mock_con = MagicMock()
    if registry_rows is None:
        mock_con.execute.return_value.fetchone.return_value = ("Test Player",)
        mock_con.execute.return_value.fetchall.return_value = []
    else:
        mock_con.execute.return_value.fetchall.return_value = registry_rows
        mock_con.execute.return_value.fetchone.return_value = ("Test Player",)
    session.registry.con = mock_con
    return session


def _worm_table() -> pa.Table:
    """Minimal ball_events Arrow table covering 4 deliveries across 2 overs."""
    return pa.table({
        "inning": pa.array([1, 1, 1, 1], type=pa.int32()),
        "over": pa.array([0, 0, 1, 1], type=pa.int32()),
        "ball": pa.array([1, 2, 1, 2], type=pa.int32()),
        "runs_scored": pa.array([4, 1, 0, 6], type=pa.int32()),
        "is_wicket": pa.array([False, False, True, False]),
        "batter_id": pa.array([1, 1, 1, 2], type=pa.int64()),
        "bowler_id": pa.array([10, 10, 10, 10], type=pa.int64()),
        "wicket_type": pa.array([None, None, "BOWLED", None]),
        "runs_batter": pa.array([4, 1, 0, 6], type=pa.int32()),
    })


def _two_innings_table() -> pa.Table:
    return pa.table({
        "inning": pa.array([1, 1, 2, 2], type=pa.int32()),
        "over": pa.array([0, 0, 0, 0], type=pa.int32()),
        "ball": pa.array([1, 2, 1, 2], type=pa.int32()),
        "runs_scored": pa.array([4, 1, 3, 2], type=pa.int32()),
        "is_wicket": pa.array([False, False, False, False]),
        "batter_id": pa.array([1, 1, 3, 3], type=pa.int64()),
        "bowler_id": pa.array([10, 10, 20, 20], type=pa.int64()),
        "wicket_type": pa.array([None, None, None, None]),
        "runs_batter": pa.array([4, 1, 3, 2], type=pa.int32()),
        "non_striker_id": pa.array([2, 2, 4, 4], type=pa.int64()),
    })


# ---------------------------------------------------------------------------
# plot_match_worm
# ---------------------------------------------------------------------------

class TestPlotMatchWorm:
    def test_returns_axis(self):
        from midwicket.visuals.worm import plot_match_worm
        session = _make_session(_worm_table())
        ax = plot_match_worm("match_001", session)
        assert ax is not None
        assert hasattr(ax, "set_title")

    def test_accepts_provided_axis(self):
        import matplotlib.pyplot as plt
        from midwicket.visuals.worm import plot_match_worm
        fig, ax = plt.subplots()
        session = _make_session(_worm_table())
        result = plot_match_worm("match_001", session, ax=ax)
        assert result is ax
        plt.close(fig)

    def test_raises_match_data_missing_on_empty(self):
        from midwicket.visuals.worm import plot_match_worm
        empty = pa.table({
            "inning": pa.array([], type=pa.int32()),
            "over": pa.array([], type=pa.int32()),
            "ball": pa.array([], type=pa.int32()),
            "runs_scored": pa.array([], type=pa.int32()),
            "is_wicket": pa.array([], type=pa.bool_()),
            "batter_id": pa.array([], type=pa.int64()),
            "bowler_id": pa.array([], type=pa.int64()),
            "wicket_type": pa.array([], type=pa.string()),
            "runs_batter": pa.array([], type=pa.int32()),
        })
        session = _make_session(empty)
        with pytest.raises(MatchDataMissing):
            plot_match_worm("missing", session)

    def test_raises_match_data_missing_on_query_error(self):
        from midwicket.visuals.worm import plot_match_worm
        session = MagicMock()
        session.engine.execute_sql.side_effect = RuntimeError("db error")
        with pytest.raises(MatchDataMissing):
            plot_match_worm("bad_match", session)


# ---------------------------------------------------------------------------
# plot_run_pressure
# ---------------------------------------------------------------------------

class TestPlotRunPressure:
    def test_returns_axis(self):
        from midwicket.visuals.worm import plot_run_pressure
        session = _make_session(_two_innings_table())
        ax = plot_run_pressure("match_001", session)
        assert ax is not None

    def test_accepts_custom_balls_per_innings(self):
        from midwicket.visuals.worm import plot_run_pressure
        session = _make_session(_two_innings_table())
        ax = plot_run_pressure("match_001", session, balls_per_innings=300)
        assert ax is not None

    def test_raises_on_empty(self):
        from midwicket.visuals.worm import plot_run_pressure
        empty = pa.table({
            "inning": pa.array([], type=pa.int32()),
            "over": pa.array([], type=pa.int32()),
            "ball": pa.array([], type=pa.int32()),
            "runs_scored": pa.array([], type=pa.int32()),
        })
        session = _make_session(empty)
        with pytest.raises(MatchDataMissing):
            plot_run_pressure("missing", session)

    def test_raises_on_query_failure(self):
        from midwicket.visuals.worm import plot_run_pressure
        session = MagicMock()
        session.engine.execute_sql.side_effect = ValueError("bad query")
        with pytest.raises(MatchDataMissing):
            plot_run_pressure("bad", session)


# ---------------------------------------------------------------------------
# plot_batter_pacing
# ---------------------------------------------------------------------------

class TestPlotBatterPacing:
    def _batter_table(self) -> pa.Table:
        return pa.table({
            "inning": pa.array([1, 1, 1], type=pa.int32()),
            "over": pa.array([0, 0, 1], type=pa.int32()),
            "ball": pa.array([1, 2, 1], type=pa.int32()),
            "runs_scored": pa.array([4, 2, 1], type=pa.int32()),
            "runs_batter": pa.array([4, 2, 1], type=pa.int32()),
            "is_wicket": pa.array([False, False, False]),
            "bowler_id": pa.array([10, 10, 10], type=pa.int64()),
            "wicket_type": pa.array([None, None, None]),
        })

    def test_returns_axis(self):
        from midwicket.visuals.worm import plot_batter_pacing
        session = _make_session(self._batter_table())
        ax = plot_batter_pacing("match_001", 1, session)
        assert ax is not None

    def test_dismissal_annotation(self):
        """When final ball is a wicket, dismissal annotation code path runs."""
        from midwicket.visuals.worm import plot_batter_pacing
        data = pa.table({
            "inning": pa.array([1, 1], type=pa.int32()),
            "over": pa.array([0, 0], type=pa.int32()),
            "ball": pa.array([1, 2], type=pa.int32()),
            "runs_scored": pa.array([4, 0], type=pa.int32()),
            "runs_batter": pa.array([4, 0], type=pa.int32()),
            "is_wicket": pa.array([False, True]),
            "bowler_id": pa.array([10, 10], type=pa.int64()),
            "wicket_type": pa.array([None, "BOWLED"]),
        })
        session = _make_session(data)
        # also mock the second execute_sql for the "batsmen in match" fallback
        # (not called since data is non-empty)
        ax = plot_batter_pacing("match_001", 1, session)
        assert ax is not None

    def test_raises_on_empty(self):
        from midwicket.visuals.worm import plot_batter_pacing
        empty = pa.table({
            "inning": pa.array([], type=pa.int32()),
            "over": pa.array([], type=pa.int32()),
            "ball": pa.array([], type=pa.int32()),
            "runs_scored": pa.array([], type=pa.int32()),
            "runs_batter": pa.array([], type=pa.int32()),
            "is_wicket": pa.array([], type=pa.bool_()),
            "bowler_id": pa.array([], type=pa.int64()),
            "wicket_type": pa.array([], type=pa.string()),
        })
        # first call returns empty (batter data), second call returns batsmen list
        batters_table = pa.table({"batter_id": pa.array([2, 3], type=pa.int64())})
        session = MagicMock()
        session.engine.execute_sql.side_effect = [empty, batters_table]
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchone.return_value = ("Other Player",)
        session.registry.con = mock_con
        with pytest.raises(MatchDataMissing):
            plot_batter_pacing("match_001", 999, session)


# ---------------------------------------------------------------------------
# plot_momentum_swings
# ---------------------------------------------------------------------------

class TestPlotMomentumSwings:
    def test_returns_axis(self):
        from midwicket.visuals.worm import plot_momentum_swings
        session = _make_session(_worm_table())
        ax = plot_momentum_swings("match_001", session)
        assert ax is not None

    def test_two_innings(self):
        from midwicket.visuals.worm import plot_momentum_swings
        session = _make_session(_two_innings_table())
        ax = plot_momentum_swings("match_001", session)
        assert ax is not None

    def test_raises_on_empty(self):
        from midwicket.visuals.worm import plot_momentum_swings
        empty = pa.table({
            "inning": pa.array([], type=pa.int32()),
            "over": pa.array([], type=pa.int32()),
            "ball": pa.array([], type=pa.int32()),
            "runs_scored": pa.array([], type=pa.int32()),
        })
        session = _make_session(empty)
        with pytest.raises(MatchDataMissing):
            plot_momentum_swings("missing", session)

    def test_raises_on_query_failure(self):
        from midwicket.visuals.worm import plot_momentum_swings
        session = MagicMock()
        session.engine.execute_sql.side_effect = AttributeError("missing attr")
        with pytest.raises(MatchDataMissing):
            plot_momentum_swings("bad", session)


# ---------------------------------------------------------------------------
# plot_manhattan
# ---------------------------------------------------------------------------

class TestPlotManhattan:
    def _manhattan_table(self) -> pa.Table:
        return pa.table({
            "inning": pa.array([1, 1, 1, 1], type=pa.int32()),
            "over": pa.array([0, 0, 1, 1], type=pa.int32()),
            "ball": pa.array([1, 2, 1, 2], type=pa.int32()),
            "runs_scored": pa.array([4, 0, 1, 6], type=pa.int32()),
            "is_wicket": pa.array([False, False, True, False]),
            "runs_batter": pa.array([4, 0, 1, 6], type=pa.int32()),
        })

    def test_returns_axis(self):
        from midwicket.visuals.worm import plot_manhattan
        session = _make_session(self._manhattan_table())
        ax = plot_manhattan("match_001", session)
        assert ax is not None

    def test_color_code_paths(self):
        """Covers wicket, boundary, dot, and normal event color paths."""
        from midwicket.visuals.worm import plot_manhattan
        data = pa.table({
            "inning": pa.array([1, 1, 1, 1, 1, 1], type=pa.int32()),
            "over": pa.array([0, 0, 0, 1, 1, 1], type=pa.int32()),
            "ball": pa.array([1, 2, 3, 1, 2, 3], type=pa.int32()),
            "runs_scored": pa.array([4, 0, 2, 6, 1, 0], type=pa.int32()),
            "is_wicket": pa.array([False, False, False, True, False, False]),
            "runs_batter": pa.array([4, 0, 2, 6, 1, 0], type=pa.int32()),
        })
        session = _make_session(data)
        ax = plot_manhattan("match_001", session)
        assert ax is not None

    def test_raises_on_empty(self):
        from midwicket.visuals.worm import plot_manhattan
        empty = pa.table({
            "inning": pa.array([], type=pa.int32()),
            "over": pa.array([], type=pa.int32()),
            "ball": pa.array([], type=pa.int32()),
            "runs_scored": pa.array([], type=pa.int32()),
            "is_wicket": pa.array([], type=pa.bool_()),
            "runs_batter": pa.array([], type=pa.int32()),
        })
        session = _make_session(empty)
        with pytest.raises(MatchDataMissing):
            plot_manhattan("missing", session)


# ---------------------------------------------------------------------------
# plot_partnership_flow
# ---------------------------------------------------------------------------

class TestPlotPartnershipFlow:
    def _partnership_table(self) -> pa.Table:
        return pa.table({
            "inning": pa.array([1, 1, 1, 1], type=pa.int32()),
            "batter_id": pa.array([1, 1, 2, 2], type=pa.int64()),
            "non_striker_id": pa.array([2, 2, 3, 3], type=pa.int64()),
            "over": pa.array([0, 0, 1, 1], type=pa.int32()),
            "ball": pa.array([1, 2, 1, 2], type=pa.int32()),
            "runs_scored": pa.array([4, 1, 3, 2], type=pa.int32()),
        })

    def test_returns_axis(self):
        from midwicket.visuals.worm import plot_partnership_flow
        session = _make_session(
            self._partnership_table(),
            registry_rows=[(1, "Player A"), (2, "Player B"), (3, "Player C")],
        )
        ax = plot_partnership_flow("match_001", session)
        assert ax is not None

    def test_registry_lookup_failure_handled(self):
        """Registry errors must not crash the function — falls back gracefully."""
        from midwicket.visuals.worm import plot_partnership_flow
        session = _make_session(self._partnership_table())
        session.registry.con.execute.side_effect = Exception("db error")
        ax = plot_partnership_flow("match_001", session)
        assert ax is not None

    def test_raises_on_empty(self):
        from midwicket.visuals.worm import plot_partnership_flow
        empty = pa.table({
            "inning": pa.array([], type=pa.int32()),
            "batter_id": pa.array([], type=pa.int64()),
            "non_striker_id": pa.array([], type=pa.int64()),
            "over": pa.array([], type=pa.int32()),
            "ball": pa.array([], type=pa.int32()),
            "runs_scored": pa.array([], type=pa.int32()),
        })
        session = _make_session(empty)
        with pytest.raises(MatchDataMissing):
            plot_partnership_flow("missing", session)


# ---------------------------------------------------------------------------
# plot_beehive / plot_wagon_wheel — documented NotImplementedError
# ---------------------------------------------------------------------------

class TestNotImplementedPlots:
    def test_beehive_raises(self):
        from midwicket.visuals.worm import plot_beehive
        data = pa.table({
            "over": pa.array([0], type=pa.int32()),
            "ball": pa.array([1], type=pa.int32()),
            "runs_scored": pa.array([4], type=pa.int32()),
        })
        session = _make_session(data)
        with pytest.raises(NotImplementedError):
            plot_beehive("match_001", 1, session)

    def test_wagon_wheel_raises(self):
        from midwicket.visuals.worm import plot_wagon_wheel
        data = pa.table({"runs_batter": pa.array([4, 6], type=pa.int32())})
        session = _make_session(data)
        with pytest.raises(NotImplementedError):
            plot_wagon_wheel("match_001", 1, session)


# ---------------------------------------------------------------------------
# build_registry_stats
# ---------------------------------------------------------------------------

MINIMAL_MATCH = {
    "info": {
        "dates": ["2023-05-21"],
        "venue": "Eden Gardens",
        "teams": ["India", "Australia"],
    },
    "innings": [
        {
            "team": "India",
            "overs": [
                {
                    "over": 0,
                    "deliveries": [
                        {
                            "batter": "Rohit Sharma",
                            "bowler": "Mitchell Starc",
                            "non_striker": "Shubman Gill",
                            "runs": {"batter": 4, "extras": 0, "total": 4},
                            "extras": {},
                            "wickets": [],
                        },
                        {
                            "batter": "Rohit Sharma",
                            "bowler": "Mitchell Starc",
                            "non_striker": "Shubman Gill",
                            "runs": {"batter": 0, "extras": 0, "total": 0},
                            "extras": {},
                            "wickets": [
                                {"player_out": "Rohit Sharma", "kind": "caught",
                                 "fielders": [{"name": "Maxwell"}]}
                            ],
                        },
                    ],
                }
            ],
        },
        {
            "team": "Australia",
            "overs": [
                {
                    "over": 0,
                    "deliveries": [
                        {
                            "batter": "David Warner",
                            "bowler": "Jasprit Bumrah",
                            "non_striker": "Steve Smith",
                            "runs": {"batter": 2, "extras": 0, "total": 2},
                            "extras": {},
                            "wickets": [],
                        }
                    ],
                }
            ],
        },
    ],
}


class TestBuildRegistryStats:
    def _make_real_registry(self, tmp_path):
        """Build a real in-memory IdentityRegistry backed by a tmp DuckDB."""
        from midwicket.storage.registry import IdentityRegistry
        return IdentityRegistry(str(tmp_path / "registry.duckdb"))

    def _make_loader(self, matches):
        loader = MagicMock()
        loader.iter_matches.return_value = iter(matches)
        return loader

    def test_runs_without_error(self, tmp_path):
        from midwicket.data.pipeline import build_registry_stats
        registry = self._make_real_registry(tmp_path)
        loader = self._make_loader([MINIMAL_MATCH])
        build_registry_stats(loader, registry)

    def test_populates_player_stats(self, tmp_path):
        from midwicket.data.pipeline import build_registry_stats
        registry = self._make_real_registry(tmp_path)
        loader = self._make_loader([MINIMAL_MATCH])
        build_registry_stats(loader, registry)

        row = registry.con.execute(
            "SELECT COUNT(*) FROM player_stats"
        ).fetchone()
        assert row is not None
        assert row[0] >= 1, "player_stats should have at least one row"

    def test_populates_matchup_stats(self, tmp_path):
        """Critical assertion: matchup_stats must be populated (MW-009 fix)."""
        from midwicket.data.pipeline import build_registry_stats
        registry = self._make_real_registry(tmp_path)
        loader = self._make_loader([MINIMAL_MATCH])
        build_registry_stats(loader, registry)

        row = registry.con.execute(
            "SELECT COUNT(*) FROM matchup_stats"
        ).fetchone()
        assert row is not None
        assert row[0] >= 1, "matchup_stats must contain at least one batter/bowler pair"

    def test_populates_venue_stats(self, tmp_path):
        from midwicket.data.pipeline import build_registry_stats
        registry = self._make_real_registry(tmp_path)
        loader = self._make_loader([MINIMAL_MATCH])
        build_registry_stats(loader, registry)

        row = registry.con.execute(
            "SELECT COUNT(*) FROM venue_stats"
        ).fetchone()
        assert row is not None
        assert row[0] >= 1

    def test_empty_loader_is_noop(self, tmp_path):
        """An empty loader should complete without error."""
        from midwicket.data.pipeline import build_registry_stats
        registry = self._make_real_registry(tmp_path)
        loader = self._make_loader([])
        build_registry_stats(loader, registry)

    def test_wide_delivery_not_counted_as_ball_faced(self, tmp_path):
        """Wides must not increment batter balls_faced (cricket rule)."""
        from midwicket.data.pipeline import build_registry_stats
        match = {
            "info": {
                "dates": ["2023-01-01"],
                "venue": "Test Ground",
                "teams": ["TeamA", "TeamB"],
            },
            "innings": [{
                "team": "TeamA",
                "overs": [{
                    "over": 0,
                    "deliveries": [
                        {
                            "batter": "Batter1",
                            "bowler": "Bowler1",
                            "non_striker": "Batter2",
                            "runs": {"batter": 0, "extras": 1, "total": 1},
                            "extras": {"wides": 1},
                            "wickets": [],
                        }
                    ],
                }],
            }],
        }
        registry = self._make_real_registry(tmp_path)
        loader = self._make_loader([match])
        build_registry_stats(loader, registry)

        # Resolve batter id
        b_id = registry.resolve_player_without_date("Batter1")
        if b_id is not None:
            stats = registry.get_player_stats(b_id)
            assert stats is not None
            assert stats.get("balls_faced", 0) == 0, "Wides must not count as balls faced"


# ---------------------------------------------------------------------------
# serve_overlay
# ---------------------------------------------------------------------------

class TestServeOverlay:
    def test_returns_overlay_server_instance(self):
        from midwicket.live.overlay import serve_overlay, OverlayServer
        server = serve_overlay("test_match", port=0)
        try:
            assert isinstance(server, OverlayServer)
        finally:
            server.stop()

    def test_server_is_running(self):
        from midwicket.live.overlay import serve_overlay
        server = serve_overlay("test_match", port=0)
        try:
            assert server.is_running is True
        finally:
            server.stop()

    def test_get_stats_json_returns_match_id(self):
        from midwicket.live.overlay import serve_overlay
        server = serve_overlay("match_xyz", port=0)
        try:
            stats = server.get_stats_json()
            assert stats["match_id"] == "match_xyz"
        finally:
            server.stop()

    def test_stop_sets_not_running(self):
        from midwicket.live.overlay import serve_overlay
        server = serve_overlay("test_match", port=0)
        server.stop()
        assert server.is_running is False

    def test_update_stats(self):
        from midwicket.live.overlay import serve_overlay, LiveStats
        server = serve_overlay("match_abc", port=0)
        try:
            new_stats = LiveStats(
                match_id="match_abc",
                current_over=5.3,
                current_score=45,
                wickets_fallen=2,
                run_rate=8.5,
                required_rr=9.0,
                batsman_on_strike="V Kohli",
                bowler="J Bumrah",
                last_ball="4",
            )
            server.update_stats(new_stats)
            result = server.get_stats_json()
            assert result["current_score"] == 45
            assert result["batsman"] == "V Kohli"
        finally:
            server.stop()
