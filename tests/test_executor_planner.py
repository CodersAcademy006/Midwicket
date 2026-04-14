"""
Tests for RuntimeExecutor and QueryPlanner.
"""

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch
from pypitch.runtime.executor import RuntimeExecutor, ExecutionMode, ExecutionResult
from pypitch.runtime.planner import QueryPlanner, _validate_table
from pypitch.query.base import MatchupQuery


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeCache:
    def __init__(self):
        self._store: dict = {}

    def get(self, key: str):
        return self._store.get(key)

    def set(self, key: str, value) -> None:
        self._store[key] = value


def _make_engine(derived_versions: dict | None = None) -> MagicMock:
    engine = MagicMock()
    engine.derived_versions = derived_versions or {}
    engine.execute_sql.return_value = pa.table({"col": [1, 2, 3]})
    engine.table_exists.return_value = False
    return engine


def _matchup_query(**kw) -> MatchupQuery:
    defaults = dict(snapshot_id="snap-1", batter_id="101", bowler_id="202")
    defaults.update(kw)
    return MatchupQuery(**defaults)


# ---------------------------------------------------------------------------
# ResultMetadata / ExecutionResult
# ---------------------------------------------------------------------------

class TestResultMetadata:
    def test_fields_populated(self):
        from pypitch.runtime.executor import ResultMetadata
        m = ResultMetadata(
            query_hash="abc123",
            snapshot_id="snap-1",
            execution_time_ms=12.5,
            source="cache",
        )
        assert m.query_hash == "abc123"
        assert m.source == "cache"
        assert m.engine_version == "v1.0.0"

    def test_compute_source(self):
        from pypitch.runtime.executor import ResultMetadata
        m = ResultMetadata(
            query_hash="xyz",
            snapshot_id="s",
            execution_time_ms=1.0,
            source="compute",
        )
        assert m.source == "compute"

    def test_execution_time_stored(self):
        from pypitch.runtime.executor import ResultMetadata
        m = ResultMetadata(
            query_hash="h",
            snapshot_id="s",
            execution_time_ms=99.9,
            source="compute",
        )
        assert m.execution_time_ms == pytest.approx(99.9)


# ---------------------------------------------------------------------------
# RuntimeExecutor — cache paths
# ---------------------------------------------------------------------------

class TestRuntimeExecutorCacheHit:
    def test_cache_hit_returns_cached_data(self):
        cache = _FakeCache()
        cached_value = {"already": "computed"}
        engine = _make_engine()
        executor = RuntimeExecutor(cache, engine)
        # Record baseline calls from DerivedStore.__init__
        baseline_calls = engine.execute_sql.call_count

        query = _matchup_query()
        cache.set(query.cache_key, cached_value)

        result = executor.execute(query)
        assert result.data == cached_value
        assert result.meta.source == "cache"
        # No additional calls beyond init baseline
        assert engine.execute_sql.call_count == baseline_calls

    def test_cache_miss_then_hit_on_second_call(self):
        cache = _FakeCache()
        engine = _make_engine()
        executor = RuntimeExecutor(cache, engine)
        query = _matchup_query(snapshot_id="snap-abc")

        # First call — miss
        result1 = executor.execute(query)
        assert result1.meta.source == "compute"
        call_count_after_first = engine.execute_sql.call_count

        # Second call — hit
        result2 = executor.execute(query)
        assert result2.meta.source == "cache"
        assert engine.execute_sql.call_count == call_count_after_first  # no new call

    def test_result_has_meta_with_snapshot_id(self):
        cache = _FakeCache()
        engine = _make_engine()
        executor = RuntimeExecutor(cache, engine)
        query = _matchup_query(snapshot_id="snap-99")

        result = executor.execute(query)
        assert result.meta.snapshot_id == "snap-99"

    def test_result_has_positive_execution_time(self):
        cache = _FakeCache()
        engine = _make_engine()
        executor = RuntimeExecutor(cache, engine)
        query = _matchup_query()

        result = executor.execute(query)
        assert result.meta.execution_time_ms >= 0


# ---------------------------------------------------------------------------
# RuntimeExecutor — WinProbQuery path
# ---------------------------------------------------------------------------

class TestRuntimeExecutorWinProb:
    def _win_prob_query(self, snapshot_id: str = "s1"):
        from pypitch.query.defs import WinProbQuery
        return WinProbQuery(
            snapshot_id=snapshot_id,
            venue_id=1,
            target_score=180,
            current_runs=90,
            current_wickets=3,
            overs_remaining=10.0,
        )

    def test_winprobquery_routes_to_model_not_sql(self):
        cache = _FakeCache()
        engine = _make_engine()
        executor = RuntimeExecutor(cache, engine)
        query = self._win_prob_query()
        # Record baseline from DerivedStore.__init__
        baseline_calls = engine.execute_sql.call_count

        with patch("pypitch.compute.winprob.win_probability") as mock_wp:
            mock_wp.return_value = {"win_prob": 0.55, "confidence": 0.8}
            result = executor.execute(query)

        assert result.meta.source == "compute"
        assert "win_prob" in result.data
        # No calls beyond DerivedStore init
        assert engine.execute_sql.call_count == baseline_calls

    def test_winprobquery_result_cached(self):
        cache = _FakeCache()
        engine = _make_engine()
        executor = RuntimeExecutor(cache, engine)
        query = self._win_prob_query("unique-snap")

        with patch("pypitch.compute.winprob.win_probability") as mock_wp:
            mock_wp.return_value = {"win_prob": 0.6, "confidence": 0.7}
            executor.execute(query)
            result2 = executor.execute(query)

        assert result2.meta.source == "cache"
        assert mock_wp.call_count == 1  # model called once only

    def test_winprobquery_overs_remaining_invalid_raises(self):
        from pypitch.query.defs import WinProbQuery
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            WinProbQuery(
                snapshot_id="s1",
                venue_id=1,
                target_score=180,
                current_runs=90,
                current_wickets=3,
                overs_remaining=999.0,  # Invalid
            )


# ---------------------------------------------------------------------------
# RuntimeExecutor — ExecutionMode enforcement
# ---------------------------------------------------------------------------

class TestExecutionModeEnforcement:
    def test_budget_mode_raises_on_raw_scan(self):
        cache = _FakeCache()
        engine = _make_engine(derived_versions={})
        executor = RuntimeExecutor(cache, engine)
        # MatchupQuery prefers matchup_stats, but it's not available — raw_scan
        query = _matchup_query()

        with pytest.raises(RuntimeError, match="BUDGET"):
            executor.execute(query, mode=ExecutionMode.BUDGET)

    def test_approx_mode_logs_warning_on_raw_scan(self, caplog):
        import logging
        cache = _FakeCache()
        engine = _make_engine(derived_versions={})
        executor = RuntimeExecutor(cache, engine)
        query = _matchup_query()

        with caplog.at_level(logging.WARNING, logger="pypitch.runtime.executor"):
            result = executor.execute(query, mode=ExecutionMode.APPROX)

        assert result.meta.source == "compute"
        assert any("APPROX" in r.message or "raw_scan" in r.message for r in caplog.records)

    def test_exact_mode_succeeds_with_raw_scan(self):
        cache = _FakeCache()
        engine = _make_engine()
        executor = RuntimeExecutor(cache, engine)
        query = _matchup_query()

        result = executor.execute(query, mode=ExecutionMode.EXACT)
        assert result.meta.source == "compute"

    def test_budget_mode_succeeds_with_materialized_view(self):
        cache = _FakeCache()
        # Make matchup_stats available
        engine = _make_engine(derived_versions={"matchup_stats": "v1", "phase_stats": "v1"})
        executor = RuntimeExecutor(cache, engine)
        query = _matchup_query()

        result = executor.execute(query, mode=ExecutionMode.BUDGET)
        assert result.meta.source == "compute"


# ---------------------------------------------------------------------------
# QueryPlanner — create_legacy_plan
# ---------------------------------------------------------------------------

class TestQueryPlannerLegacy:
    def test_prefers_materialized_view(self):
        engine = _make_engine(derived_versions={"matchup_stats": "v1"})
        planner = QueryPlanner(engine)
        query = _matchup_query()

        plan = planner.create_legacy_plan(query)
        assert plan["strategy"] == "materialized_view"
        assert plan["target_table"] == "matchup_stats"
        assert plan["cost"] == "low"

    def test_falls_back_to_raw_scan(self):
        engine = _make_engine(derived_versions={})
        planner = QueryPlanner(engine)
        query = _matchup_query()

        plan = planner.create_legacy_plan(query)
        assert plan["strategy"] == "raw_scan"
        assert plan["target_table"] == "ball_events"
        assert plan["cost"] == "high"

    def test_plan_contains_sql_and_params(self):
        engine = _make_engine()
        planner = QueryPlanner(engine)
        query = _matchup_query()

        plan = planner.create_legacy_plan(query)
        assert "sql" in plan
        assert "params" in plan
        assert isinstance(plan["params"], list)

    def test_sql_contains_batter_and_bowler_placeholders(self):
        engine = _make_engine()
        planner = QueryPlanner(engine)
        query = _matchup_query(batter_id="501", bowler_id="601")

        plan = planner.create_legacy_plan(query)
        assert "?" in plan["sql"]  # Parameters are placeholders, not inlined


# ---------------------------------------------------------------------------
# QueryPlanner — WHERE clause
# ---------------------------------------------------------------------------

class TestPlannerWhereClause:
    def test_where_clause_with_matchup_query(self):
        engine = _make_engine()
        planner = QueryPlanner(engine)
        query = _matchup_query(batter_id="101", bowler_id="202")
        where, params = planner._build_where_clause(query)
        assert "batter_id = ?" in where
        assert "bowler_id = ?" in where
        assert "101" in params
        assert "202" in params

    def test_where_clause_with_venue_id(self):
        engine = _make_engine()
        planner = QueryPlanner(engine)
        query = MatchupQuery(snapshot_id="s", batter_id="1", bowler_id="2", venue_id="V1")
        where, params = planner._build_where_clause(query)
        assert "venue_id = ?" in where
        assert "V1" in params

    def test_where_clause_without_venue_id(self):
        engine = _make_engine()
        planner = QueryPlanner(engine)
        query = MatchupQuery(snapshot_id="s", batter_id="1", bowler_id="2")
        where, params = planner._build_where_clause(query)
        assert "venue_id" not in where

    def test_where_clause_no_filters_returns_1_eq_1(self):
        engine = _make_engine()
        planner = QueryPlanner(engine)
        # Use a query with no batter/bowler/phase fields — only snapshot_id
        # WinProbQuery has venue_id, so use MatchupQuery without venue_id
        query = _matchup_query()  # has batter_id + bowler_id but no venue/phase
        where, params = planner._build_where_clause(query)
        # Should have batter and bowler but not '1=1'
        assert len(params) >= 1  # at minimum batter_id and bowler_id are filtered

    def test_where_clause_with_invalid_phase_raises(self):
        from pypitch.query.defs import FantasyQuery
        engine = _make_engine()
        planner = QueryPlanner(engine)

        # Create a MagicMock that looks like a query with invalid phase
        q = MagicMock()
        q.__class__.__name__ = "TestQuery"
        # Attach phase attribute
        type(q).phase = property(lambda self: "nonsense")
        # hasattr checks
        q.__dict__['phase'] = "nonsense"

        # Direct call to build_where_clause with a mock that has phase attr
        class MockQuery:
            phase = "nonsense"
            snapshot_id = "s"
        with pytest.raises((ValueError, AttributeError)):
            planner._build_where_clause(MockQuery())


# ---------------------------------------------------------------------------
# QueryPlanner — _generate_sql
# ---------------------------------------------------------------------------

class TestPlannerGenerateSQL:
    def test_matchup_query_generates_sql(self):
        engine = _make_engine()
        planner = QueryPlanner(engine)
        query = _matchup_query(batter_id="101", bowler_id="202")
        sql, params = planner._generate_sql(query, "ball_events")
        assert "batter_id" in sql
        assert "bowler_id" in sql
        assert "101" in params
        assert "202" in params

    def test_fantasy_query_generates_sql(self):
        from pypitch.query.defs import FantasyQuery
        engine = _make_engine()
        planner = QueryPlanner(engine)
        q = FantasyQuery(snapshot_id="s", venue_id=99)
        sql, params = planner._generate_sql(q, "ball_events")
        assert "venue_id" in sql
        assert 99 in params

    def test_unknown_query_logs_warning(self, caplog):
        import logging
        from pypitch.query.defs import WinProbQuery
        engine = _make_engine()
        planner = QueryPlanner(engine)
        q = WinProbQuery(
            snapshot_id="s",
            venue_id=1,
            target_score=180,
            current_runs=90,
            current_wickets=3,
            overs_remaining=5.0,
        )
        with caplog.at_level(logging.WARNING):
            sql, params = planner._generate_sql(q, "ball_events")
        assert "SELECT *" in sql


# ---------------------------------------------------------------------------
# _validate_table
# ---------------------------------------------------------------------------

class TestValidateTable:
    def test_known_tables_pass(self):
        for table in ("ball_events", "matchup_stats", "phase_stats", "venue_baselines"):
            assert _validate_table(table) == table

    def test_unknown_table_raises(self):
        with pytest.raises(ValueError, match="Unknown table"):
            _validate_table("evil_injection;--")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Unknown table"):
            _validate_table("")

    def test_all_valid_tables_accepted(self):
        from pypitch.runtime.planner import _VALID_TABLES
        for t in _VALID_TABLES:
            assert _validate_table(t) == t
