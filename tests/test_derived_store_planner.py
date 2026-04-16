"""Regression tests for DerivedStore and QueryPlanner synchronization."""

from typing import Any, Dict

from pypitch.compute.derived.store import DerivedStore
from pypitch.query.base import BaseQuery
from pypitch.runtime.cache_duckdb import DuckDBCache
from pypitch.runtime.executor import RuntimeExecutor
from pypitch.runtime.planner import QueryPlanner
from pypitch.storage.engine import QueryEngine


class _VenueBaselineQuery(BaseQuery):
    @property
    def requires(self) -> Dict[str, Any]:
        return {
            "preferred_tables": ["venue_baselines"],
            "fallback_table": "ball_events",
            "entities": ["venue"],
            "granularity": "match",
        }


def _engine_with_ball_events() -> QueryEngine:
    engine = QueryEngine(":memory:")
    engine.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS ball_events (
            venue_id INTEGER,
            runs_batter INTEGER,
            runs_extras INTEGER
        )
        """,
        read_only=False,
    )
    return engine


def test_ensure_materialized_updates_derived_versions() -> None:
    engine = _engine_with_ball_events()
    store = DerivedStore(engine)

    store.ensure_materialized("venue_baselines", snapshot_id="snap-1")

    assert engine.derived_versions.get("venue_baselines") == "snap-1"
    engine.close()


def test_planner_sees_materialized_table_after_ensure() -> None:
    engine = _engine_with_ball_events()
    store = DerivedStore(engine)
    planner = QueryPlanner(engine)

    store.ensure_materialized("venue_baselines", snapshot_id="snap-2")
    plan = planner.plan(_VenueBaselineQuery(snapshot_id="snap-2"))

    assert plan["strategy"] == "materialized_view"
    assert plan["target_table"] == "venue_baselines"
    engine.close()


def test_executor_reads_materialized_table_from_derived_schema() -> None:
    engine = _engine_with_ball_events()
    cache = DuckDBCache(":memory:")
    store = DerivedStore(engine)
    executor = RuntimeExecutor(cache, engine)

    try:
        store.ensure_materialized("venue_baselines", snapshot_id="snap-3")
        result = executor.execute(_VenueBaselineQuery(snapshot_id="snap-3"))

        assert result.meta.source == "compute"
    finally:
        cache.close()
        engine.close()
