"""End-to-end smoke test exercising the REAL pipeline.

This is the regression net that would have caught the breakage that shipped to
`main` (the package failing to import, and `ball_events` schema drift). It runs
the production path — ``canonicalize_match`` → ``QueryEngine.ingest_events`` —
rather than a hand-built table, so it stays honest about the real v1 schema.

Keep these assertions tied to *currently correct* behavior. As remediation PRs
land (schema unification, analytics on v1, etc.), extend this file with the
stronger end-to-end assertions they unlock.
"""

import pyarrow as pa
import pytest

from midwicket.core.canonicalize import canonicalize_match
from midwicket.schema.v1 import BALL_EVENT_SCHEMA
from midwicket.storage.engine import QueryEngine
from midwicket.storage.registry import IdentityRegistry
from midwicket.compute.winprob import win_probability


def _sample_match() -> dict:
    """A minimal but Cricsheet-shaped single-innings match."""
    return {
        "info": {
            "teams": ["Team A", "Team B"],
            "dates": ["2023-05-21"],
            "venue": "Test Stadium",
        },
        "innings": [
            {
                "team": "Team A",
                "overs": [
                    {
                        "over": 0,
                        "deliveries": [
                            {"batter": "AB Player", "bowler": "XY Bowler",
                             "non_striker": "CD Player",
                             "runs": {"batter": 4, "extras": 0, "total": 4}},
                            {"batter": "AB Player", "bowler": "XY Bowler",
                             "non_striker": "CD Player",
                             "runs": {"batter": 1, "extras": 0, "total": 1}},
                        ],
                    }
                ],
            }
        ],
    }


def test_package_imports():
    """A bare ``import midwicket`` must succeed (guards against half-commits)."""
    import midwicket  # noqa: F401


def test_real_ingest_emits_v1_schema_and_is_queryable():
    reg = IdentityRegistry(":memory:")
    eng = QueryEngine(":memory:")
    try:
        table = canonicalize_match(_sample_match(), reg, match_id="m1")
        # The canonical contract is the v1 schema (IDs, not names).
        assert isinstance(table, pa.Table)
        assert set(BALL_EVENT_SCHEMA.names).issubset(set(table.schema.names))
        assert "batter_id" in table.schema.names

        eng.ingest_events(table, snapshot_tag="m1")
        rows = eng.execute_sql("SELECT COUNT(*) AS n FROM ball_events").to_pydict()
        assert rows["n"][0] == 2
    finally:
        eng.close()
        reg.close()


def test_win_probability_returns_bounded_values():
    out = win_probability(target=180, current_runs=90, wickets_down=3, overs_done=10.0)
    assert 0.0 <= out["win_prob"] <= 1.0
    assert 0.0 <= out["confidence"] <= 1.0


def test_sql_guard_blocks_dangerous_queries_but_allows_allowlisted():
    # sql_guard depends on serve extras (fastapi/uvicorn) which may not be
    # installed in core-deps-only CI environments.
    sql_guard = pytest.importorskip("midwicket.serve.sql_guard")
    validate_read_only_query = sql_guard.validate_read_only_query
    SQLValidationError = sql_guard.SQLValidationError

    with pytest.raises(SQLValidationError):
        validate_read_only_query("SELECT * FROM ball_events; DROP TABLE ball_events")
    with pytest.raises(SQLValidationError):
        validate_read_only_query("SELECT * FROM information_schema.tables")
    # An allowlisted read-only query is accepted and returned normalized.
    stmt = validate_read_only_query("SELECT batter_id, runs_batter FROM ball_events LIMIT 5")
    assert "ball_events" in stmt.lower()
