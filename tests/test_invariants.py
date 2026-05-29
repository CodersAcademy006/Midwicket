import pytest
from pydantic import ValidationError
from midwicket.query.base import BaseQuery
from midwicket.query.defs import MatchupQuery

class TestArchitecturalInvariants:
    
    def test_invariant_query_stability(self):
        """
        INVARIANT 1: Reproducibility
        Two query objects created with identical parameters must produce 
        identically the same cache key.
        """
        q1 = MatchupQuery(
            batter_id="kohli_18", 
            bowler_id="bumrah_93", 
        )
        q2 = MatchupQuery(
            batter_id="kohli_18", 
            bowler_id="bumrah_93", 
        )
        
        assert q1.cache_key == q2.cache_key, "CRITICAL: Identical intents produced different cache keys."

    def test_invariant_runtime_isolation(self):
        """
        INVARIANT 2: Runtime Policy Isolation
        Changing execution parameters (verbosity, timeout, memory limits) 
        MUST NOT change the data cache key.
        
        If this fails, you are re-computing data just because I asked for a progress bar.
        """
        q_strict = MatchupQuery(
            batter_id="kohli_18", 
            bowler_id="bumrah_93",
            execution_opts={"timeout": 10, "verbose": False}
        )
        q_debug = MatchupQuery(
            batter_id="kohli_18", 
            bowler_id="bumrah_93",
            execution_opts={"timeout": 999, "verbose": True}
        )
        
        # The objects are different...
        assert q_strict != q_debug
        # ...but the DATA signature must be identical.
        assert q_strict.cache_key == q_debug.cache_key, \
            "CRITICAL: Runtime options leaked into cache key. This destroys cache efficiency."

    def test_invariant_snapshot_sensitivity(self):
        """
        INVARIANT 3: Explicit Context
        Changing the snapshot_id MUST change the executor's bound cache key.
        """
        from unittest.mock import MagicMock
        from midwicket.runtime.executor import RuntimeExecutor

        q = MatchupQuery(batter_id="X", bowler_id="Y")

        mock_cache = MagicMock()

        engine_v1 = MagicMock()
        engine_v1.snapshot_id = "snap-1"
        engine_v1.derived_versions = {}
        executor_v1 = RuntimeExecutor(mock_cache, engine_v1)
        key_v1 = executor_v1._bind_data_versions(q.cache_key, engine_v1.snapshot_id)

        engine_v2 = MagicMock()
        engine_v2.snapshot_id = "snap-2"
        engine_v2.derived_versions = {}
        executor_v2 = RuntimeExecutor(mock_cache, engine_v2)
        key_v2 = executor_v2._bind_data_versions(q.cache_key, engine_v2.snapshot_id)

        assert key_v1 != key_v2, "CRITICAL: Cache collision across data versions."

    def test_invariant_query_type_isolation(self):
        """
        INVARIANT 3b: Query Type Isolation
        Different query classes with identical field payloads must not share
        a cache key, or cross-query cache poisoning can occur.
        """

        class QueryA(BaseQuery):
            @property
            def requires(self):
                return {
                    "preferred_tables": ["matchup_stats"],
                    "fallback_table": "ball_events",
                    "entities": ["batter", "bowler"],
                    "granularity": "ball",
                }

        class QueryB(BaseQuery):
            @property
            def requires(self):
                return {
                    "preferred_tables": ["phase_stats"],
                    "fallback_table": "ball_events",
                    "entities": ["batter"],
                    "granularity": "ball",
                }

        qa = QueryA()
        qb = QueryB()

        assert qa.cache_key != qb.cache_key, (
            "CRITICAL: Different query classes produced the same cache key."
        )

    def test_invariant_schema_strictness(self):
        """
        INVARIANT 4: No Hidden State
        The query object must forbid arbitrary arguments.
        """
        with pytest.raises(ValidationError):
            MatchupQuery(
                batter_id="X", 
                bowler_id="Y", 
                magic_parameter="please_work" # Should fail
            )

    def test_invariant_execution_timeout_accepts_fractional_seconds(self):
        """Execution options should support sub-second timeout precision."""
        query = MatchupQuery(
            batter_id="kohli_18",
            bowler_id="bumrah_93",
            execution_opts={"timeout": 0.25},
        )

        assert query.execution_opts.timeout == pytest.approx(0.25)