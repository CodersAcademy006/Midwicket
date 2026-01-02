import pytest
from pydantic import ValidationError
from pypitch.query.defs import MatchupQuery

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
            snapshot_id="2025-01-01"
        )
        q2 = MatchupQuery(
            batter_id="kohli_18", 
            bowler_id="bumrah_93", 
            snapshot_id="2025-01-01"
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
            snapshot_id="2025-01-01",
            execution_opts={"timeout": 10, "verbose": False}
        )
        q_debug = MatchupQuery(
            batter_id="kohli_18", 
            bowler_id="bumrah_93",
            snapshot_id="2025-01-01",
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
        Changing the snapshot_id MUST change the cache key.
        """
        q_v1 = MatchupQuery(batter_id="X", bowler_id="Y", snapshot_id="2024-12-31")
        q_v2 = MatchupQuery(batter_id="X", bowler_id="Y", snapshot_id="2025-01-01")
        
        assert q_v1.cache_key != q_v2.cache_key, "CRITICAL: Cache collision across data versions."

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