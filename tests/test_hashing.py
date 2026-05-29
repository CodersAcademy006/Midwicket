import unittest
from midwicket.query.defs import MatchupQuery

class TestDeterministicHashing(unittest.TestCase):
    def test_hash_stability(self):
        from unittest.mock import MagicMock
        from midwicket.runtime.executor import RuntimeExecutor

        # Intent A
        q1 = MatchupQuery(batter_id="1", bowler_id="2")
        h1 = q1.cache_key
        
        # Intent B
        q2 = MatchupQuery(batter_id="1", bowler_id="2")
        h2 = q2.cache_key
        
        self.assertEqual(h1, h2, "Hashes must be identical for identical intent")

        # Intent C (Different Snapshot via executor binding)
        mock_cache = MagicMock()
        engine_v1 = MagicMock()
        engine_v1.snapshot_id = "snap-1"
        engine_v1.derived_versions = {}
        executor_v1 = RuntimeExecutor(mock_cache, engine_v1)
        h1_bound = executor_v1._bind_data_versions(h1, engine_v1.snapshot_id)

        engine_v2 = MagicMock()
        engine_v2.snapshot_id = "snap-2"
        engine_v2.derived_versions = {}
        executor_v2 = RuntimeExecutor(mock_cache, engine_v2)
        h3_bound = executor_v2._bind_data_versions(h1, engine_v2.snapshot_id)
        
        self.assertNotEqual(h1_bound, h3_bound, "Bound cache key must change if Snapshot ID changes")

if __name__ == "__main__":
    unittest.main()

