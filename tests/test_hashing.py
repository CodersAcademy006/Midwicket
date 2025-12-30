import unittest
from pypitch.query.defs import MatchupQuery
from pypitch.query.base import QueryContext

class TestDeterministicHashing(unittest.TestCase):
    def test_hash_stability(self):
        ctx = QueryContext(
            schema_version="1.0", snapshot_id="snap1", 
            planner_version="1.0", derived_versions={}
        )
        
        # Intent A
        q1 = MatchupQuery(batter_ids=[1], bowler_ids=[2], phases=["Death"])
        h1 = q1.compute_cache_key(ctx)
        
        # Intent B (Same data, different order in list definition shouldn't matter for dict dump, 
        # but standard lists DO care about order. phases=["Death"] is same.)
        q2 = MatchupQuery(batter_ids=[1], bowler_ids=[2], phases=["Death"])
        h2 = q2.compute_cache_key(ctx)
        
        self.assertEqual(h1, h2, "Hashes must be identical for identical intent")

        # Intent C (Different Context)
        ctx2 = QueryContext(
            schema_version="1.0", snapshot_id="snap2", # Snapshot changed
            planner_version="1.0", derived_versions={}
        )
        h3 = q1.compute_cache_key(ctx2)
        
        self.assertNotEqual(h1, h3, "Hash must change if Snapshot ID changes")

if __name__ == '__main__':
    unittest.main()