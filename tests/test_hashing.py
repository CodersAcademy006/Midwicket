import unittest
from pypitch.query.defs import MatchupQuery

class TestDeterministicHashing(unittest.TestCase):
    def test_hash_stability(self):
        # Intent A
        q1 = MatchupQuery(batter_id="1", bowler_id="2", snapshot_id="snap1")
        h1 = q1.cache_key
        
        # Intent B
        q2 = MatchupQuery(batter_id="1", bowler_id="2", snapshot_id="snap1")
        h2 = q2.cache_key
        
        self.assertEqual(h1, h2, "Hashes must be identical for identical intent")

        # Intent C (Different Snapshot)
        q3 = MatchupQuery(batter_id="1", bowler_id="2", snapshot_id="snap2")
        h3 = q3.cache_key
        
        self.assertNotEqual(h1, h3, "Hash must change if Snapshot ID changes")

if __name__ == "__main__":
    unittest.main()

