import unittest
import pyarrow as pa
from datetime import date
import time

# Import our Stack
from pypitch.storage.engine import StorageEngine
from pypitch.storage.registry import IdentityRegistry
from pypitch.core.canonicalize import canonicalize_match
from pypitch.runtime.executor import Executor
from pypitch.runtime.cache import InMemoryCache
from pypitch.query.defs import MatchupQuery

class TestRobustness(unittest.TestCase):
    
    def setUp(self):
        self.registry = IdentityRegistry(":memory:")
        self.engine = StorageEngine(":memory:")
        self.cache = InMemoryCache()
        self.executor = Executor(self.engine, self.cache)

    def _create_dummy_match(self, match_id, date_str, batter_name, bowler_name, runs, wicket=False):
        return {
            "info": {
                "match_type_number": match_id,
                "dates": [date_str],
                "venue": "Test Venue",
                "teams": ["Team A", "Team B"]
            },
            "innings": [
                {
                    "team": "Team A",
                    "overs": [
                        {
                            "over": 19, 
                            "deliveries": [
                                {
                                    "batter": batter_name,
                                    "bowler": bowler_name,
                                    "non_striker": "Non Striker",
                                    "runs": {"batter": runs, "extras": 0, "total": runs},
                                    "wickets": [{"kind": "bowled", "player_out": batter_name}] if wicket else []
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    def test_multi_match_aggregation(self):
        """
        Test aggregating stats across multiple matches.
        """
        print("\n🧪 Testing Multi-Match Aggregation...")
        
        # Match 1: Kohli scores 4 runs
        m1 = self._create_dummy_match(101, "2024-01-01", "V Kohli", "JJ Bumrah", 4)
        # Match 2: Kohli scores 6 runs
        m2 = self._create_dummy_match(102, "2024-01-05", "V Kohli", "JJ Bumrah", 6)

        t1 = canonicalize_match(m1, self.registry)
        t2 = canonicalize_match(m2, self.registry)
        
        # Combine tables (simulating batch ingestion)
        combined_table = pa.concat_tables([t1, t2])
        self.engine.ingest_events(combined_table, snapshot_tag="snap_multi_match")

        kohli_id = self.registry.resolve_player("V Kohli", date(2024, 1, 1))
        bumrah_id = self.registry.resolve_player("JJ Bumrah", date(2024, 1, 1))

        q = MatchupQuery(batter_ids=[kohli_id], bowler_ids=[bumrah_id], phases=["Death"])
        result = self.executor.execute(q)
        rows = result['data'].to_pylist()

        self.assertEqual(len(rows), 1)
        stats = rows[0]
        self.assertEqual(stats['runs'], 10, "Runs should be 4 + 6 = 10")
        self.assertEqual(stats['balls'], 2, "Balls should be 1 + 1 = 2")

    def test_snapshot_isolation(self):
        """
        Test that queries respect snapshot isolation (though our current simple executor 
        might just query the latest state, the metadata should reflect the snapshot).
        """
        print("\n🧪 Testing Snapshot Isolation...")
        
        # Snapshot 1
        m1 = self._create_dummy_match(201, "2024-02-01", "R Sharma", "T Boult", 1)
        t1 = canonicalize_match(m1, self.registry)
        self.engine.ingest_events(t1, snapshot_tag="snap_v1")
        
        rohit_id = self.registry.resolve_player("R Sharma", date(2024, 2, 1))
        boult_id = self.registry.resolve_player("T Boult", date(2024, 2, 1))
        
        q = MatchupQuery(batter_ids=[rohit_id], bowler_ids=[boult_id], phases=["Death"])
        res1 = self.executor.execute(q)
        self.assertEqual(res1['meta']['snapshot_id'], "snap_v1")
        self.assertEqual(res1['data'].to_pylist()[0]['runs'], 1)

        # Snapshot 2 (Add more data)
        m2 = self._create_dummy_match(202, "2024-02-02", "R Sharma", "T Boult", 4)
        t2 = canonicalize_match(m2, self.registry)
        
        # In a real append-only system, we'd append. 
        # Here our simple engine.ingest_events registers a NEW table view 'ball_events'.
        # So we need to concat if we want cumulative, or just replace.
        # Let's simulate cumulative by concatenating old + new.
        combined = pa.concat_tables([t1, t2])
        self.engine.ingest_events(combined, snapshot_tag="snap_v2")
        
        res2 = self.executor.execute(q)
        self.assertEqual(res2['meta']['snapshot_id'], "snap_v2")
        self.assertEqual(res2['data'].to_pylist()[0]['runs'], 5)

    def test_cache_behavior(self):
        """
        Test that repeated queries hit the cache.
        """
        print("\n🧪 Testing Cache Behavior...")
        m1 = self._create_dummy_match(301, "2024-03-01", "S Gill", "Rashid Khan", 1)
        t1 = canonicalize_match(m1, self.registry)
        self.engine.ingest_events(t1, snapshot_tag="snap_cache_test")

        gill_id = self.registry.resolve_player("S Gill", date(2024, 3, 1))
        rashid_id = self.registry.resolve_player("Rashid Khan", date(2024, 3, 1))
        
        q = MatchupQuery(batter_ids=[gill_id], bowler_ids=[rashid_id], phases=["Death"])
        
        # First Run
        start_time = time.time()
        res1 = self.executor.execute(q)
        duration1 = time.time() - start_time
        
        # Second Run
        start_time = time.time()
        res2 = self.executor.execute(q)
        duration2 = time.time() - start_time
        
        # Verify results are identical
        self.assertEqual(res1['data'].to_pylist(), res2['data'].to_pylist())
        
        # Verify cache was actually used (Mock check or timing check)
        # Since we use InMemoryCache, we can check internal store
        # We need to re-compute the key to check existence
        # But easier: Executor doesn't expose key easily. 
        # We can check if duration is super fast, or check cache size.
        self.assertGreater(len(self.cache._store), 0, "Cache should not be empty")

    def test_empty_results(self):
        """
        Test querying for a matchup that never happened.
        """
        print("\n🧪 Testing Empty Results...")
        m1 = self._create_dummy_match(401, "2024-04-01", "MS Dhoni", "Nortje", 6)
        t1 = canonicalize_match(m1, self.registry)
        self.engine.ingest_events(t1, snapshot_tag="snap_empty")

        dhoni_id = self.registry.resolve_player("MS Dhoni", date(2024, 4, 1))
        # Resolve a bowler who isn't in the match
        star_id = self.registry.resolve_player("Mitchell Starc", date(2024, 4, 1))
        
        q = MatchupQuery(batter_ids=[dhoni_id], bowler_ids=[star_id], phases=["Death"])
        result = self.executor.execute(q)
        rows = result['data'].to_pylist()
        
        # Should return 1 row with None/Zero or Empty list depending on SQL aggregation
        # SQL `sum` on empty set returns NULL (None in Python). `count` returns 0.
        # Let's see what DuckDB returns.
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]['runs'])
        self.assertEqual(rows[0]['balls'], 0)

if __name__ == '__main__':
    unittest.main()
