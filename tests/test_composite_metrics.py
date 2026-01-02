import unittest
import pyarrow as pa
from datetime import date
from pypitch.storage.engine import QueryEngine
from pypitch.storage.registry import IdentityRegistry
from pypitch.runtime.executor import RuntimeExecutor
from pypitch.runtime.cache_duckdb import DuckDBCache
from pypitch.query.defs import MatchupQuery
from pypitch.core.canonicalize import canonicalize_match
from pypitch.compute.decorators import requires
from pypitch.compute.metrics.batting import relative_strike_rate
import pyarrow.compute as pc

# Define a test metric with dependencies
@requires("venue_baselines", join_key="venue_id")
def metric_rsr(events: pa.Table):
    # This assertion proves the JOIN happened
    if "venue_avg_sr" not in events.column_names:
        raise AssertionError("Dependency column 'venue_avg_sr' missing! Columns: " + str(events.column_names))
    
    balls = len(events)
    if balls == 0:
        return 0.0

    # Calculate simplistic RSR
    # Player SR = (Total Runs / Balls) * 100
    # RSR = Player SR / Venue Avg SR
    
    total_runs = (pc.sum(events['runs_batter']).as_py() or 0) + (pc.sum(events['runs_extras']).as_py() or 0)
    
    player_sr = (total_runs / balls) * 100.0
    
    # Venue Avg SR is repeated for each row, take mean
    venue_avg_sr = pc.mean(events['venue_avg_sr']).as_py()
    
    if not venue_avg_sr or venue_avg_sr == 0:
        return 0.0
        
    return player_sr / venue_avg_sr

class TestCompositeMetrics(unittest.TestCase):
    
    def setUp(self):
        self.registry = IdentityRegistry(":memory:")
        self.engine = QueryEngine(":memory:")
        self.cache = DuckDBCache(":memory:")
        self.executor = RuntimeExecutor(self.cache, self.engine)

    def _create_dummy_match(self, match_id, date_str, venue_name, batter_name, bowler_name, runs):
        return {
            "info": {
                "match_type_number": match_id,
                "dates": [date_str],
                "venue": venue_name,
                "teams": ["Team A", "Team B"]
            },
            "innings": [
                {
                    "team": "Team A",
                    "overs": [
                        {
                            "over": 1, 
                            "deliveries": [
                                {
                                    "batter": batter_name,
                                    "bowler": bowler_name,
                                    "non_striker": "Non Striker",
                                    "runs": {"batter": runs, "extras": 0, "total": runs},
                                    "wickets": []
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    def test_executor_injects_dependencies(self):
        print("\n🧪 Testing Composite Metric Execution...")
        
        # 1. Ingest Data
        # Match 1: Venue A, High Scoring (6 runs)
        m1 = self._create_dummy_match(101, "2024-01-01", "Venue A", "Player X", "Bowler Y", 6)
        t1 = canonicalize_match(m1, self.registry)
        
        # Match 2: Venue A, Low Scoring (1 run) - to set baseline
        m2 = self._create_dummy_match(102, "2024-01-02", "Venue A", "Player Z", "Bowler Y", 1)
        t2 = canonicalize_match(m2, self.registry)
        
        # Combine and Ingest
        t_combined = pa.concat_tables([t1, t2])
        self.engine.ingest_events(t_combined, snapshot_tag="snap_comp")
        
        # 2. Resolve IDs
        player_x_id = self.registry.resolve_player("Player X", date(2024, 1, 1))
        bowler_y_id = self.registry.resolve_player("Bowler Y", date(2024, 1, 1))
        venue_a_id = self.registry.resolve_venue("Venue A", date(2024, 1, 1))
        
        # 3. Query for Player X at Venue A
        # Player X scored 6 runs off 1 ball. SR = 600.
        # Venue A total: 7 runs off 2 balls. Avg SR = 350.
        # Expected RSR = 600 / 350 = 1.71
        
        q = MatchupQuery(
            batter_id=str(player_x_id), 
            bowler_id=str(bowler_y_id), # Filter by bowler too
            snapshot_id="snap_comp"
        )
        
        # 4. Execute Metric
        # We test both the local dummy metric and the real imported metric
        result = self.executor.execute_metric(q, metric_rsr)
        
        print(f"RSR Result (Dummy): {result.data}")
        
        # 5. Verify
        self.assertIsNotNone(result.data)
        self.assertGreater(result.data, 0)
        self.assertAlmostEqual(result.data, 600/350, places=2)

        # 5b. Verify Real Metric
        result_real = self.executor.execute_metric(q, relative_strike_rate)
        print(f"RSR Result (Real): {result_real.data}")
        self.assertAlmostEqual(result_real.data, 600/350, places=2)
        
        # 6. Verify Caching
        # Run again, should be faster/cached
        result2 = self.executor.execute_metric(q, metric_rsr)
        self.assertEqual(result2.meta.source, "cache")
        self.assertEqual(result.data, result2.data)
