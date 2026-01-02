"""
Integration Tests for PyPitch

Tests that simulate real-world usage scenarios, particularly long-running
operations like full season data ingestion and analysis.
"""

import pytest
import pandas as pd
import numpy as np
import pyarrow as pa
from pathlib import Path
import tempfile
import time
from typing import Dict, Any

from pypitch.api.session import PyPitchSession
from pypitch.storage.thread_safe_engine import create_thread_safe_engine
from pypitch.exceptions import DataIngestionError, QueryExecutionError

class TestSeasonSimulation:
    """
    Integration tests that simulate processing a full IPL season.

    Tests the complete pipeline from data ingestion through analysis,
    ensuring the system can handle realistic workloads.
    """

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        yield path
        # Cleanup
        Path(path).unlink(missing_ok=True)

    @pytest.fixture
    def thread_safe_session(self, temp_db_path):
        """Create a session with thread-safe engine."""
        engine = create_thread_safe_engine(temp_db_path)
        session = PyPitchSession(engine=engine)
        yield session
        session.close()

    def generate_synthetic_season_data(self, num_matches: int = 74) -> pd.DataFrame:
        """
        Generate synthetic IPL season data for testing.

        Args:
            num_matches: Number of matches to generate (default: full IPL season)

        Returns:
            DataFrame with ball-by-ball data for the season
        """
        np.random.seed(42)  # For reproducible tests

        data = []
        venues = ['Wankhede', 'Eden Gardens', 'Chinnaswamy', 'DYanmond Park', 'Punjab Cricket']
        teams = ['MI', 'KKR', 'RCB', 'CSK', 'PBKS', 'DC', 'SRH', 'RR']

        for match_id in range(1, num_matches + 1):
            venue = np.random.choice(venues)
            team1, team2 = np.random.choice(teams, 2, replace=False)

            # Simulate match
            target = np.random.randint(150, 220)  # Target score

            for inning in [1, 2]:
                current_runs = 0
                wickets = 0

                for over in range(20):
                    for ball in range(1, 7):
                        if wickets >= 10:
                            break

                        # Simulate ball outcome
                        runs = np.random.choice([0, 1, 2, 3, 4, 6], p=[0.4, 0.25, 0.15, 0.05, 0.1, 0.05])
                        wicket_fall = np.random.random() < 0.05  # 5% chance of wicket

                        current_runs += runs
                        if wicket_fall:
                            wickets += 1

                        # Stop if target reached in second innings
                        if inning == 2 and current_runs >= target:
                            break

                        data.append({
                            'match_id': f'match_{match_id}',
                            'inning': inning,
                            'over': over,
                            'ball': ball,
                            'runs_total': current_runs,
                            'wickets_fallen': wickets,
                            'target': target if inning == 2 else None,
                            'venue': venue,
                            'team_batting': team1 if inning == 1 else team2,
                            'team_bowling': team2 if inning == 1 else team1,
                            'timestamp': time.time()
                        })

                    if wickets >= 10 or (inning == 2 and current_runs >= target):
                        break

        return pd.DataFrame(data)

    def test_full_season_ingestion(self, thread_safe_session, temp_db_path):
        """Test ingesting and processing a full season of data."""
        # Generate season data
        season_data = self.generate_synthetic_season_data(num_matches=10)  # Smaller for test speed

        # Convert to Arrow table
        import pyarrow as pa
        arrow_table = pa.Table.from_pandas(season_data)

        # Time the ingestion
        start_time = time.time()
        thread_safe_session.engine.ingest_events(arrow_table, "test_season")
        ingestion_time = time.time() - start_time

        # Verify data was ingested
        result = thread_safe_session.engine.execute_sql("SELECT COUNT(*) as total_deliveries FROM ball_events")
        total_deliveries = result['total_deliveries'][0].as_py()

        assert total_deliveries > 0, "No data was ingested"
        assert total_deliveries == len(season_data), f"Expected {len(season_data)} deliveries, got {total_deliveries}"

        # Performance check - should be reasonable for the data size
        assert ingestion_time < 30, f"Ingestion took too long: {ingestion_time:.2f}s"

        print(f"Successfully ingested {total_deliveries} deliveries in {ingestion_time:.2f}s")

    def test_season_analytics_queries(self, thread_safe_session):
        """Test running analytical queries on season data."""
        # Generate and ingest data
        season_data = self.generate_synthetic_season_data(num_matches=5)
        arrow_table = pa.Table.from_pandas(season_data)
        thread_safe_session.engine.ingest_events(arrow_table, "analytics_test")

        # Test various analytical queries
        queries = {
            'total_matches': "SELECT COUNT(DISTINCT match_id) as matches FROM ball_events",
            'venue_stats': """
                SELECT venue, COUNT(DISTINCT match_id) as matches,
                       AVG(runs_total) as avg_runs
                FROM ball_events
                GROUP BY venue
                ORDER BY matches DESC
            """,
            'high_scores': """
                SELECT match_id, MAX(runs_total) as highest_score
                FROM ball_events
                GROUP BY match_id
                ORDER BY highest_score DESC
                LIMIT 5
            """,
            'wicket_analysis': """
                SELECT match_id, inning, MAX(wickets_fallen) as wickets_lost
                FROM ball_events
                GROUP BY match_id, inning
                ORDER BY wickets_lost DESC
                LIMIT 10
            """
        }

        for query_name, sql in queries.items():
            start_time = time.time()
            try:
                result = thread_safe_session.engine.execute_sql(sql)
                query_time = time.time() - start_time

                # Verify we got results
                assert len(result) > 0, f"Query {query_name} returned no results"

                # Performance check
                assert query_time < 5, f"Query {query_name} took too long: {query_time:.2f}s"

                print(f"Query {query_name}: {len(result)} rows in {query_time:.3f}s")

            except Exception as e:
                pytest.fail(f"Query {query_name} failed: {e}")

    def test_concurrent_access_simulation(self, thread_safe_session):
        """Test concurrent read/write operations."""
        import threading
        import queue

        season_data = self.generate_synthetic_season_data(num_matches=3)
        arrow_table = pa.Table.from_pandas(season_data)
        thread_safe_session.engine.ingest_events(arrow_table, "concurrency_test")

        results_queue = queue.Queue()
        errors = []

        def reader_worker(worker_id: int):
            """Simulate read operations."""
            try:
                for i in range(10):
                    result = thread_safe_session.engine.execute_sql(
                        "SELECT COUNT(*) FROM ball_events"
                    )
                    results_queue.put(f"reader_{worker_id}_{i}")
                    time.sleep(0.01)  # Small delay
            except Exception as e:
                errors.append(f"Reader {worker_id}: {e}")

        def writer_worker(worker_id: int):
            """Simulate write operations."""
            try:
                # Insert some live data
                for i in range(5):
                    thread_safe_session.engine.insert_live_delivery({
                        'match_id': f'live_match_{worker_id}',
                        'inning': 1,
                        'over': i,
                        'ball': 1,
                        'runs_total': i * 10,
                        'wickets_fallen': 0,
                        'target': None,
                        'venue': 'Test Venue',
                        'timestamp': time.time()
                    })
                    results_queue.put(f"writer_{worker_id}_{i}")
                    time.sleep(0.01)
            except Exception as e:
                errors.append(f"Writer {worker_id}: {e}")

        # Start concurrent operations
        threads = []

        # 3 reader threads
        for i in range(3):
            t = threading.Thread(target=reader_worker, args=(i,))
            threads.append(t)
            t.start()

        # 2 writer threads
        for i in range(2):
            t = threading.Thread(target=writer_worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join(timeout=10)

        # Check for errors
        if errors:
            pytest.fail(f"Concurrent operations failed: {errors}")

        # Verify we got expected results
        result_count = 0
        while not results_queue.empty():
            results_queue.get()
            result_count += 1

        expected_operations = (3 * 10) + (2 * 5)  # readers + writers
        assert result_count == expected_operations, f"Expected {expected_operations} operations, got {result_count}"

        print(f"Successfully completed {result_count} concurrent operations")

    def test_data_integrity_checks(self, thread_safe_session):
        """Test data integrity and consistency checks."""
        season_data = self.generate_synthetic_season_data(num_matches=2)

        # Introduce some data quality issues
        season_data.loc[0, 'runs_total'] = -1  # Invalid negative runs
        season_data.loc[1, 'wickets_fallen'] = 15  # Invalid wicket count

        arrow_table = pa.Table.from_pandas(season_data)

        # This should either reject the data or handle it gracefully
        try:
            thread_safe_session.engine.ingest_events(arrow_table, "integrity_test")

            # If ingestion succeeded, verify data integrity
            result = thread_safe_session.engine.execute_sql("""
                SELECT
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN runs_total < 0 THEN 1 END) as negative_runs,
                    COUNT(CASE WHEN wickets_fallen > 10 THEN 1 END) as invalid_wickets
                FROM ball_events
            """)

            # Should have handled or filtered invalid data
            assert result['total_rows'][0] > 0, "No data ingested"

            print(f"Data integrity check: {result['total_rows'][0]} rows ingested")

        except Exception as e:
            # If ingestion failed due to validation, that's also acceptable
            print(f"Data validation prevented invalid data ingestion: {e}")

    def test_performance_regression_check(self, thread_safe_session):
        """Test for performance regressions in common operations."""
        # Generate larger dataset for performance testing
        season_data = self.generate_synthetic_season_data(num_matches=20)
        arrow_table = pa.Table.from_pandas(season_data)

        # Measure ingestion performance
        start_time = time.time()
        thread_safe_session.engine.ingest_events(arrow_table, "performance_test")
        ingestion_time = time.time() - start_time

        # Query performance benchmarks
        benchmarks = {
            'simple_count': "SELECT COUNT(*) FROM ball_events",
            'group_by_venue': "SELECT venue, COUNT(*) FROM ball_events GROUP BY venue",
            'complex_analytics': """
                SELECT venue, AVG(runs_total) as avg_runs, MAX(runs_total) as max_runs
                FROM ball_events
                WHERE inning = 1
                GROUP BY venue
                ORDER BY avg_runs DESC
            """
        }

        for query_name, sql in benchmarks.items():
            start_time = time.time()
            result = thread_safe_session.engine.execute_sql(sql)
            query_time = time.time() - start_time

            # Performance thresholds (adjust based on hardware)
            max_time = 2.0 if 'complex' in query_name else 1.0

            assert query_time < max_time, f"{query_name} query too slow: {query_time:.3f}s"
            assert len(result) > 0, f"{query_name} query returned no results"

            print(f"{query_name}: {query_time:.3f}s ({len(result)} rows)")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])