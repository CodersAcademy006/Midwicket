import pyarrow as pa
from pypitch.storage.engine import QueryEngine

class DerivedStore:
    def __init__(self, engine: QueryEngine):
        self.engine = engine
        self._init_schema()

    def _init_schema(self):
        """Ensure the 'derived' schema exists in DuckDB."""
        self.engine.con.execute("CREATE SCHEMA IF NOT EXISTS derived;")

    def ensure_materialized(self, table_name: str, snapshot_id: str):
        """
        Ensures the requested derived table exists in the 'derived' schema.
        If not, it computes it and persists it for the session.
        """
        # Check if table exists
        exists = self.engine.con.execute(f"""
            SELECT count(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'derived' AND table_name = '{table_name}'
        """).fetchone()[0] > 0

        if exists:
            return

        # Dispatch to specific builder based on table name
        if table_name == "venue_baselines":
            self._build_venue_baselines(snapshot_id)
        else:
            raise ValueError(f"Unknown derived table: {table_name}")

    def _build_venue_baselines(self, snapshot_id: str):
        """
        Materializes venue baselines into derived.venue_baselines.
        """
        # Correcting the formula to match 'venue_avg_sr' expectation
        query = """
        CREATE OR REPLACE TABLE derived.venue_baselines AS
        SELECT 
            venue_id, 
            (SUM(runs_batter + runs_extras) / COUNT(*)) * 100 as venue_avg_sr
        FROM ball_events 
        GROUP BY venue_id
        """
        self.engine.con.execute(query)

    def get_venue_baselines(self, snapshot_id: str) -> pa.Table:
        """
        Returns (venue_id, avg_runs_per_over)
        Cached automatically by the engine's standard caching logic (if we were using the Executor).
        Here we query the engine directly.
        """
        # Note: In a real system, we might want to filter by snapshot_id if we had multiple snapshots loaded.
        # For now, we assume the engine has the correct snapshot loaded or we query the 'ball_events' table directly.
        # The user's snippet used 'balls' but our table is 'ball_events'.
        
        query = """
        SELECT 
            venue_id, 
            AVG(runs_batter + runs_extras) * 6 as avg_runs_per_over
        FROM ball_events 
        GROUP BY venue_id
        """
        return self.engine.execute_sql(query)
