import pyarrow as pa
from pypitch.storage.engine import QueryEngine


class DerivedStore:
    def __init__(self, engine: QueryEngine) -> None:
        self.engine = engine
        self._init_schema()

    def _init_schema(self) -> None:
        """Ensure the 'derived' schema exists in DuckDB."""
        self.engine.execute_sql(
            "CREATE SCHEMA IF NOT EXISTS derived;", read_only=False
        )

    def ensure_materialized(self, table_name: str, snapshot_id: str) -> None:
        """
        Ensures the requested derived table exists in the 'derived' schema.
        If not, it computes it and persists it for the session.
        """
        sql = (
            "SELECT count(*) as count "
            "FROM information_schema.tables "
            "WHERE table_schema = 'derived' AND table_name = ?"
        )
        table = self.engine.execute_sql(sql, params=[table_name], read_only=True)
        exists = table["count"][0].as_py() > 0

        if exists:
            return

        # Dispatch to specific builder
        if table_name == "venue_baselines":
            self._build_venue_baselines(snapshot_id)
        else:
            raise ValueError(f"Unknown derived table: {table_name}")

    def _build_venue_baselines(self, snapshot_id: str) -> None:
        """Materialises venue baselines into derived.venue_baselines."""
        query = """
        CREATE OR REPLACE TABLE derived.venue_baselines AS
        SELECT
            venue_id,
            (SUM(runs_batter + runs_extras) / COUNT(*)) * 100 as venue_avg_sr
        FROM ball_events
        GROUP BY venue_id
        """
        self.engine.execute_sql(query, read_only=False)

    def get_venue_baselines(self, snapshot_id: str) -> pa.Table:
        """Returns (venue_id, avg_runs_per_over)."""
        query = """
        SELECT
            venue_id,
            AVG(runs_batter + runs_extras) * 6 as avg_runs_per_over
        FROM ball_events
        GROUP BY venue_id
        """
        return self.engine.execute_sql(query)
