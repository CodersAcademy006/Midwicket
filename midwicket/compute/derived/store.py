import pyarrow as pa
from midwicket.storage.engine import QueryEngine


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
        Ensures the requested derived table exists and is fresh for snapshot_id.
        Rebuilds when the table exists but was built for a different snapshot.
        """
        sql = (
            "SELECT count(*) as count "
            "FROM information_schema.tables "
            "WHERE table_schema = 'derived' AND table_name = ?"
        )
        table = self.engine.execute_sql(sql, params=[table_name], read_only=True)
        exists = table["count"][0].as_py() > 0

        # Return early only when the table exists AND was built for this exact snapshot.
        if exists and self.engine.derived_versions.get(table_name) == snapshot_id:
            return

        # Build (or rebuild for a new snapshot).
        if table_name == "venue_baselines":
            self._build_venue_baselines(snapshot_id)
            if hasattr(self.engine, "_derived_versions"):
                import threading
                lock = getattr(self.engine, "_state_lock", None)
                if lock is not None:
                    with lock:
                        self.engine._derived_versions[table_name] = snapshot_id
                else:
                    self.engine._derived_versions[table_name] = snapshot_id
            else:
                self.engine.derived_versions[table_name] = snapshot_id
        else:
            raise ValueError(f"Unknown derived table: {table_name}")

    def _build_venue_baselines(self, snapshot_id: str) -> None:
        """Materialises venue baselines into derived.venue_baselines."""
        query = """
        CREATE OR REPLACE TABLE derived.venue_baselines AS
        SELECT
            venue_id,
            CASE WHEN SUM(CASE WHEN extras_type = 'wides' THEN 0 ELSE 1 END) = 0 THEN 0.0
                 ELSE (SUM(runs_batter) / SUM(CASE WHEN extras_type = 'wides' THEN 0 ELSE 1 END)) * 100
            END as venue_avg_sr
        FROM ball_events
        GROUP BY venue_id
        """
        self.engine.execute_sql(query, read_only=False)

    def get_venue_baselines(self) -> pa.Table:
        """Returns venue-level average runs per over from ball_events."""
        query = """
        SELECT
            venue_id,
            AVG(runs_batter + runs_extras) * 6 as avg_runs_per_over
        FROM ball_events
        GROUP BY venue_id
        """
        return self.engine.execute_sql(query)
