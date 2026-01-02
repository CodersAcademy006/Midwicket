import duckdb
import pyarrow as pa
from typing import Dict, Any, Optional
from pypitch.schema.v1 import BALL_EVENT_SCHEMA

class QueryEngine:
    def __init__(self, db_path: str = ":memory:") -> None:
        """
        Initializes the DuckDB engine.
        :memory: is fast but volatile. Use a path for persistence.
        """
        self.con = duckdb.connect(db_path)
        # Performance tuning for analytical workloads
        self.con.execute("PRAGMA threads=4;")
        self.con.execute("PRAGMA memory_limit='2GB';")
        
        # State tracking for deterministic hashing
        self._snapshot_id = "initial_empty"
        self._derived_versions: Dict[str, str] = {}

    @property
    def snapshot_id(self) -> str:
        return self._snapshot_id

    @property
    def derived_versions(self) -> Dict[str, str]:
        return self._derived_versions

    def ingest_events(self, arrow_table: pa.Table, snapshot_tag: str, append: bool = False) -> None:
        """
        Ingests strict Schema V1 Arrow Tables.
        Rejects anything that doesn't match the contract.
        """
        if not arrow_table.schema.equals(BALL_EVENT_SCHEMA):
            # In a real system, we might diff the schemas to give a better error
            raise ValueError("Schema Violation: Input does not match BALL_EVENT_SCHEMA v1")
        # Debug: log incoming table info
        try:
            incoming_rows = getattr(arrow_table, 'num_rows', None)
        except Exception:
            incoming_rows = None
        print(f"[QueryEngine.ingest_events] snapshot_tag={snapshot_tag} append={append} incoming_rows={incoming_rows}")

        # Registers the Arrow table as a queryable view in DuckDB
        # This is a zero-copy operation (pointers only)
        self.con.register('arrow_view', arrow_table)
        try:
            exists = self.table_exists("ball_events")
            print(f"[QueryEngine.ingest_events] ball_events exists={exists}")

            # Persist to disk
            if append and exists:
                print("[QueryEngine.ingest_events] Performing INSERT INTO ball_events FROM arrow_view")
                self.con.execute("INSERT INTO ball_events SELECT * FROM arrow_view")
            else:
                print("[QueryEngine.ingest_events] Creating or replacing ball_events from arrow_view")
                self.con.execute("CREATE OR REPLACE TABLE ball_events AS SELECT * FROM arrow_view")

            # Check resulting row count for quick verification
            try:
                res = self.con.execute("SELECT COUNT(*) FROM ball_events").fetchone()
                print(f"[QueryEngine.ingest_events] ball_events row_count_after_write={res[0] if res else 'unknown'}")
            except Exception as e:
                print(f"[QueryEngine.ingest_events] Failed to fetch row count after write: {e}")
        finally:
            try:
                self.con.unregister('arrow_view')
            except Exception:
                pass

        self._snapshot_id = snapshot_tag

    def execute_sql(self, sql: str, params: Optional[list] = None) -> pa.Table:
        """
        Executes raw SQL and returns an Arrow Table.
        Supports parameterized queries for safety.
        """
        if params is None:
            params = []
            
        result = self.con.execute(sql, params).arrow()
        # Ensure we return a Table, not a RecordBatchReader
        if isinstance(result, pa.RecordBatchReader):
            return result.read_all()
        return result

    def run(self, plan: Dict[str, Any]) -> pa.Table:
        """
        Executes the plan.
        """
        if "sql" in plan:
            return self.execute_sql(plan["sql"])
        raise NotImplementedError("Plan execution without SQL not implemented")

    def insert_live_delivery(self, delivery_data: Dict[str, Any]) -> None:
        """
        Insert live delivery data.
        """
        # Ensure table exists
        if not self.table_exists("ball_events"):
            # Create table if not exists (simplified schema for demo)
            # Note: In real app, use full schema
            self.con.execute("""
            CREATE TABLE IF NOT EXISTS ball_events (
                match_id VARCHAR, inning INTEGER, over INTEGER, ball INTEGER,
                runs_total INTEGER, wickets_fallen INTEGER, target INTEGER,
                venue VARCHAR, timestamp DOUBLE,
                runs_batter INTEGER DEFAULT 0, runs_extras INTEGER DEFAULT 0,
                is_wicket BOOLEAN DEFAULT FALSE, batter VARCHAR DEFAULT '', bowler VARCHAR DEFAULT ''
            )
            """)

        self.con.execute("""
            INSERT INTO ball_events (
                match_id, inning, over, ball, runs_total,
                wickets_fallen, target, venue, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            delivery_data['match_id'],
            delivery_data['inning'],
            delivery_data['over'],
            delivery_data['ball'],
            delivery_data['runs_total'],
            delivery_data['wickets_fallen'],
            delivery_data.get('target'),
            delivery_data.get('venue'),
            delivery_data.get('timestamp')
        ])

    def table_exists(self, table_name: str) -> bool:
        """Checks if a table exists in the database."""
        try:
            # DuckDB specific query
            res = self.con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
            return res[0] > 0 if res else False
        except Exception:
            return False

    def close(self) -> None:
        """Close the database connection."""
        self.con.close()

# Alias for backward compatibility if needed, but we will update references
StorageEngine = QueryEngine
