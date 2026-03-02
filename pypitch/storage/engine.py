import duckdb
import pyarrow as pa
import logging
from typing import Any, Optional
from pypitch.schema.v1 import BALL_EVENT_SCHEMA
from pypitch.storage.connection_pool import ConnectionPool

logger = logging.getLogger(__name__)

class QueryEngine:
    def __init__(self, db_path: str = ":memory:") -> None:
        """
        Initializes the DuckDB engine with connection pooling.
        :memory: is fast but volatile. Use a path for persistence.
        """
        self.db_path = db_path
        self.pool = ConnectionPool(db_path, max_connections=5)

        # Initialize database schema on first connection
        with self.pool.connection() as con:
            pass  # PRAGMAs are set in ConnectionPool._create_connection

        # State tracking for deterministic hashing
        self._snapshot_id = "initial_empty"
        self._derived_versions: dict[str, str] = {}

    @property
    def snapshot_id(self) -> str:
        return self._snapshot_id

    @property
    def derived_versions(self) -> dict[str, str]:
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
        logger.debug("ingest_events: snapshot_tag=%s append=%s incoming_rows=%s",
                     snapshot_tag, append, incoming_rows)

        # Write operations must go through write_connection() to avoid
        # concurrent-writer conflicts on the DuckDB file.
        with self.pool.write_connection() as con:
            con.register('arrow_view', arrow_table)
            try:
                exists = self._table_exists("ball_events", con)

                if append and exists:
                    con.execute("INSERT INTO ball_events SELECT * FROM arrow_view")
                else:
                    con.execute("CREATE OR REPLACE TABLE ball_events AS SELECT * FROM arrow_view")

                # Stamp the snapshot so the planner can reason about versions.
                self._snapshot_id = snapshot_tag
            finally:
                try:
                    con.unregister('arrow_view')
                except Exception:
                    pass

    def execute_sql(self, sql: str, params: Optional[list] = None, read_only: bool = True) -> pa.Table:
        """
        Execute a SQL query and return results as a PyArrow Table.
        Write operations (DDL / DML) are serialized via the write lock.
        """
        if params is None:
            params = []

        ctx = self.pool.write_connection() if not read_only else self.pool.connection()
        with ctx as con:
            if not read_only:
                con.execute(sql, params)
                return pa.Table.from_pylist([])  # DDL/DML returns no rows

            result = con.execute(sql, params).arrow()
            # Ensure we return a Table, not a RecordBatchReader
            if isinstance(result, pa.RecordBatchReader):
                return result.read_all()
            return result

    def run(self, plan: dict[str, Any]) -> pa.Table:
        """
        Executes the plan.
        """
        if "sql" in plan:
            return self.execute_sql(plan["sql"])
        raise NotImplementedError("Plan execution without SQL not implemented")

    def insert_live_delivery(self, delivery_data: dict[str, Any]) -> None:
        """
        Insert a live delivery into the dedicated ``live_events`` table.

        Live data has a different (simplified) shape from the historical V1
        schema so we keep it in a separate table rather than mixing it with
        ``ball_events``, which is reserved for Schema V1-compliant data.
        """
        with self.pool.write_connection() as con:
            # Create the live_events table if it does not yet exist.
            con.execute("""
                CREATE TABLE IF NOT EXISTS live_events (
                    match_id    VARCHAR,
                    inning      INTEGER,
                    over        INTEGER,
                    ball        INTEGER,
                    runs_total  INTEGER,
                    wickets_fallen INTEGER,
                    target      INTEGER,
                    venue       VARCHAR,
                    timestamp   DOUBLE,
                    runs_batter INTEGER DEFAULT 0,
                    runs_extras INTEGER DEFAULT 0,
                    is_wicket   BOOLEAN DEFAULT FALSE,
                    batter      VARCHAR DEFAULT '',
                    bowler      VARCHAR DEFAULT ''
                )
            """)

            con.execute("""
                INSERT INTO live_events (
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

    def table_exists(self, table_name: str, con=None) -> bool:
        """Checks if a table exists in the database."""
        if con is None:
            with self.pool.connection() as con:
                return self._table_exists(table_name, con)
        else:
            return self._table_exists(table_name, con)

    def _table_exists(self, table_name: str, con) -> bool:
        """Checks if a table exists using the provided connection."""
        try:
            # DuckDB specific query
            res = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table_name]).fetchone()
            return res[0] > 0 if res else False
        except duckdb.Error as e:
            logger.warning("Error checking table existence: %s", e)
            return False

    def close(self) -> None:
        """Close the database connection pool."""
        self.pool.close()

# Alias for backward compatibility if needed, but we will update references
StorageEngine = QueryEngine
