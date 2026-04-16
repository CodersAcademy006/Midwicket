"""
Thread-Safe Query Engine with Connection Pooling

Addresses concurrency issues by providing separate read/write connections
and managing them safely across multiple threads.
"""

import duckdb
import pyarrow as pa
from typing import Dict, Any, Optional, List
import threading
import queue
import time
import uuid
from datetime import date
from contextlib import contextmanager

from .engine import QueryEngine
from ..exceptions import ConnectionError, QueryTimeoutError, DataIngestionError

class ConnectionPool:
    """
    Thread-safe connection pool for DuckDB.

    Maintains separate pools for read and write operations.
    """

    def __init__(self, db_path: str = ":memory:", max_connections: int = 10,
                 read_pool_size: int = 5, write_pool_size: int = 2):
        self.db_path = db_path
        self._connect_path = db_path
        if db_path == ":memory:":
            # DuckDB's plain :memory: creates isolated databases per connection.
            # Use a unique named in-memory database so all pooled connections
            # share the same state within this engine instance.
            self._connect_path = f":memory:pypitch_pool_{uuid.uuid4().hex}"
        self.max_connections = max_connections
        self.read_pool_size = read_pool_size
        self.write_pool_size = write_pool_size

        # Connection pools
        self.read_pool: queue.Queue = queue.Queue(maxsize=read_pool_size)
        self.write_pool: queue.Queue = queue.Queue(maxsize=write_pool_size)

        # Pool management
        self._lock = threading.RLock()
        self._created_connections = 0

        # Initialize pools
        self._initialize_pools()

    def _initialize_pools(self):
        """Initialize connection pools."""
        # For file-based databases, we need to create the database first with a write connection
        if self.db_path != ":memory:":
            # Create the database file if it doesn't exist or is invalid
            import os
            if not os.path.exists(self.db_path):
                temp_conn = duckdb.connect(self.db_path)
                temp_conn.close()
            else:
                # Try to connect and create if invalid
                try:
                    temp_conn = duckdb.connect(self.db_path, read_only=True)
                    temp_conn.close()
                except Exception:
                    # File exists but is invalid, recreate it
                    os.remove(self.db_path)
                    temp_conn = duckdb.connect(self.db_path)
                    temp_conn.close()

        # Create read connections
        for _ in range(self.read_pool_size):
            conn = self._create_connection(read_only=True)
            self.read_pool.put(conn)

        # Create write connections
        for _ in range(self.write_pool_size):
            conn = self._create_connection(read_only=False)
            self.write_pool.put(conn)

    def _create_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """Create a new DuckDB connection with appropriate settings."""
        # Always create read-write connections to avoid configuration conflicts
        # We manage read/write separation via the pools
        conn = duckdb.connect(self._connect_path, read_only=False)

        # Performance tuning
        conn.execute("PRAGMA threads=2;")  # Reduced for connection pooling
        conn.execute("PRAGMA memory_limit='1GB';")

        with self._lock:
            self._created_connections += 1

        return conn

    @contextmanager
    def get_read_connection(self, timeout: float = 5.0):
        """Get a read connection from the pool."""
        conn = None
        try:
            conn = self.read_pool.get(timeout=timeout)
            yield conn
        except queue.Empty:
            raise ConnectionError("No read connections available (pool exhausted)")
        finally:
            if conn:
                try:
                    self.read_pool.put(conn, timeout=1.0)
                except queue.Full:
                    # Pool is full, close this connection
                    conn.close()

    @contextmanager
    def get_write_connection(self, timeout: float = 5.0):
        """Get a write connection from the pool."""
        conn = None
        try:
            conn = self.write_pool.get(timeout=timeout)
            yield conn
        except queue.Empty:
            raise ConnectionError("No write connections available (pool exhausted)")
        finally:
            if conn:
                try:
                    self.write_pool.put(conn, timeout=1.0)
                except queue.Full:
                    # Pool is full, close this connection
                    conn.close()

    def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        return {
            'read_pool_size': self.read_pool.qsize(),
            'write_pool_size': self.write_pool.qsize(),
            'total_created': self._created_connections,
            'max_connections': self.max_connections
        }

    def close(self):
        """Close all connections in the pools."""
        # Close read connections
        while not self.read_pool.empty():
            try:
                conn = self.read_pool.get_nowait()
                conn.close()
            except queue.Empty:
                break

        # Close write connections
        while not self.write_pool.empty():
            try:
                conn = self.write_pool.get_nowait()
                conn.close()
            except queue.Empty:
                break

class ThreadSafeQueryEngine:
    """
    Thread-safe version of QueryEngine using connection pooling.

    Supports concurrent read operations and serialized write operations.
    """

    def __init__(self, db_path: str = ":memory:", pool_config: Dict[str, Any] = None):
        if pool_config is None:
            pool_config = {}

        self.db_path = db_path
        self.pool = ConnectionPool(db_path, **pool_config)

        # State tracking (needs to be thread-safe)
        self._snapshot_id = "initial_empty"
        self._derived_versions: Dict[str, str] = {}
        self._state_lock = threading.RLock()

        # Initialize database schema if needed
        self._ensure_schema()

    def _ensure_schema(self):
        """Ensure basic schema exists."""
        with self.pool.get_write_connection() as conn:
            # Create basic tables if they don't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ball_events (
                    match_id VARCHAR,
                    inning INTEGER,
                    over INTEGER,
                    ball INTEGER,
                    runs_total INTEGER,
                    wickets_fallen INTEGER,
                    target INTEGER,
                    venue VARCHAR,
                    timestamp DOUBLE
                )
            """)

    @property
    def snapshot_id(self) -> str:
        with self._state_lock:
            return self._snapshot_id

    @property
    def derived_versions(self) -> Dict[str, str]:
        with self._state_lock:
            # Return the underlying mapping for compatibility with QueryEngine
            # and DerivedStore, which mutate this dict directly.
            return self._derived_versions

    def ingest_events(self, arrow_table: pa.Table, snapshot_tag: str, append: bool = False) -> None:
        """
        Thread-safe ingestion of events.
        Write operations are serialized through the connection pool.
        """
        with self.pool.get_write_connection() as conn:
            # Register the Arrow table
            conn.register('arrow_view', arrow_table)

            try:
                exists = self._table_exists_conn(conn, "ball_events")

                # Persist to disk
                if append and exists:
                    conn.execute("INSERT INTO ball_events SELECT * FROM arrow_view")
                else:
                    conn.execute("CREATE OR REPLACE TABLE ball_events AS SELECT * FROM arrow_view")

                # New source data invalidates any previously materialized
                # derived state to avoid stale table reuse.
                self._invalidate_derived_state_conn(conn)

            finally:
                try:
                    conn.unregister('arrow_view')
                except Exception:  # nosec B110 — view may not be registered if ingest failed early
                    pass

        with self._state_lock:
            self._snapshot_id = snapshot_tag

    def _invalidate_derived_state_conn(self, conn) -> None:
        """Drop stale derived schema and clear planner-visible versions."""
        conn.execute("DROP SCHEMA IF EXISTS derived CASCADE")
        conn.execute("CREATE SCHEMA IF NOT EXISTS derived")
        with self._state_lock:
            self._derived_versions.clear()

    def insert_live_delivery(self, delivery_data: Dict[str, Any]):
        """
        Insert live delivery data.

        Args:
            delivery_data: Dictionary with delivery information
        """
        with self.pool.get_write_connection() as conn:
            columns = self._table_columns_conn(conn, "ball_events")
            legacy_cols = {"runs_total", "wickets_fallen", "target", "venue", "timestamp"}
            schema_v1_cols = {
                "date", "venue_id", "batter_id", "bowler_id", "non_striker_id",
                "batting_team_id", "bowling_team_id", "runs_batter", "runs_extras",
                "is_wicket", "wicket_type", "phase",
            }

            if legacy_cols.issubset(columns):
                conn.execute(
                    """
                    INSERT INTO ball_events (
                        match_id, inning, over, ball, runs_total,
                        wickets_fallen, target, venue, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        delivery_data["match_id"],
                        delivery_data["inning"],
                        delivery_data["over"],
                        delivery_data["ball"],
                        delivery_data["runs_total"],
                        delivery_data["wickets_fallen"],
                        delivery_data.get("target"),
                        delivery_data.get("venue"),
                        delivery_data.get("timestamp", time.time()),
                    ],
                )
                return

            if schema_v1_cols.issubset(columns):
                conn.execute(
                    """
                    INSERT INTO ball_events (
                        match_id, date, venue_id, inning, over, ball,
                        batter_id, bowler_id, non_striker_id,
                        batting_team_id, bowling_team_id,
                        runs_batter, runs_extras, is_wicket, wicket_type, phase
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        delivery_data["match_id"],
                        delivery_data.get("date", date.today()),
                        delivery_data.get("venue_id"),
                        delivery_data["inning"],
                        delivery_data["over"],
                        delivery_data["ball"],
                        delivery_data.get("batter_id"),
                        delivery_data.get("bowler_id"),
                        delivery_data.get("non_striker_id"),
                        delivery_data.get("batting_team_id"),
                        delivery_data.get("bowling_team_id"),
                        delivery_data.get("runs_batter", 0),
                        delivery_data.get("runs_extras", 0),
                        bool(delivery_data.get("is_wicket", False)),
                        delivery_data.get("wicket_type"),
                        delivery_data.get("phase", self._infer_phase(delivery_data.get("over"))),
                    ],
                )
                return

            raise DataIngestionError(
                "Unsupported ball_events schema for live delivery insert in thread-safe engine"
            )

    @staticmethod
    def _infer_phase(over_value: Any) -> str:
        """Infer innings phase from over index when explicit phase is unavailable."""
        try:
            over = int(over_value)
        except (TypeError, ValueError):
            return "middle"
        if over <= 5:
            return "powerplay"
        if over <= 14:
            return "middle"
        return "death"

    @staticmethod
    def _table_columns_conn(conn, table_name: str) -> set[str]:
        """Return lowercase column names for a table."""
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table_name],
        ).fetchall()
        return {str(row[0]).lower() for row in rows}

    def execute_sql(self, sql: str, params: Optional[list] = None,
                   read_only: bool = True, timeout: float = 30.0) -> pa.Table:
        """
        Execute SQL with connection pooling.

        Args:
            sql: SQL query string
            params: Query parameters
            read_only: Whether this is a read-only query
            timeout: Query timeout in seconds
        """
        if params is None:
            params = []

        timeout_enabled = timeout is not None and timeout > 0
        conn_timeout = timeout if timeout_enabled else 5.0

        connection_ctx = (
            self.pool.get_read_connection(timeout=conn_timeout)
            if read_only
            else self.pool.get_write_connection(timeout=conn_timeout)
        )

        with connection_ctx as conn:
            timed_out = False
            timer: Optional[threading.Timer] = None

            def _interrupt_query() -> None:
                nonlocal timed_out
                timed_out = True
                interrupt = getattr(conn, "interrupt", None)
                if callable(interrupt):
                    try:
                        interrupt()
                    except Exception:
                        pass

            if timeout_enabled:
                timer = threading.Timer(timeout, _interrupt_query)
                timer.daemon = True
                timer.start()

            try:
                result = conn.execute(sql, params).arrow()
                if timed_out:
                    raise QueryTimeoutError(f"Query timed out after {timeout}s: {sql}")

                # Ensure we return a Table
                if isinstance(result, pa.RecordBatchReader):
                    return result.read_all()
                return result
            except Exception as e:
                if timed_out:
                    raise QueryTimeoutError(f"Query timed out after {timeout}s: {sql}") from e
                raise
            finally:
                if timer is not None:
                    timer.cancel()

    def run(self, plan: Dict[str, Any]) -> pa.Table:
        """Execute a query plan."""
        if "sql" in plan:
            return self.execute_sql(
                plan["sql"],
                params=plan.get("params"),
                read_only=plan.get("read_only", True),
                timeout=plan.get("timeout", 30.0),
            )
        raise NotImplementedError("Plan execution without SQL not implemented")

    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """Check if a table exists."""
        with self.pool.get_read_connection() as conn:
            return self._table_exists_conn(conn, table_name, schema=schema)

    def _table_exists_conn(self, conn, table_name: str, schema: Optional[str] = None) -> bool:
        """Check table existence using a specific connection."""
        try:
            if schema is None:
                res = conn.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                    [table_name],
                ).fetchone()
            else:
                res = conn.execute(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = ? AND table_name = ?",
                    [schema, table_name],
                ).fetchone()
            return res[0] > 0 if res else False
        except Exception:
            return False

    def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        return self.pool.get_pool_stats()

    def close(self) -> None:
        """Close all connections."""
        self.pool.close()

# Factory function for backward compatibility
def create_thread_safe_engine(db_path: str = ":memory:",
                            pool_config: Dict[str, Any] = None) -> ThreadSafeQueryEngine:
    """Create a thread-safe query engine instance."""
    return ThreadSafeQueryEngine(db_path, pool_config)

__all__ = ['ThreadSafeQueryEngine', 'ConnectionPool', 'create_thread_safe_engine']