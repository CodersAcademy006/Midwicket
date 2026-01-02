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
from contextlib import contextmanager

from .engine import QueryEngine
from ..exceptions import ConnectionError, QueryTimeoutError

class ConnectionPool:
    """
    Thread-safe connection pool for DuckDB.

    Maintains separate pools for read and write operations.
    """

    def __init__(self, db_path: str = ":memory:", max_connections: int = 10,
                 read_pool_size: int = 5, write_pool_size: int = 2):
        self.db_path = db_path
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
        conn = duckdb.connect(self.db_path, read_only=read_only)

        # Performance tuning
        conn.execute("PRAGMA threads=2;")  # Reduced for connection pooling
        conn.execute("PRAGMA memory_limit='1GB';")

        # Additional settings for read connections
        if read_only:
            pass  # Removed unsupported PRAGMA

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
            return self._derived_versions.copy()

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

            finally:
                try:
                    conn.unregister('arrow_view')
                except Exception:
                    pass

        with self._state_lock:
            self._snapshot_id = snapshot_tag

    def insert_live_delivery(self, delivery_data: Dict[str, Any]):
        """
        Insert live delivery data.

        Args:
            delivery_data: Dictionary with delivery information
        """
        with self.pool.get_write_connection() as conn:
            # Insert the delivery
            conn.execute("""
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
                delivery_data.get('timestamp', time.time())
            ])

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

        start_time = time.time()

        try:
            if read_only:
                with self.pool.get_read_connection(timeout=5.0) as conn:
                    result = conn.execute(sql, params).arrow()
            else:
                with self.pool.get_write_connection(timeout=5.0) as conn:
                    result = conn.execute(sql, params).arrow()

            # Ensure we return a Table
            if isinstance(result, pa.RecordBatchReader):
                return result.read_all()
            return result

        except Exception as e:
            if time.time() - start_time > timeout:
                raise QueryTimeoutError(f"Query timed out after {timeout}s: {sql}")
            raise e

    def run(self, plan: Dict[str, Any]) -> pa.Table:
        """Execute a query plan."""
        if "sql" in plan:
            return self.execute_sql(plan["sql"])
        raise NotImplementedError("Plan execution without SQL not implemented")

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        with self.pool.get_read_connection() as conn:
            return self._table_exists_conn(conn, table_name)

    def _table_exists_conn(self, conn, table_name: str) -> bool:
        """Check table existence using a specific connection."""
        try:
            res = conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name]
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