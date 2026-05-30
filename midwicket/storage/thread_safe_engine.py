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

    def __init__(self, db_path: str = ":memory:", max_connections: Optional[int] = None,
                 read_pool_size: Optional[int] = None, write_pool_size: Optional[int] = None,
                 threads: Optional[int] = None, memory_limit: Optional[str] = None):
        from midwicket.config import (
            DATABASE_THREADS,
            DATABASE_MEMORY_LIMIT,
            DATABASE_MAX_CONNECTIONS,
            DATABASE_READ_POOL_SIZE,
            DATABASE_WRITE_POOL_SIZE,
        )

        self.db_path = db_path
        self._connect_path = db_path
        if db_path == ":memory:":
            # DuckDB's plain :memory: creates isolated databases per connection.
            # Use a unique named in-memory database so all pooled connections
            # share the same state within this engine instance.
            self._connect_path = f":memory:midwicket_pool_{uuid.uuid4().hex}"
        self.max_connections = max_connections or DATABASE_MAX_CONNECTIONS
        self.read_pool_size = read_pool_size or DATABASE_READ_POOL_SIZE
        self.write_pool_size = write_pool_size or DATABASE_WRITE_POOL_SIZE
        # Honor configured resource limits instead of hardcoding them (MW-018).
        self._threads = threads or DATABASE_THREADS
        self._memory_limit = memory_limit or DATABASE_MEMORY_LIMIT

        # Connection pools
        self.read_pool: queue.Queue = queue.Queue(maxsize=self.read_pool_size)
        self.write_pool: queue.Queue = queue.Queue(maxsize=self.write_pool_size)

        # Pool management
        self._lock = threading.RLock()
        self._created_connections = 0

        # Initialize pools
        self._initialize_pools()

    @property
    def connect_path(self) -> str:
        """Return the concrete DuckDB path used by pooled connections."""
        return self._connect_path

    def _initialize_pools(self):
        """Initialize connection pools.

        Ordering matters: the write pool (and therefore the database file and
        its schema) must be created BEFORE any read connection is opened. On a
        file-based database an empty/non-existent file cannot be opened, and a
        read connection that opens before the writer has materialized state may
        not observe it. Create the write pool first, then the read pool.
        """
        # For file-based databases, ensure the database file exists and is a
        # valid DuckDB file before any pooled connection is opened.
        if self.db_path != ":memory:":
            import os
            if not os.path.exists(self.db_path):
                temp_conn = duckdb.connect(self.db_path)
                temp_conn.close()
            else:
                # Validate the existing file; recreate it if it is not a usable
                # DuckDB database.
                try:
                    temp_conn = duckdb.connect(self.db_path, read_only=True)
                    temp_conn.close()
                except Exception:
                    os.remove(self.db_path)
                    temp_conn = duckdb.connect(self.db_path)
                    temp_conn.close()

        # Create write connections FIRST so the database/schema is initialized
        # by a writer before any reader is opened.
        for _ in range(self.write_pool_size):
            conn = self._create_connection(read_only=False)
            self.write_pool.put(conn)

        # Create read connections.
        for _ in range(self.read_pool_size):
            conn = self._create_connection(read_only=True)
            self.read_pool.put(conn)

    def _create_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """Create a new DuckDB connection with appropriate settings.

        Read-only enforcement note (DuckDB 1.5.x, single-process):
        DuckDB refuses to open a second connection to a database with a
        *different* ``read_only`` configuration than connections already open
        ("Can't open a connection to same database file with a different
        configuration than existing connections"). Because the write pool must
        keep read-write connections open, we therefore CANNOT open the read
        pool with connection-level ``read_only=True`` for either file-based or
        named in-memory databases — they share the same database and the
        configurations would conflict.

        Instead, read-pool connections are flagged here and the pool's
        ``get_read_connection`` context manager wraps every use in a
        ``BEGIN TRANSACTION READ ONLY`` block. That gives genuine
        database-level write rejection (DuckDB raises a TransactionException on
        any INSERT/UPDATE/DELETE/DDL), works identically for file and in-memory
        databases, and does not conflict with the writer pool — true
        defense-in-depth beyond the SQL guard.
        """
        conn = duckdb.connect(self._connect_path, read_only=False)

        # Performance tuning — honor configured limits instead of hardcoding
        # values that silently ignored MIDWICKET_DB_THREADS / MIDWICKET_DB_MEMORY (MW-018).
        threads = int(self._threads)
        if not (1 <= threads <= 16):
            raise ValueError(f"Invalid thread count {threads}; must be 1-16")
        conn.execute(f"PRAGMA threads={threads};")  # nosec B608 — integer validated above
        conn.execute(f"PRAGMA memory_limit='{self._memory_limit}';")

        # Tag the connection so the read pool can enforce read-only semantics
        # at the DuckDB transaction level on every checkout.
        try:
            conn._midwicket_read_only = read_only  # type: ignore[attr-defined]
        except Exception:  # nosec B110 — best-effort tag; enforcement falls back gracefully
            pass

        with self._lock:
            self._created_connections += 1

        return conn

    @staticmethod
    def _end_read_only_transaction(conn) -> None:
        """Best-effort close of a read-only transaction on a read connection.

        Uses ROLLBACK (reads make no changes, so this is equivalent to COMMIT
        but never fails on an aborted transaction). A no-op when no transaction
        is active.
        """
        try:
            conn.execute("ROLLBACK")
        except Exception:  # nosec B110 — no active transaction is fine
            pass

    @contextmanager
    def get_read_connection(self, timeout: float = 5.0):
        """Get a read connection from the pool.

        The connection is wrapped in a ``BEGIN TRANSACTION READ ONLY`` block so
        any write/DDL attempt is rejected by DuckDB itself (defense-in-depth;
        see ``_create_connection`` for why connection-level read_only is not
        usable here). The read-only transaction is always closed before the
        connection is returned to the pool.
        """
        conn = None
        read_only_txn = False
        try:
            conn = self.read_pool.get(timeout=timeout)
            try:
                conn.execute("BEGIN TRANSACTION READ ONLY")
                read_only_txn = True
            except Exception:
                # If a read-only transaction cannot be started, fall back to
                # handing out the connection without it rather than failing the
                # read. The SQL guard remains in place at the API layer.
                read_only_txn = False
            yield conn
        except queue.Empty:
            raise ConnectionError("No read connections available (pool exhausted)")
        finally:
            if conn:
                if read_only_txn:
                    self._end_read_only_transaction(conn)
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

class ThreadSafeQueryEngine(QueryEngine):
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
            
            # Schema Migration for v1 additions (e.g. extras_type)
            cols = self._table_columns_conn(conn, "ball_events")
            if "extras_type" not in cols:
                try:
                    conn.execute("ALTER TABLE ball_events ADD COLUMN extras_type VARCHAR")
                except Exception:
                    pass
            if "player_dismissed" not in cols:
                try:
                    conn.execute("ALTER TABLE ball_events ADD COLUMN player_dismissed VARCHAR")
                except Exception:
                    pass

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
                required_legacy = [
                    "match_id",
                    "inning",
                    "over",
                    "ball",
                    "runs_total",
                    "wickets_fallen",
                ]
                missing = [
                    field
                    for field in required_legacy
                    if delivery_data.get(field) is None
                ]
                if missing:
                    raise DataIngestionError(
                        "Missing required live delivery fields for legacy schema: "
                        + ", ".join(missing)
                    )

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
                # Resolve names to IDs via IdentityRegistry if missing
                if any(delivery_data.get(f) is None for f in ["venue_id", "batter_id", "bowler_id", "non_striker_id", "batting_team_id", "bowling_team_id"]):
                    import contextlib
                    from midwicket.storage.registry import IdentityRegistry
                    from pathlib import Path
                    
                    if self.db_path == ":memory:":
                        reg_path = ":memory:"
                    else:
                        reg_path = str(Path(self.db_path).parent / "registry.duckdb")
                    
                    with contextlib.closing(IdentityRegistry(reg_path)) as registry:
                        match_date = delivery_data.get("date")
                        if not match_date:
                            match_date = date.today()
                        elif isinstance(match_date, str):
                            match_date = date.fromisoformat(match_date)
                        
                        if delivery_data.get("batter_id") is None and delivery_data.get("batter") is not None:
                            delivery_data["batter_id"] = registry.resolve_player(delivery_data["batter"], match_date, auto_ingest=True)
                        if delivery_data.get("bowler_id") is None and delivery_data.get("bowler") is not None:
                            delivery_data["bowler_id"] = registry.resolve_player(delivery_data["bowler"], match_date, auto_ingest=True)
                        if delivery_data.get("non_striker_id") is None and delivery_data.get("non_striker") is not None:
                            delivery_data["non_striker_id"] = registry.resolve_player(delivery_data["non_striker"], match_date, auto_ingest=True)
                        if delivery_data.get("venue_id") is None and delivery_data.get("venue") is not None:
                            delivery_data["venue_id"] = registry.resolve_venue(delivery_data["venue"], match_date, auto_ingest=True)
                        if delivery_data.get("batting_team_id") is None and delivery_data.get("batting_team") is not None:
                            delivery_data["batting_team_id"] = registry.resolve_team(delivery_data["batting_team"], match_date, auto_ingest=True)
                        if delivery_data.get("bowling_team_id") is None and delivery_data.get("bowling_team") is not None:
                            delivery_data["bowling_team_id"] = registry.resolve_team(delivery_data["bowling_team"], match_date, auto_ingest=True)

                required_schema_v1 = [
                    "match_id",
                    "inning",
                    "over",
                    "ball",
                    "venue_id",
                    "batter_id",
                    "bowler_id",
                    "non_striker_id",
                    "batting_team_id",
                    "bowling_team_id",
                ]
                missing = [
                    field
                    for field in required_schema_v1
                    if delivery_data.get(field) is None
                ]
                if missing:
                    raise DataIngestionError(
                        "Missing required live delivery fields for schema v1 table: "
                        + ", ".join(missing)
                    )

                insert_cols = [
                    "match_id", "date", "venue_id", "inning", "over", "ball",
                    "batter_id", "bowler_id", "non_striker_id",
                    "batting_team_id", "bowling_team_id",
                    "runs_batter", "runs_extras", "extras_type", "is_wicket", "wicket_type", "phase"
                ]
                
                match_date = delivery_data.get("date")
                if not match_date:
                    match_date = date.today()
                elif isinstance(match_date, str):
                    match_date = date.fromisoformat(match_date)

                values = [
                    delivery_data["match_id"],
                    match_date,
                    delivery_data["venue_id"],
                    delivery_data["inning"],
                    delivery_data["over"],
                    delivery_data["ball"],
                    delivery_data["batter_id"],
                    delivery_data["bowler_id"],
                    delivery_data["non_striker_id"],
                    delivery_data["batting_team_id"],
                    delivery_data["bowling_team_id"],
                    delivery_data.get("runs_batter", 0),
                    delivery_data.get("runs_extras", 0),
                    delivery_data.get("extras_type"),
                    bool(delivery_data.get("is_wicket", False)),
                    delivery_data.get("wicket_type"),
                    delivery_data.get("phase", self._infer_phase(delivery_data.get("over"))),
                ]
                
                if "batter" in columns:
                    insert_cols.append("batter")
                    values.append(delivery_data.get("batter", ""))
                if "bowler" in columns:
                    insert_cols.append("bowler")
                    values.append(delivery_data.get("bowler", ""))
                if "venue" in columns:
                    insert_cols.append("venue")
                    values.append(delivery_data.get("venue", "Unknown Venue"))
                if "player_dismissed" in columns:
                    insert_cols.append("player_dismissed")
                    values.append(delivery_data.get("player_dismissed"))

                placeholders = ", ".join("?" for _ in values)
                conn.execute(
                    f"INSERT INTO ball_events ({', '.join(insert_cols)}) VALUES ({placeholders})",
                    values,
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
            return "Middle"
        if over <= 5:
            return "Powerplay"
        if over <= 14:
            return "Middle"
        return "Death"

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
            started_at = time.perf_counter()

            def _has_timed_out() -> bool:
                if not timeout_enabled:
                    return False
                if timed_out:
                    return True
                return (time.perf_counter() - started_at) >= float(timeout)

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

                # Ensure we return a Table
                if isinstance(result, pa.RecordBatchReader):
                    result = result.read_all()

                if _has_timed_out():
                    raise QueryTimeoutError(f"Query timed out after {timeout}s: {sql}")
                return result
            except Exception as e:
                if _has_timed_out():
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