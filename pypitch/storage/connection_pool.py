"""
Connection pooling for database engines.
"""

import threading
import time
from typing import Any
from contextlib import contextmanager
import duckdb
import logging

logger = logging.getLogger(__name__)

class ConnectionPool:
    """Thread-safe connection pool for database connections.

    DuckDB serializes writes at the file level: while multiple read
    connections are fine, concurrent writers on the same file will race
    for an OS-level lock.  ``write_connection()`` acquires a process-wide
    mutex so that write operations are serialized and no two threads can
    hold a write connection simultaneously.
    """

    def __init__(self, db_path: str, max_connections: int = 10, max_idle_time: int = 300) -> None:
        self.db_path = db_path
        self.max_connections = max_connections
        self.max_idle_time = max_idle_time
        self._connections: list[dict[str, Any]] = []
        self._condition = threading.Condition(threading.Lock())
        # Serializes all write operations so only one thread writes at a time.
        self._write_lock = threading.Lock()
        self._closed = False

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new database connection."""
        con = duckdb.connect(self.db_path)
        # Performance tuning
        con.execute("PRAGMA threads=4;")
        con.execute("PRAGMA memory_limit='2GB';")
        return con

    def _is_connection_valid(self, conn_info: dict) -> bool:
        """Check if a connection is still valid."""
        if time.time() - conn_info['last_used'] > self.max_idle_time:
            return False
        try:
            conn_info['connection'].execute("SELECT 1")
            return True
        except Exception:
            return False

    def _cleanup_invalid_connections(self) -> None:
        """Clean up invalid idle connections."""
        # Only check idle connections to avoid blocking
        valid_connections = []
        for conn_info in self._connections:
            if conn_info['in_use']:
                valid_connections.append(conn_info)
            elif self._is_connection_valid(conn_info):
                valid_connections.append(conn_info)
            else:
                # Close invalid connection
                try:
                    conn_info['connection'].close()
                except Exception:
                    pass
        self._connections = valid_connections

    def get_connection(self, timeout: float | None = None) -> duckdb.DuckDBPyConnection:
        """Get a connection from the pool."""
        with self._condition:
            if self._closed:
                raise RuntimeError("Connection pool is closed")

            start_time = time.time()
            while True:
                # Try to find an available connection
                for conn_info in self._connections:
                    if not conn_info['in_use']:
                        if self._is_connection_valid(conn_info):
                            conn_info['in_use'] = True
                            conn_info['last_used'] = time.time()
                            return conn_info['connection']
                        else:
                            # Remove invalid connection
                            self._connections.remove(conn_info)
                            try:
                                conn_info['connection'].close()
                            except Exception:
                                pass
                            break # Restart loop

                # Create new connection if under limit
                if len(self._connections) < self.max_connections:
                    conn = self._create_connection()
                    conn_info = {
                        'connection': conn,
                        'in_use': True,
                        'last_used': time.time()
                    }
                    self._connections.append(conn_info)
                    return conn

                # Wait for a connection to become available
                if timeout is not None:
                    elapsed = time.time() - start_time
                    if elapsed >= timeout:
                        raise TimeoutError(
                            f"Connection pool exhausted. Timeout: {timeout}s, "
                            f"Max connections: {self.max_connections}, "
                            f"Active connections: {len(self._connections)}"
                        )
                    self._condition.wait(timeout - elapsed)
                else:
                    self._condition.wait()
                
                # Clean up invalid connections after waiting
                self._cleanup_invalid_connections()

    def return_connection(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Return a connection to the pool."""
        with self._condition:
            for conn_info in self._connections:
                if conn_info['connection'] is connection:
                    conn_info['in_use'] = False
                    conn_info['last_used'] = time.time()
                    self._condition.notify()
                    return
            raise ValueError(f"Connection not managed by this pool: {connection}")

    def close(self) -> None:
        """Close all connections in the pool."""
        with self._condition:
            for conn_info in self._connections:
                try:
                    conn_info['connection'].close()
                except Exception as e:
                    logger.warning("Error closing connection: %s", e)
            self._connections.clear()
            self._closed = True

    @contextmanager
    def connection(self):
        """Context manager for getting a read connection."""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.return_connection(conn)

    @contextmanager
    def write_connection(self):
        """Context manager for a write connection.

        Acquires the process-wide write lock before handing out the
        connection so that concurrent write paths are serialized and the
        DuckDB file is never contended by multiple writers at once.
        """
        with self._write_lock:
            conn = self.get_connection()
            try:
                yield conn
            finally:
                self.return_connection(conn)