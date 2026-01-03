"""
Connection pooling for database engines.
"""

import threading
import time
from typing import Optional, Any, List
from contextlib import contextmanager
import duckdb

class ConnectionPool:
    """Thread-safe connection pool for database connections."""

    def __init__(self, db_path: str, max_connections: int = 10, max_idle_time: int = 300):
        self.db_path = db_path
        self.max_connections = max_connections
        self.max_idle_time = max_idle_time
        self._connections: List[dict] = []
        self._lock = threading.Lock()
        self._closed = False

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new database connection."""
        con = duckdb.connect(self.db_path)
        # Performance tuning
        con.execute("PRAGMA threads=2;")
        con.execute("PRAGMA memory_limit='1GB';")
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

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get a connection from the pool."""
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        with self._lock:
            # Clean up invalid connections
            self._connections = [c for c in self._connections if self._is_connection_valid(c)]

            # Try to find an available connection
            for conn_info in self._connections:
                if not conn_info['in_use']:
                    conn_info['in_use'] = True
                    conn_info['last_used'] = time.time()
                    return conn_info['connection']

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
            raise RuntimeError("Connection pool exhausted")

    def return_connection(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Return a connection to the pool."""
        with self._lock:
            for conn_info in self._connections:
                if conn_info['connection'] is connection:
                    conn_info['in_use'] = False
                    conn_info['last_used'] = time.time()
                    break

    def close(self) -> None:
        """Close all connections in the pool."""
        with self._lock:
            for conn_info in self._connections:
                try:
                    conn_info['connection'].close()
                except Exception:
                    pass  # Ignore errors during cleanup
            self._connections.clear()
            self._closed = True

    @contextmanager
    def connection(self):
        """Context manager for getting a connection."""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.return_connection(conn)