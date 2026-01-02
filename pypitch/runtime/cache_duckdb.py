import duckdb
import pickle
import pyarrow as pa
import time
from typing import Any, Optional, Tuple
from pypitch.runtime.cache import CacheInterface

class DuckDBCache(CacheInterface):
    def __init__(self, path: str = ".pypitch_cache.db"):
        self.path = path
        self._init_db()

    def _init_db(self) -> None:
        """
        Creates the KV schema if missing.
        Uses a separate connection to avoid threading issues during init.
        """
        # Handle :memory: case specifically
        if self.path == ":memory:":
            self.con = duckdb.connect(":memory:")
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS cache_store (
                    key VARCHAR PRIMARY KEY,
                    value BLOB,
                    is_arrow BOOLEAN,
                    expires_at BIGINT
                );
                CREATE INDEX IF NOT EXISTS idx_cache_expiry ON cache_store(expires_at);
            """)
            return

        with duckdb.connect(self.path) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS cache_store (
                    key VARCHAR PRIMARY KEY,
                    value BLOB,
                    is_arrow BOOLEAN,
                    expires_at BIGINT
                );
                CREATE INDEX IF NOT EXISTS idx_cache_expiry ON cache_store(expires_at);
            """)

    def _serialize(self, value: Any) -> Tuple[bytes, bool]:
        """
        Smart serialization:
        - Arrow Tables -> IPC Stream (Zero-Copy compatible)
        - Python Objects -> Pickle (Fallback)
        """
        if isinstance(value, pa.Table):
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, value.schema) as writer:
                writer.write_table(value)
            return sink.getvalue().to_pybytes(), True
        else:
            return pickle.dumps(value), False

    def _deserialize(self, blob: bytes, is_arrow: bool) -> Any:
        if is_arrow:
            # Zero-copy read from memory buffer
            reader = pa.ipc.open_stream(blob)
            return reader.read_all()
        else:
            return pickle.loads(blob)

    def _get_con(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        if self.path == ":memory:":
            return self.con
        return duckdb.connect(self.path, read_only=read_only)

    def get(self, key: str) -> Optional[Any]:
        current_time = int(time.time())
        
        # Connect strictly for this operation
        con = self._get_con(read_only=True if self.path != ":memory:" else False)
        try:
            # 1. Check existence and expiry in SQL (Pushdown optimization)
            row = con.execute("""
                SELECT value, is_arrow 
                FROM cache_store 
                WHERE key = ? AND expires_at > ?
            """, [key, current_time]).fetchone()

            if row is None:
                return None
            
            blob, is_arrow = row
            return self._deserialize(blob, is_arrow)
        finally:
            if self.path != ":memory:":
                con.close()

    def set(self, key: str, value: Any, ttl: int = 3600) -> None:
        blob, is_arrow = self._serialize(value)
        expires_at = int(time.time()) + ttl

        # ACID Transaction for Write
        con = self._get_con()
        try:
            con.execute("""
                INSERT OR REPLACE INTO cache_store (key, value, is_arrow, expires_at)
                VALUES (?, ?, ?, ?)
            """, [key, blob, is_arrow, expires_at])
        finally:
            if self.path != ":memory:":
                con.close()

    def clear(self) -> None:
        con = self._get_con()
        try:
            con.execute("DELETE FROM cache_store")
            if self.path != ":memory:":
                con.execute("CHECKPOINT")  # Reclaim disk space
        finally:
            if self.path != ":memory:":
                con.close()

    def close(self) -> None:
        """Close any persistent connections. For DuckDBCache, connections are managed per operation."""
        pass  # Connections are opened/closed per operation, no persistent connection to close

