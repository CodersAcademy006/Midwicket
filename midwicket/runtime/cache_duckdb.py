import duckdb
import json
import pyarrow as pa
import time
from typing import Any, Optional, Tuple
import threading
from contextlib import nullcontext
from midwicket.runtime.cache import CacheInterface

try:
    from midwicket.config import CACHE_TTL as _DEFAULT_TTL
except Exception:
    _DEFAULT_TTL = 3600

class DuckDBCache(CacheInterface):
    def __init__(self, path: str = ".midwicket_cache.db"):
        self.path = path
        self._in_memory = path == ":memory:"
        # MW-016: use a single shared lock for ALL modes (file and in-memory)
        # to prevent concurrent writes from corrupting the DuckDB file.
        self._lock = threading.RLock()
        self._init_db()
        # Background eviction: purge expired rows every 5 minutes for file mode
        if not self._in_memory:
            self._start_eviction_thread()

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
        Safe serialization — no pickle:
        - Arrow Tables  → IPC Stream (zero-copy compatible)
        - Python objects → JSON (dict, list, str, int, float, bool, None)

        Raises ``TypeError`` for values that cannot be safely serialized
        instead of silently falling back to pickle.
        """
        if isinstance(value, pa.Table):
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, value.schema) as writer:
                writer.write_table(value)
            return sink.getvalue().to_pybytes(), True
        else:
            try:
                return json.dumps(value).encode("utf-8"), False
            except (TypeError, ValueError) as exc:
                raise TypeError(
                    f"Cache value of type {type(value).__name__!r} is not "
                    f"JSON-serializable and pickle is disabled for security. "
                    f"Convert the value to a dict/list/Arrow Table before caching. "
                    f"Original error: {exc}"
                ) from exc

    def _deserialize(self, blob: bytes, is_arrow: bool) -> Any:
        if is_arrow:
            # Zero-copy read from memory buffer
            reader = pa.ipc.open_stream(blob)
            return reader.read_all()
        else:
            return json.loads(blob.decode("utf-8") if isinstance(blob, (bytes, bytearray)) else blob)

    def _start_eviction_thread(self) -> None:
        """Start a daemon thread that periodically removes expired rows."""
        def _evict():
            while True:
                time.sleep(300)  # every 5 minutes
                try:
                    with self._lock:
                        with duckdb.connect(self.path) as con:
                            con.execute(
                                "DELETE FROM cache_store WHERE expires_at <= ?",
                                [int(time.time())],
                            )
                except Exception:
                    pass  # eviction is best-effort
        t = threading.Thread(target=_evict, daemon=True, name="DuckDBCache-evict")
        t.start()

    def _get_con(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        if self.path == ":memory:":
            return self.con
        return duckdb.connect(self.path, read_only=read_only)

    def _operation_guard(self):
        # MW-016: always use the lock (file-based mode is now also serialized)
        return self._lock

    def get(self, key: str) -> Optional[Any]:
        current_time = int(time.time())

        with self._operation_guard():
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

    def set(self, key: str, value: Any, ttl: int = _DEFAULT_TTL) -> None:
        blob, is_arrow = self._serialize(value)
        expires_at = int(time.time()) + ttl

        with self._operation_guard():
            # ACID Transaction for Write
            con = self._get_con()
            try:
                con.execute("""
                    INSERT OR REPLACE INTO cache_store (key, value, is_arrow, expires_at)
                    VALUES (?, ?, ?, ?)
                """, [key, blob, is_arrow, expires_at])
                # Inline opportunistic eviction: remove a batch of expired rows
                con.execute(
                    "DELETE FROM cache_store WHERE expires_at <= ?",
                    [int(time.time())],
                )
            finally:
                if self.path != ":memory:":
                    con.close()

    def clear(self) -> None:
        with self._operation_guard():
            con = self._get_con()
            try:
                con.execute("DELETE FROM cache_store")
                if self.path != ":memory:":
                    con.execute("CHECKPOINT")  # Reclaim disk space
            finally:
                if self.path != ":memory:":
                    con.close()

    def close(self) -> None:
        """Close persistent connections (in-memory caches hold one)."""
        if self.path == ":memory:" and hasattr(self, "con"):
            with self._operation_guard():
                try:
                    self.con.close()
                except Exception:  # nosec B110 — best-effort cleanup; connection may already be closed
                    pass

