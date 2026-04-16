"""
Rate limiting utilities for PyPitch API.
"""

from __future__ import annotations

import hashlib
import ipaddress
import logging
import os
import time
from collections import defaultdict
import threading
import bisect
from pathlib import Path
from fastapi import HTTPException, Request
from pypitch.config import DEFAULT_DATA_DIR

logger = logging.getLogger(__name__)

class RateLimiter:
    """Simple in-memory rate limiter using sliding window."""

    def __init__(self, requests_per_minute: int = 60):
        self.requests_per_minute = requests_per_minute
        self.requests: dict[str, list] = defaultdict(list)
        self.lock = threading.Lock()
        self.cleanup_counter = 0

    def _cleanup_old_requests(self, key: str, window_start: float) -> None:
        """Remove requests older than window_start for the given key."""
        timestamps = self.requests.get(key)
        if timestamps is None:
            return
        # Find first valid timestamp
        start_index = bisect.bisect_right(timestamps, window_start)
            
        if start_index > 0:
            self.requests[key] = timestamps[start_index:]

    def _cleanup_old_keys(self, window_start: float) -> None:
        """Remove keys with no recent requests."""
        keys_to_remove = []
        for key, timestamps in self.requests.items():
            if not timestamps or timestamps[-1] <= window_start:
                keys_to_remove.append(key)
        for key in keys_to_remove:
            del self.requests[key]

    def is_allowed(self, key: str) -> bool:
        """Check if request is allowed for the given key."""
        now = time.time()
        window_start = now - 60  # 1 minute window

        with self.lock:
            # Amortized cleanup: only run full cleanup every 100 requests
            self.cleanup_counter += 1
            if self.cleanup_counter >= 100:
                self._cleanup_old_keys(window_start)
                self.cleanup_counter = 0
            
            # Remove old requests outside the window
            self._cleanup_old_requests(key, window_start)

            # Check if under limit
            if len(self.requests[key]) < self.requests_per_minute:
                self.requests[key].append(now)
                return True
            return False

    def get_remaining_requests(self, key: str) -> int:
        """Get remaining requests for the key in current window."""
        now = time.time()
        window_start = now - 60

        with self.lock:
            if key not in self.requests:
                return self.requests_per_minute
            # Clean up old requests
            self._cleanup_old_requests(key, window_start)
            timestamps = self.requests.get(key, [])
            return max(0, self.requests_per_minute - len(timestamps))

    def get_reset_time(self, key: str) -> float:
        """Get time until rate limit resets."""
        now = time.time()
        window_start = now - 60
        with self.lock:
            if key not in self.requests:
                return 0
            # Expire stale entries first so the oldest remaining timestamp is
            # actually inside the current window.  Without this, a completed
            # burst from >60 s ago would report a reset time far in the past
            # while requests in the new window counted against the wrong limit.
            self._cleanup_old_requests(key, window_start)
            timestamps = self.requests.get(key, [])
            if not timestamps:
                return 0
            oldest_request = timestamps[0]  # list is sorted; first = oldest
            return max(0, (oldest_request + 60) - now)

    def cleanup_old_keys(self) -> None:
        """Remove keys with no recent requests to prevent memory growth."""
        now = time.time()
        window_start = now - 60
        with self.lock:
            self._cleanup_old_keys(window_start)


class DuckDBRateLimiter:
    """DuckDB-backed sliding-window limiter shared across worker processes."""

    def __init__(self, requests_per_minute: int = 60, db_path: str | None = None):
        import duckdb

        self.requests_per_minute = requests_per_minute
        target_path = Path(db_path) if db_path else (DEFAULT_DATA_DIR / "rate_limit.duckdb")
        target_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = str(target_path)
        self.con = duckdb.connect(self.db_path)
        self.lock = threading.Lock()

        with self.lock:
            self.con.execute(
                """
                CREATE TABLE IF NOT EXISTS rate_limit_events (
                    client_key VARCHAR,
                    ts DOUBLE
                )
                """
            )
            self.con.execute(
                "CREATE INDEX IF NOT EXISTS idx_rate_limit_key_ts ON rate_limit_events(client_key, ts)"
            )

    def _cleanup_old_requests(self, key: str, window_start: float) -> None:
        self.con.execute(
            "DELETE FROM rate_limit_events WHERE client_key = ? AND ts <= ?",
            [key, window_start],
        )

    def _cleanup_old_keys(self, window_start: float) -> None:
        self.con.execute("DELETE FROM rate_limit_events WHERE ts <= ?", [window_start])

    def is_allowed(self, key: str) -> bool:
        now = time.time()
        window_start = now - 60

        with self.lock:
            self._cleanup_old_requests(key, window_start)
            count_row = self.con.execute(
                "SELECT COUNT(*) FROM rate_limit_events WHERE client_key = ?",
                [key],
            ).fetchone()
            current_count = count_row[0] if count_row else 0

            if current_count < self.requests_per_minute:
                self.con.execute(
                    "INSERT INTO rate_limit_events(client_key, ts) VALUES (?, ?)",
                    [key, now],
                )
                return True
            return False

    def get_remaining_requests(self, key: str) -> int:
        now = time.time()
        window_start = now - 60

        with self.lock:
            self._cleanup_old_requests(key, window_start)
            count_row = self.con.execute(
                "SELECT COUNT(*) FROM rate_limit_events WHERE client_key = ?",
                [key],
            ).fetchone()
            current_count = count_row[0] if count_row else 0
            return max(0, self.requests_per_minute - current_count)

    def get_reset_time(self, key: str) -> float:
        now = time.time()
        window_start = now - 60

        with self.lock:
            self._cleanup_old_requests(key, window_start)
            row = self.con.execute(
                "SELECT MIN(ts) FROM rate_limit_events WHERE client_key = ?",
                [key],
            ).fetchone()
            oldest = row[0] if row else None
            if oldest is None:
                return 0
            return max(0, (oldest + 60) - now)

    def cleanup_old_keys(self) -> None:
        now = time.time()
        window_start = now - 60
        with self.lock:
            self._cleanup_old_keys(window_start)

    def close(self) -> None:
        with self.lock:
            self.con.close()


def _build_rate_limiter() -> RateLimiter | DuckDBRateLimiter:
    requests_per_minute = int(os.getenv("PYPITCH_RATE_LIMIT_REQUESTS_PER_MINUTE", "60"))
    configured_backend = os.getenv("PYPITCH_RATE_LIMIT_BACKEND", "").strip().lower()

    if configured_backend in {"duckdb", "memory"}:
        backend = configured_backend
    else:
        backend = "duckdb" if os.getenv("PYPITCH_ENV", "development").lower() == "production" else "memory"

    if backend == "duckdb":
        try:
            db_path = os.getenv("PYPITCH_RATE_LIMIT_DB_PATH", "").strip() or None
            limiter = DuckDBRateLimiter(requests_per_minute=requests_per_minute, db_path=db_path)
            logger.info("Rate limiter backend: duckdb (%s)", limiter.db_path)
            return limiter
        except Exception as exc:
            logger.warning("Failed to initialize DuckDB rate limiter (%s). Falling back to memory.", exc)

    logger.info("Rate limiter backend: memory")
    return RateLimiter(requests_per_minute=requests_per_minute)

# Global rate limiter instance
rate_limiter = _build_rate_limiter()


def _is_trusted_proxy(host: str | None) -> bool:
    """Return True when *host* matches PYPITCH_TRUSTED_PROXIES."""
    if not host:
        return False

    raw = os.getenv("PYPITCH_TRUSTED_PROXIES", "").strip()
    if not raw:
        return False

    try:
        peer_ip = ipaddress.ip_address(host)
    except ValueError:
        return False

    for item in raw.split(","):
        token = item.strip()
        if not token:
            continue
        try:
            if "/" in token:
                if peer_ip in ipaddress.ip_network(token, strict=False):
                    return True
            elif peer_ip == ipaddress.ip_address(token):
                return True
        except ValueError:
            logger.warning("Ignoring invalid PYPITCH_TRUSTED_PROXIES entry: %r", token)
            continue

    return False

def get_client_key(request: Request) -> str:
    """Get client identifier for rate limiting."""
    # Prefer Bearer token if available
    auth_header = request.headers.get("Authorization", "")
    if auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
        if token:
            digest = hashlib.sha256(token.encode()).hexdigest()[:32]
            return f"api_key:{digest}"

    # Backward compatibility for legacy clients
    api_key = request.headers.get("X-API-Key")
    if api_key:
        digest = hashlib.sha256(api_key.encode()).hexdigest()[:32]
        return f"api_key:{digest}"

    peer_host = request.client.host if request.client and request.client.host else None

    # Trust X-Forwarded-For only when the direct peer is a trusted proxy.
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded and _is_trusted_proxy(peer_host):
        forwarded_ip = forwarded.split(",", 1)[0].strip()
        if forwarded_ip:
            try:
                canonical_ip = str(ipaddress.ip_address(forwarded_ip))
                return f"ip:{canonical_ip}"
            except ValueError:
                logger.warning("Ignoring malformed X-Forwarded-For value: %r", forwarded_ip)

    # Fallback to direct client IP
    if peer_host:
        return f"ip:{peer_host}"

    return "ip:unknown"

async def check_rate_limit(request: Request) -> None:
    """Middleware function to check rate limits."""
    client_key = get_client_key(request)

    if not rate_limiter.is_allowed(client_key):
        remaining = rate_limiter.get_remaining_requests(client_key)
        reset_time = rate_limiter.get_reset_time(client_key)

        raise HTTPException(
            status_code=429,
            detail={
                "error": "Rate limit exceeded",
                "message": f"Too many requests. Limit: {rate_limiter.requests_per_minute} per minute",
                "remaining_requests": remaining,
                "reset_in_seconds": int(reset_time)
            }
        )