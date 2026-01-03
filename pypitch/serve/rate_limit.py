"""
Rate limiting utilities for PyPitch API.
"""

import time
from typing import Dict, Tuple
from collections import defaultdict
import threading
from fastapi import HTTPException, Request

class RateLimiter:
    """Simple in-memory rate limiter using sliding window."""

    def __init__(self, requests_per_minute: int = 60):
        self.requests_per_minute = requests_per_minute
        self.requests: Dict[str, list] = defaultdict(list)
        self.lock = threading.Lock()

    def is_allowed(self, key: str) -> bool:
        """Check if request is allowed for the given key."""
        now = time.time()
        window_start = now - 60  # 1 minute window

        with self.lock:
            # Remove old requests outside the window
            self.requests[key] = [t for t in self.requests[key] if t > window_start]

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
            # Clean up old requests
            self.requests[key] = [t for t in self.requests[key] if t > window_start]
            return max(0, self.requests_per_minute - len(self.requests[key]))

    def get_reset_time(self, key: str) -> float:
        """Get time until rate limit resets."""
        with self.lock:
            if not self.requests[key]:
                return 0
            oldest_request = min(self.requests[key])
            return max(0, (oldest_request + 60) - time.time())

# Global rate limiter instance
rate_limiter = RateLimiter()

def get_client_key(request: Request) -> str:
    """Get client identifier for rate limiting."""
    # Use API key if available, otherwise use IP
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return f"api_key:{api_key}"

    # Fallback to IP address
    client_ip = request.client.host if request.client else "unknown"
    return f"ip:{client_ip}"

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