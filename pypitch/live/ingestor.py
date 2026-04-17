"""
PyPitch Live Data Ingestion

Real-time data ingestion pipeline for live cricket matches.
Supports webhooks, API polling, and streaming data sources.
"""

import asyncio
import hashlib
import threading
import time
import os
from typing import Dict, Any, Optional, Callable, List, Set
from dataclasses import dataclass, field
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
import queue
from pathlib import Path

from ..storage.engine import QueryEngine
from ..exceptions import DataIngestionError, ConnectionError

logger = logging.getLogger(__name__)

# Retry policy constants
_MAX_RETRY_ATTEMPTS = 3
_RETRY_BACKOFF_BASE = 0.5   # seconds; actual delay = base * 2^attempt
_DEAD_LETTER_MAX = 1_000    # cap dead-letter queue to prevent unbounded growth

@dataclass
class LiveMatch:
    """Represents a live match being tracked."""
    match_id: str
    source: str  # 'webhook', 'api_poll', 'stream'
    last_update: float
    status: str  # 'active', 'completed', 'abandoned'
    metadata: Dict[str, Any]

class StreamIngestor:
    """
    Real-time data ingestion for live cricket matches.

    Supports multiple data sources:
    - Webhook endpoints for push notifications
    - API polling for regular updates
    - Streaming connections for real-time feeds
    """

    def __init__(self, query_engine: QueryEngine, max_workers: int = 4):
        self.query_engine = query_engine
        self.max_workers = max_workers
        self.executor: Optional[ThreadPoolExecutor] = None
        self.webhook_executor: Optional[ThreadPoolExecutor] = None
        self._lifecycle_lock = threading.Lock()
        self._started = False
        self._create_executors()

        # Live match tracking — protected by _matches_lock (C4)
        self.live_matches: Dict[str, LiveMatch] = {}
        self._matches_lock = threading.Lock()

        # Bounded queue — 10 000 deliveries ≈ ~4 full T20 matches worth of
        # pending updates.  Callers that overflow get queue.Full and should
        # back off rather than silently exhausting server memory.
        self.update_queue: queue.Queue = queue.Queue(maxsize=10_000)
        self.stop_event = threading.Event()

        # Durable deduplication — keyed by (match_id, inning, over, ball).
        # Protected by _seen_lock so concurrent ingestion threads never
        # insert the same delivery twice.
        self._seen_delivery_keys: Set[str] = set()
        self._seen_lock = threading.Lock()

        # Dead-letter storage for deliveries that exhausted all retries.
        # A bounded list; oldest entries are dropped when the cap is hit.
        self.dead_letter: List[Dict[str, Any]] = []
        self._dead_letter_lock = threading.Lock()

        # Webhook server
        self.webhook_server = None
        self.webhook_host = os.getenv("PYPITCH_WEBHOOK_HOST", "localhost").strip() or "localhost"
        self.webhook_port = 8080

        # Polling configuration
        self.poll_interval = 30  # seconds
        self.api_endpoints: Dict[str, str] = {}
        self._api_endpoints_lock = threading.Lock()
        self._api_backoff_state: Dict[str, Dict[str, float]] = {}

        # Callbacks
        self.on_match_update: Optional[Callable] = None
        self.on_match_complete: Optional[Callable] = None

    def _create_executors(self) -> None:
        """Create background executors for worker and webhook threads."""
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.webhook_executor = ThreadPoolExecutor(max_workers=1)

    def start(self):
        """Start the ingestion pipeline."""
        with self._lifecycle_lock:
            if self._started:
                logger.info("Live data ingestion pipeline already started")
                return

            logger.info("Starting live data ingestion pipeline...")
            if self.executor is None or self.webhook_executor is None:
                self._create_executors()
            if self.stop_event.is_set():
                self.stop_event.clear()

            # Start background threads
            self.executor.submit(self._process_updates)
            self.executor.submit(self._poll_apis)

            # Start webhook server
            self._start_webhook_server()

            self._started = True

        logger.info("Live data ingestion pipeline started")

    def stop(self):
        """Stop the ingestion pipeline."""
        logger.info("Stopping live data ingestion pipeline...")
        self.stop_event.set()

        with self._lifecycle_lock:
            webhook_server = self.webhook_server
            self.webhook_server = None
            executor = self.executor
            webhook_executor = self.webhook_executor
            self.executor = None
            self.webhook_executor = None
            self._started = False

        if webhook_server:
            webhook_server.shutdown()
            webhook_server.server_close()

        if executor is not None:
            executor.shutdown(wait=True)
        if webhook_executor is not None:
            webhook_executor.shutdown(wait=True)

        # Drain remaining queued deliveries into dead-letter on shutdown.
        while True:
            try:
                match_id, delivery_data = self.update_queue.get_nowait()
            except queue.Empty:
                break
            self._send_to_dead_letter(match_id, delivery_data, "shutdown_in_flight")
            self.update_queue.task_done()

        logger.info("Live data ingestion pipeline stopped")

    def register_match(self, match_id: str, source: str, metadata: Dict[str, Any] = None) -> bool:
        """
        Register a match for live tracking.

        Args:
            match_id: Unique match identifier
            source: Data source type ('webhook', 'api_poll', 'stream')
            metadata: Additional match metadata

        Returns:
            True if registered successfully
        """
        with self._matches_lock:
            if match_id in self.live_matches:
                logger.warning("Match %s already registered", match_id)
                return False

            if metadata is None:
                metadata = {}

            self.live_matches[match_id] = LiveMatch(
                match_id=match_id,
                source=source,
                last_update=time.time(),
                status='active',
                metadata=metadata
            )

        logger.info("Registered live match: %s (source: %s)", match_id, source)
        return True

    def unregister_match(self, match_id: str):
        """Unregister a match from live tracking."""
        removed = False
        with self._matches_lock:
            if match_id in self.live_matches:
                del self.live_matches[match_id]
                removed = True

        if removed:
            # Prevent unbounded memory growth: once a match is no longer tracked,
            # dedup keys for that match are no longer needed.
            self._prune_seen_keys_for_match(match_id)
            logger.info("Unregistered live match: %s", match_id)

    def _prune_seen_keys_for_match(self, match_id: str) -> None:
        """Remove dedup cache entries for a completed/unregistered match."""
        prefix = f"{match_id}:"
        with self._seen_lock:
            self._seen_delivery_keys = {
                key for key in self._seen_delivery_keys if not key.startswith(prefix)
            }

    def update_match_data(self, match_id: str, delivery_data: Dict[str, Any]) -> bool:
        """
        Update match data for a registered match.

        Args:
            match_id: Match identifier
            delivery_data: Delivery/ball data to ingest

        Returns:
            True when the update was accepted and queued, False when the
            match is not currently registered for live tracking.
        """
        with self._matches_lock:
            if match_id not in self.live_matches:
                logger.warning("Match %s not registered for live tracking", match_id)
                return False
            self.live_matches[match_id].last_update = time.time()

        # Add to processing queue outside the lock — non-blocking; raise
        # immediately if full so the API layer can return HTTP 429.
        try:
            self.update_queue.put_nowait((match_id, delivery_data))
        except queue.Full:
            raise DataIngestionError(
                "Live ingestion queue is full. "
                "The server is under load; please retry after a short delay."
            )
        return True

    def add_api_endpoint(self, name: str, url: str, headers: Dict[str, str] = None):
        """
        Add an API endpoint for polling.

        Args:
            name: Endpoint name
            url: API URL
            headers: Optional HTTP headers
        """
        with self._api_endpoints_lock:
            self.api_endpoints[name] = {
                'url': url,
                'headers': headers or {}
            }
        logger.info(f"Added API endpoint: {name} -> {url}")

    def set_webhook_port(self, port: int):
        """Set the webhook server port."""
        if not (1 <= int(port) <= 65535):
            raise ValueError("webhook port must be between 1 and 65535")
        self.webhook_port = port

    def set_webhook_host(self, host: str):
        """Set the webhook server bind host."""
        host = str(host).strip()
        if not host:
            raise ValueError("webhook host must be a non-empty string")
        self.webhook_host = host

    def _start_webhook_server(self):
        """Start the webhook HTTP server."""
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import urllib.parse

        class WebhookHandler(BaseHTTPRequestHandler):
            def __init__(self, ingestor, *args, **kwargs):
                self.ingestor = ingestor
                super().__init__(*args, **kwargs)

            def do_POST(self):
                """Handle webhook POST requests."""
                try:
                    content_length = int(self.headers['Content-Length'])
                    post_data = self.rfile.read(content_length)
                    data = json.loads(post_data.decode('utf-8'))

                    # Extract match_id from URL path or data
                    path_parts = urllib.parse.urlparse(self.path).path.strip('/').split('/')
                    path_match_id = path_parts[-1] if path_parts and path_parts[-1] else None
                    match_id = path_match_id or data.get('match_id')

                    if match_id:
                        match_id_str = str(match_id)
                        safe_id = Path(match_id_str).name
                        if safe_id != match_id_str or "/" in match_id_str or "\\" in match_id_str:
                            self.send_response(400)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({'error': 'invalid match_id'}).encode())
                            return

                        try:
                            accepted = self.ingestor.update_match_data(safe_id, data)
                        except DataIngestionError as exc:
                            message = str(exc)
                            status = 429 if "queue is full" in message.lower() else 400
                            self.send_response(status)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({'error': message}).encode())
                            return

                        if accepted is False:
                            self.send_response(404)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({'error': 'match not registered'}).encode())
                            return

                        self.send_response(200)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({'status': 'accepted'}).encode())
                    else:
                        self.send_response(400)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({'error': 'match_id required'}).encode())

                except Exception as e:
                    logger.error("Webhook error", exc_info=True)
                    self.send_response(500)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'error': 'Internal server error'}).encode())

            def log_message(self, format, *args):
                # Suppress default HTTP server logs
                return

        # Create server with custom handler
        def create_handler(*args, **kwargs):
            return WebhookHandler(self, *args, **kwargs)

        try:
            self.webhook_server = HTTPServer((self.webhook_host, self.webhook_port), create_handler)
            if self.webhook_executor is None:
                raise RuntimeError("Webhook executor is not initialized")
            self.webhook_executor.submit(self.webhook_server.serve_forever)
            logger.info("Webhook server started on %s:%s", self.webhook_host, self.webhook_port)
        except Exception as e:
            logger.error(f"Failed to start webhook server: {e}")

    @staticmethod
    def _delivery_key(match_id: str, delivery_data: Dict[str, Any]) -> str:
        """
        Build a stable deduplication key for a delivery.

        Uses (match_id, inning, over, ball) — the natural primary key for a
        ball in a cricket match.  Falls back to a content hash so that
        deliveries without full coordinates are still deduplicated when the
        identical payload is enqueued twice.
        """
        inning = delivery_data.get("inning")
        over = delivery_data.get("over")
        ball = delivery_data.get("ball")
        if inning is not None and over is not None and ball is not None:
            return f"{match_id}:{inning}:{over}:{ball}"
        # Fallback: hash the full payload
        payload = json.dumps({**delivery_data, "_mid": match_id}, sort_keys=True)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        return f"{match_id}:hash:{digest}"

    def _is_duplicate(self, key: str) -> bool:
        """Thread-safe duplicate check + mark-seen in one atomic operation."""
        with self._seen_lock:
            if key in self._seen_delivery_keys:
                return True
            self._seen_delivery_keys.add(key)
            return False

    def _clear_seen_key(self, key: str) -> None:
        """Remove a dedup key so failed deliveries can be retried later."""
        with self._seen_lock:
            self._seen_delivery_keys.discard(key)

    def _set_api_backoff(self, name: str, now: float) -> float:
        """Increment endpoint backoff and return delay in seconds."""
        prev_attempts = int(self._api_backoff_state.get(name, {}).get('attempts', 0))
        attempts = prev_attempts + 1
        delay = min(300.0, _RETRY_BACKOFF_BASE * (2 ** attempts))
        self._api_backoff_state[name] = {
            'attempts': float(attempts),
            'next_retry': now + delay,
        }
        return delay

    def _send_to_dead_letter(
        self,
        match_id: str,
        delivery_data: Dict[str, Any],
        reason: str,
    ) -> None:
        """Append to dead-letter list; drops the oldest entry when at cap."""
        entry = {
            "match_id": match_id,
            "delivery": delivery_data,
            "reason": reason,
            "failed_at": time.time(),
        }
        with self._dead_letter_lock:
            if len(self.dead_letter) >= _DEAD_LETTER_MAX:
                self.dead_letter.pop(0)  # evict oldest
            self.dead_letter.append(entry)
        logger.error(
            "ingestor: delivery sent to dead-letter — match=%s reason=%s",
            match_id, reason,
        )

    def _process_updates(self):
        """Process match updates from the queue with deduplication and retry."""
        while not self.stop_event.is_set():
            task_acquired = False
            match_id: Optional[str] = None
            delivery_data: Optional[Dict[str, Any]] = None
            key: Optional[str] = None
            try:
                match_id, delivery_data = self.update_queue.get(timeout=1.0)
                task_acquired = True

                key = self._delivery_key(match_id, delivery_data)
                if self._is_duplicate(key):
                    logger.debug(
                        "ingestor: duplicate delivery dropped — match=%s key=%s",
                        match_id, key,
                    )
                    continue

                # Retry loop with exponential back-off
                last_exc: Optional[Exception] = None
                for attempt in range(_MAX_RETRY_ATTEMPTS):
                    try:
                        self._ingest_delivery_data(match_id, delivery_data)
                        last_exc = None
                        break
                    except Exception as exc:
                        last_exc = exc
                        delay = _RETRY_BACKOFF_BASE * (2 ** attempt)
                        logger.warning(
                            "ingestor: attempt %d/%d failed for match=%s (%s); "
                            "retrying in %.1fs",
                            attempt + 1, _MAX_RETRY_ATTEMPTS, match_id, exc, delay,
                        )
                        time.sleep(delay)

                if last_exc is not None:
                    self._send_to_dead_letter(
                        match_id, delivery_data,
                        reason=f"exhausted {_MAX_RETRY_ATTEMPTS} retries: {last_exc}",
                    )
                    # Keep dedup strict on successful ingest only. Failed
                    # deliveries must remain retryable when re-sent later.
                    self._clear_seen_key(key)
                else:
                    # Notify callbacks only on success
                    if self.on_match_update:
                        try:
                            self.on_match_update(match_id, delivery_data)
                        except Exception as cb_exc:
                            logger.error("ingestor: update callback error: %s", cb_exc)

            except queue.Empty:
                continue
            except Exception as exc:
                logger.error("ingestor: unexpected processing error: %s", exc)
                if task_acquired and match_id is not None and delivery_data is not None:
                    self._send_to_dead_letter(
                        match_id,
                        delivery_data,
                        reason=f"processing_error: {exc}",
                    )
                    if key is not None:
                        self._clear_seen_key(key)
            finally:
                if task_acquired:
                    self.update_queue.task_done()

    def _ingest_delivery_data(self, match_id: str, delivery_data: Dict[str, Any]):
        """Ingest a single delivery into the database (raises on failure)."""
        required_fields = ['inning', 'over', 'ball', 'runs_total', 'wickets_fallen']
        missing = [f for f in required_fields if f not in delivery_data]
        if missing:
            raise DataIngestionError(f"Missing required fields: {missing}")

        delivery_data['match_id'] = match_id
        delivery_data['timestamp'] = time.time()

        self.query_engine.insert_live_delivery(delivery_data)
        logger.debug("ingestor: ingested delivery for match %s: %s", match_id, delivery_data)

    def _poll_apis(self):
        """Poll configured API endpoints for updates."""
        while not self.stop_event.is_set():
            try:
                with self._api_endpoints_lock:
                    endpoints_snapshot = list(self.api_endpoints.items())

                for name, config in endpoints_snapshot:
                    backoff = self._api_backoff_state.get(name)
                    now = time.time()
                    if backoff and now < backoff.get('next_retry', 0.0):
                        continue

                    try:
                        response = requests.get(config['url'], headers=config['headers'], timeout=10)
                        response.raise_for_status()

                        data = response.json()

                        # Process API response (format depends on API)
                        if isinstance(data, list):
                            for match_data in data:
                                match_id = match_data.get('match_id')
                                if match_id:
                                    self.update_match_data(match_id, match_data)
                        elif isinstance(data, dict):
                            match_id = data.get('match_id')
                            if match_id:
                                self.update_match_data(match_id, data)

                        # Successful call resets backoff state for this endpoint.
                        if name in self._api_backoff_state:
                            self._api_backoff_state.pop(name, None)

                    except requests.RequestException as e:
                        delay = self._set_api_backoff(name, now)
                        attempts = int(self._api_backoff_state[name]['attempts'])
                        logger.warning(
                            "API poll failed for %s: %s (attempt=%d, next_retry_in=%.1fs)",
                            name,
                            e,
                            attempts,
                            delay,
                        )
                    except DataIngestionError as e:
                        message = str(e).lower()
                        if "queue is full" in message:
                            delay = self._set_api_backoff(name, now)
                            attempts = int(self._api_backoff_state[name]['attempts'])
                            logger.warning(
                                "API poll backoff for %s due to ingest pressure: %s "
                                "(attempt=%d, next_retry_in=%.1fs)",
                                name,
                                e,
                                attempts,
                                delay,
                            )
                        else:
                            logger.error(f"API processing error for {name}: {e}")
                    except Exception as e:
                        logger.error(f"API processing error for {name}: {e}")

                # Wait before next poll (responsive sleep)
                for _ in range(int(self.poll_interval * 10)):
                    if self.stop_event.is_set():
                        break
                    time.sleep(0.1)

            except Exception as e:
                logger.error(f"API polling error: {e}")
                time.sleep(5)  # Brief pause on error

    def get_live_matches(self) -> List[Dict[str, Any]]:
        """Get list of currently tracked live matches."""
        with self._matches_lock:
            return [
                {
                    'match_id': match.match_id,
                    'source': match.source,
                    'last_update': match.last_update,
                    'status': match.status,
                    'metadata': match.metadata
                }
                for match in self.live_matches.values()
            ]

    def get_dead_letter_items(self) -> List[Dict[str, Any]]:
        """Return a snapshot of the dead-letter list (failed deliveries)."""
        with self._dead_letter_lock:
            return list(self.dead_letter)

    def clear_dead_letter(self) -> int:
        """Clear dead-letter list; returns number of entries removed."""
        with self._dead_letter_lock:
            count = len(self.dead_letter)
            self.dead_letter.clear()
        return count

    def get_match_status(self, match_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific match."""
        with self._matches_lock:
            if match_id not in self.live_matches:
                return None
            match = self.live_matches[match_id]
            return {
                'match_id': match.match_id,
                'source': match.source,
                'last_update': match.last_update,
                'status': match.status,
                'metadata': match.metadata,
                'seconds_since_update': time.time() - match.last_update
            }

# Convenience functions
def create_stream_ingestor(query_engine: QueryEngine) -> StreamIngestor:
    """Create and configure a stream ingestor."""
    ingestor = StreamIngestor(query_engine)

    # Set up basic callbacks
    def on_update(match_id, data):
        logger.info(f"Match {match_id} updated: {data.get('runs_total', 'N/A')} runs")

    def on_complete(match_id):
        logger.info(f"Match {match_id} completed")

    ingestor.on_match_update = on_update
    ingestor.on_match_complete = on_complete

    return ingestor

__all__ = ['StreamIngestor', 'LiveMatch', 'create_stream_ingestor']