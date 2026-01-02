"""
PyPitch Live Data Ingestion

Real-time data ingestion pipeline for live cricket matches.
Supports webhooks, API polling, and streaming data sources.
"""

import asyncio
import threading
import time
from typing import Dict, Any, Optional, Callable, List
from dataclasses import dataclass
import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
import queue

from ..storage.engine import QueryEngine
from ..exceptions import DataIngestionError, ConnectionError

logger = logging.getLogger(__name__)

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
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        # Live match tracking
        self.live_matches: Dict[str, LiveMatch] = {}
        self.update_queue = queue.Queue()
        self.stop_event = threading.Event()

        # Webhook server
        self.webhook_server = None
        self.webhook_port = 8080

        # Polling configuration
        self.poll_interval = 30  # seconds
        self.api_endpoints: Dict[str, str] = {}

        # Callbacks
        self.on_match_update: Optional[Callable] = None
        self.on_match_complete: Optional[Callable] = None

    def start(self):
        """Start the ingestion pipeline."""
        logger.info("Starting live data ingestion pipeline...")

        # Start background threads
        self.executor.submit(self._process_updates)
        self.executor.submit(self._poll_apis)

        # Start webhook server
        self._start_webhook_server()

        logger.info("Live data ingestion pipeline started")

    def stop(self):
        """Stop the ingestion pipeline."""
        logger.info("Stopping live data ingestion pipeline...")
        self.stop_event.set()

        if self.webhook_server:
            self.webhook_server.shutdown()

        self.executor.shutdown(wait=True)
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
        if match_id in self.live_matches:
            logger.warning(f"Match {match_id} already registered")
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

        logger.info(f"Registered live match: {match_id} (source: {source})")
        return True

    def unregister_match(self, match_id: str):
        """Unregister a match from live tracking."""
        if match_id in self.live_matches:
            del self.live_matches[match_id]
            logger.info(f"Unregistered live match: {match_id}")

    def update_match_data(self, match_id: str, delivery_data: Dict[str, Any]):
        """
        Update match data for a registered match.

        Args:
            match_id: Match identifier
            delivery_data: Delivery/ball data to ingest
        """
        if match_id not in self.live_matches:
            logger.warning(f"Match {match_id} not registered for live tracking")
            return

        # Add to processing queue
        self.update_queue.put((match_id, delivery_data))
        self.live_matches[match_id].last_update = time.time()

    def add_api_endpoint(self, name: str, url: str, headers: Dict[str, str] = None):
        """
        Add an API endpoint for polling.

        Args:
            name: Endpoint name
            url: API URL
            headers: Optional HTTP headers
        """
        self.api_endpoints[name] = {
            'url': url,
            'headers': headers or {}
        }
        logger.info(f"Added API endpoint: {name} -> {url}")

    def set_webhook_port(self, port: int):
        """Set the webhook server port."""
        self.webhook_port = port

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
                    match_id = path_parts[-1] if path_parts else data.get('match_id')

                    if match_id:
                        self.ingestor.update_match_data(match_id, data)
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
                    logger.error(f"Webhook error: {e}")
                    self.send_response(500)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'error': str(e)}).encode())

            def log_message(self, format, *args):
                # Suppress default HTTP server logs
                return

        # Create server with custom handler
        def create_handler(*args, **kwargs):
            return WebhookHandler(self, *args, **kwargs)

        try:
            self.webhook_server = HTTPServer(('localhost', self.webhook_port), create_handler)
            self.executor.submit(self.webhook_server.serve_forever)
            logger.info(f"Webhook server started on port {self.webhook_port}")
        except Exception as e:
            logger.error(f"Failed to start webhook server: {e}")

    def _process_updates(self):
        """Process match updates from the queue."""
        while not self.stop_event.is_set():
            try:
                # Get update with timeout
                match_id, delivery_data = self.update_queue.get(timeout=1.0)

                # Process the update
                self._ingest_delivery_data(match_id, delivery_data)

                # Notify callbacks
                if self.on_match_update:
                    try:
                        self.on_match_update(match_id, delivery_data)
                    except Exception as e:
                        logger.error(f"Update callback error: {e}")

                self.update_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Update processing error: {e}")

    def _ingest_delivery_data(self, match_id: str, delivery_data: Dict[str, Any]):
        """Ingest delivery data into the database."""
        try:
            # Validate required fields
            required_fields = ['inning', 'over', 'ball', 'runs_total', 'wickets_fallen']
            missing = [f for f in required_fields if f not in delivery_data]
            if missing:
                raise DataIngestionError(f"Missing required fields: {missing}")

            # Add match_id and timestamp
            delivery_data['match_id'] = match_id
            delivery_data['timestamp'] = time.time()

            # Insert into database
            # This assumes the query engine has a method for live data insertion
            self.query_engine.insert_live_delivery(delivery_data)

            logger.debug(f"Ingested delivery for match {match_id}: {delivery_data}")

        except Exception as e:
            logger.error(f"Failed to ingest delivery data for {match_id}: {e}")
            raise

    def _poll_apis(self):
        """Poll configured API endpoints for updates."""
        while not self.stop_event.is_set():
            try:
                for name, config in self.api_endpoints.items():
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

                    except requests.RequestException as e:
                        logger.warning(f"API poll failed for {name}: {e}")
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

    def get_match_status(self, match_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific match."""
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