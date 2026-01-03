"""
Monitoring and metrics collection for PyPitch API.
"""

import time
import threading
from typing import Any, Optional
from collections import defaultdict
import psutil
import logging
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)

class MetricsCollector:
    """Collects and stores API metrics."""

    def __init__(self, disk_path: Optional[str] = None) -> None:
        self.metrics = defaultdict(list)
        self.lock = threading.Lock()
        self.max_metrics_age = 3600  # Keep metrics for 1 hour
        
        # Configure disk monitoring path
        if disk_path:
            if not os.path.isabs(disk_path):
                raise ValueError(f"disk_path must be absolute: {disk_path}")
            if not os.path.exists(disk_path):
                raise ValueError(f"disk_path does not exist: {disk_path}")
            if not os.access(disk_path, os.R_OK):
                raise ValueError(f"disk_path is not accessible: {disk_path}")
            self.disk_path = disk_path
        else:
            # Default to system root
            self.disk_path = os.path.abspath(os.sep)
            # On Windows, try to resolve to system drive if default is used
            if os.name == 'nt':
                system_drive = os.environ.get('SystemDrive')
                if system_drive:
                    self.disk_path = os.path.abspath(system_drive + os.sep)
            
            # Verify default path is valid and readable
            if not os.path.exists(self.disk_path) or not os.access(self.disk_path, os.R_OK):
                # Fallback to current working directory if system root fails
                try:
                    fallback_path = os.path.abspath(os.getcwd())
                    if os.path.exists(fallback_path) and os.access(fallback_path, os.R_OK):
                        logger.warning("Disk path %s not accessible; falling back to %s", self.disk_path, fallback_path)
                        self.disk_path = fallback_path
                    else:
                        logger.error("Neither disk path %s nor fallback %s are accessible. Disk metrics may fail.", 
                                   self.disk_path, fallback_path)
                        # Keep the original path; get_system_metrics will handle errors
                except (FileNotFoundError, OSError):
                    logger.exception("Cannot determine accessible disk path. Disk metrics may be unavailable.")

    def record_request(self, method: str, endpoint: str, status_code: int, duration: float):
        """Record an API request."""
        with self.lock:
            timestamp = time.time()
            self.metrics['requests'].append({
                'timestamp': timestamp,
                'method': method,
                'endpoint': endpoint,
                'status_code': status_code,
                'duration': duration
            })

            # Clean old metrics
            self._cleanup_old_metrics()

    def record_error(self, error_type: str, message: str):
        """Record an error."""
        with self.lock:
            timestamp = time.time()
            self.metrics['errors'].append({
                'timestamp': timestamp,
                'type': error_type,
                'message': message
            })

    def get_system_metrics(self) -> dict[str, Any]:
        """Get current system metrics."""
        try:
            return {
                'cpu_percent': psutil.cpu_percent(interval=1),
                'memory_percent': psutil.virtual_memory().percent,
                'memory_used_mb': psutil.virtual_memory().used / 1024 / 1024,
                'memory_available_mb': psutil.virtual_memory().available / 1024 / 1024,
                'disk_usage_percent': psutil.disk_usage(self.disk_path).percent,
                'timestamp': time.time()
            }
        except Exception as e:
            logger.exception("Failed to collect system metrics: %s", e)
            return {}

    def get_api_metrics(self, since: Optional[float] = None) -> dict[str, Any]:
        """Get API usage metrics."""
        if since is None:
            since = time.time() - 3600  # Last hour

        with self.lock:
            requests = [r for r in self.metrics['requests'] if r['timestamp'] >= since]
            errors = [e for e in self.metrics['errors'] if e['timestamp'] >= since]

        if not requests:
            return {
                'total_requests': 0,
                'avg_response_time': 0,
                'error_rate': 0,
                'requests_per_minute': 0,
                'status_codes': {},
                'endpoints': {}
            }

        total_requests = len(requests)
        avg_response_time = sum(r['duration'] for r in requests) / total_requests
        error_rate = len(errors) / total_requests if total_requests > 0 else 0

        # Calculate requests per minute
        time_span = time.time() - since
        requests_per_minute = total_requests / (time_span / 60)

        # Status code distribution
        status_codes = defaultdict(int)
        for r in requests:
            status_codes[r['status_code']] += 1

        # Endpoint usage
        endpoints = defaultdict(int)
        for r in requests:
            endpoints[r['endpoint']] += 1

        return {
            'total_requests': total_requests,
            'avg_response_time': round(avg_response_time, 3),
            'error_rate': round(error_rate, 3),
            'requests_per_minute': round(requests_per_minute, 2),
            'status_codes': dict(status_codes),
            'endpoints': dict(endpoints)
        }

    def _cleanup_old_metrics(self) -> None:
        """Remove metrics older than max_metrics_age."""
        cutoff = time.time() - self.max_metrics_age
        for metric_list in self.metrics.values():
            metric_list[:] = [m for m in metric_list if m['timestamp'] > cutoff]

# Global metrics collector
metrics_collector = MetricsCollector()

def record_request_metrics(method: str, endpoint: str, status_code: int, duration: float):
    """Helper function to record request metrics."""
    metrics_collector.record_request(method, endpoint, status_code, duration)

def record_error_metrics(error_type: str, message: str):
    """Helper function to record error metrics."""
    metrics_collector.record_error(error_type, message)