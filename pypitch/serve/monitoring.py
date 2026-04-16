"""
Monitoring and metrics collection for PyPitch API.
"""

import time
import threading
from typing import Any, Optional
from collections import defaultdict
import psutil
import logging
import os

logger = logging.getLogger(__name__)

class MetricsCollector:
    """Collects and stores API metrics."""

    def __init__(self, disk_path: Optional[str] = None) -> None:
        self.metrics: dict = defaultdict(list)
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
            # Keep error-only traffic from growing unbounded in memory.
            self._cleanup_old_metrics()

    def get_system_metrics(self) -> dict[str, Any]:
        """Get current system metrics.

        CPU is sampled non-blocking (interval=None) to avoid blocking the event
        loop for ~1 second on every call.  The first call after process start
        always returns 0.0 — subsequent calls return the delta since the last
        sample, which is accurate enough for monitoring dashboards.
        """
        try:
            return {
                # interval=None: returns cached value from the last call — no blocking
                'cpu_percent': psutil.cpu_percent(interval=None),
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
        if time_span <= 0:
            requests_per_minute = 0.0
        else:
            requests_per_minute = total_requests / (time_span / 60)

        # Status code distribution
        status_codes: dict = defaultdict(int)
        for r in requests:
            status_codes[r['status_code']] += 1

        # Endpoint usage
        endpoints: dict = defaultdict(int)
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

# Prime the psutil CPU counter so subsequent non-blocking calls return a useful
# value rather than 0.0.  This single blocking call happens once at import time,
# not on every metrics request.
try:
    psutil.cpu_percent(interval=0.1)
except Exception:  # nosec B110 — psutil may be unavailable in minimal environments; non-fatal
    pass

# Global metrics collector
metrics_collector = MetricsCollector()

def record_request_metrics(method: str, endpoint: str, status_code: int, duration: float):
    """Helper function to record request metrics."""
    metrics_collector.record_request(method, endpoint, status_code, duration)

def record_error_metrics(error_type: str, message: str):
    """Helper function to record error metrics."""
    metrics_collector.record_error(error_type, message)


def generate_prometheus_metrics() -> str:
    """Generate Prometheus text exposition format from the current metrics snapshot."""
    lines: list = []
    api = metrics_collector.get_api_metrics()
    system = metrics_collector.get_system_metrics()

    # ── Request counters by (method, endpoint, status_code) ──────────────────
    lines.append("# HELP pypitch_requests_total Total HTTP requests received")
    lines.append("# TYPE pypitch_requests_total counter")
    with metrics_collector.lock:
        from collections import defaultdict as _dd
        counts: dict = _dd(int)
        for r in metrics_collector.metrics.get("requests", []):
            key = (r.get("method", ""), r.get("endpoint", ""), str(r.get("status_code", "")))
            counts[key] += 1
    for (method, endpoint, status), count in counts.items():
        lines.append(
            f'pypitch_requests_total{{method="{method}",endpoint="{endpoint}",status="{status}"}} {count}'
        )

    # ── Response-time summary ─────────────────────────────────────────────────
    lines.append("# HELP pypitch_request_duration_seconds Average HTTP request duration in seconds")
    lines.append("# TYPE pypitch_request_duration_seconds gauge")
    lines.append(f"pypitch_request_duration_seconds {api.get('avg_response_time', 0):.6f}")

    # ── Error counter ─────────────────────────────────────────────────────────
    lines.append("# HELP pypitch_errors_total Total errors recorded")
    lines.append("# TYPE pypitch_errors_total counter")
    with metrics_collector.lock:
        error_count = len(metrics_collector.metrics.get("errors", []))
    lines.append(f"pypitch_errors_total {error_count}")

    # ── System gauges ─────────────────────────────────────────────────────────
    if system:
        lines.append("# HELP pypitch_cpu_percent CPU usage percentage")
        lines.append("# TYPE pypitch_cpu_percent gauge")
        lines.append(f"pypitch_cpu_percent {system.get('cpu_percent', 0):.2f}")

        lines.append("# HELP pypitch_memory_percent Memory usage percentage")
        lines.append("# TYPE pypitch_memory_percent gauge")
        lines.append(f"pypitch_memory_percent {system.get('memory_percent', 0):.2f}")

        lines.append("# HELP pypitch_disk_usage_percent Disk usage percentage")
        lines.append("# TYPE pypitch_disk_usage_percent gauge")
        lines.append(f"pypitch_disk_usage_percent {system.get('disk_usage_percent', 0):.2f}")

    lines.append("")  # trailing newline required by spec
    return "\n".join(lines)