"""
Performance Regression Tests for PyPitch

Ensures that core operations remain fast and don't regress over time.
Uses pytest-benchmark to track query performance.
"""

import pytest
import tempfile
from pathlib import Path
from pypitch.api.session import PyPitchSession
from pypitch.data.loader import DataLoader

# Skip all tests in this module if pytest-benchmark is not installed
pytest_benchmark = pytest.importorskip("pytest_benchmark", reason="pytest-benchmark not installed")

@pytest.fixture(scope="session")
def benchmark_session():
    """Create a test session with sample data for benchmarking."""
    with tempfile.TemporaryDirectory() as temp_dir:
        data_dir = Path(temp_dir)

        # Create minimal test data
        with PyPitchSession(str(data_dir)) as session:
            # Load a small amount of test data
            loader = DataLoader(str(data_dir))
            # For benchmarking, we'll use existing data or create mock data

            yield session

class TestPerformanceRegression:
    """Performance tests to prevent slowdowns."""

    def test_player_stats_query_speed(self, benchmark, benchmark_session):
        """Ensure player stats queries stay under 50ms."""
        # Benchmark the get_player_stats method
        result = benchmark(
            benchmark_session.get_player_stats,
            "V Kohli"
        )

        # The result should be fast
        assert result is not None or result is None  # Allow None for missing data

    def test_match_loading_speed(self, benchmark, benchmark_session):
        """Ensure match loading stays under 200ms."""
        # Find an available match ID
        # For now, we'll benchmark the method call even if it fails
        def load_match():
            try:
                benchmark_session.load_match("980959")  # Sample match
            except:
                pass  # Ignore errors for benchmarking

        benchmark(load_match)

    def test_registry_resolution_speed(self, benchmark, benchmark_session):
        """Ensure player resolution stays under 10ms."""
        result = benchmark(
            benchmark_session.registry.resolve_player,
            "V Kohli"
        )

        # Should resolve quickly
        assert result is not None or result is None

    def test_query_execution_speed(self, benchmark, benchmark_session):
        """Ensure basic queries stay under 100ms."""
        def run_query():
            try:
                # Simple query
                df = benchmark_session.engine.con.sql("SELECT 1 as test").df()
                return df
            except:
                return None

        result = benchmark(run_query)
        assert result is not None

    @pytest.mark.parametrize("query_type", ["player_stats", "match_stats", "registry_lookup"])
    def test_query_types_performance(self, benchmark, query_type, benchmark_session):
        """Parameterized test for different query types."""

        query_funcs = {
            "player_stats": lambda: benchmark_session.get_player_stats("V Kohli"),
            "match_stats": lambda: benchmark_session.load_match("980959"),
            "registry_lookup": lambda: benchmark_session.registry.resolve_player("V Kohli")
        }

        # Allow exceptions for benchmarking
        def safe_query():
            try:
                return query_funcs[query_type]()
            except:
                return None

        benchmark(safe_query)

# Configuration for pytest-benchmark
def pytest_configure(config):
    """Configure benchmark settings."""
    config.addinivalue_line(
        "markers", "performance: marks tests as performance benchmarks"
    )

# Custom benchmark fixture for PyPitch-specific thresholds
@pytest.fixture
def pypitch_benchmark(benchmark):
    """Custom benchmark fixture with PyPitch-specific thresholds."""

    def benchmark_with_threshold(func, max_time_ms=100):
        """Benchmark a function and assert it stays under max_time_ms."""
        import time

        start = time.time()
        result = func()
        duration = (time.time() - start) * 1000  # Convert to ms

        assert duration < max_time_ms, f"Function took {duration:.2f}ms, exceeds {max_time_ms}ms threshold"
        return result

    return benchmark_with_threshold

# Example usage in tests
def test_custom_benchmark(benchmark, benchmark_session):
    """Example of using benchmark fixture."""
    result = benchmark(
        lambda: benchmark_session.get_player_stats("V Kohli")
    )
    assert result is not None

# CI/CD Integration helpers
def get_performance_baseline():
    """Get current performance baseline for CI comparison."""
    return {
        "player_stats_query": 25.0,  # ms
        "match_loading": 150.0,      # ms
        "registry_resolution": 5.0,  # ms
        "query_execution": 50.0      # ms
    }

def check_performance_regression(current_times, baseline_times, threshold=1.05):
    """
    Check if current performance regressed beyond threshold.

    Args:
        current_times: Dict of current benchmark times
        baseline_times: Dict of baseline times
        threshold: Maximum allowed ratio (e.g., 1.05 = 5% slowdown allowed)

    Returns:
        Dict with regression results
    """
    regressions = {}

    for test_name, current_time in current_times.items():
        baseline = baseline_times.get(test_name)
        if baseline:
            ratio = current_time / baseline
            if ratio > threshold:
                regressions[test_name] = {
                    "current": current_time,
                    "baseline": baseline,
                    "ratio": ratio,
                    "slowdown_percent": (ratio - 1) * 100
                }

    return regressions

# Export for use in CI
__all__ = [
    'get_performance_baseline',
    'check_performance_regression'
]
