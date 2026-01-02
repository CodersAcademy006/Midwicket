import pytest
import time

def test_query_speed(benchmark):
    from pypitch.api.session import get_executor
    executor = get_executor()
    # Example: Query a known match (replace with a real match_id in your data)
    match_id = '1082591'
    def run():
        # Simulate a simple query (replace with real query logic)
        return executor.execute({'match_id': match_id})
    result = benchmark(run)
    assert result is not None
