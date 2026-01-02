"""
Test cases for debug mode eager execution.
"""
import pytest
from pypitch.runtime import modes
from pypitch.runtime.executor import RuntimeExecutor
from unittest.mock import Mock

def test_debug_mode_toggle():
    modes.set_debug_mode(True)
    assert modes.debug_mode == True
    modes.set_debug_mode(False)
    assert modes.debug_mode == False

def test_executor_debug_forces_collect():
    # Mock the engine and cache
    mock_engine = Mock()
    mock_cache = Mock()
    mock_relation = Mock()
    mock_relation.collect.return_value = "materialized_data"
    mock_engine.execute_sql.return_value = mock_relation

    executor = RuntimeExecutor(mock_cache, mock_engine)

    # Enable debug mode
    modes.set_debug_mode(True)

    # Mock query - make sure it's not a WinProbQuery
    mock_query = Mock()
    mock_query.cache_key = "test_hash"
    mock_query.snapshot_id = "snap1"
    mock_cache.get.return_value = None  # No cache
    # Ensure it's not a WinProbQuery by setting its type
    mock_query.__class__ = Mock  # Not WinProbQuery

    # Mock planner
    executor.planner = Mock()
    executor.planner.create_legacy_plan.return_value = {"sql": "SELECT * FROM test"}

    result = executor.execute(mock_query)

    # Assert collect was called
    mock_relation.collect.assert_called_once()
    assert result.data == "materialized_data"

    # Reset
    modes.set_debug_mode(False)