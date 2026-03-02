"""
Core pypitch functionality tests — session lifecycle and cleanup.
Moved from repo root to tests/ for correct pytest discovery.
Fixes: Session → PyPitchSession, StorageEngine → QueryEngine.
"""
import pytest
from pypitch.api.session import PyPitchSession


def test_session_initializes():
    """PyPitchSession can be created and closed without errors."""
    session = PyPitchSession()
    try:
        assert session is not None
    finally:
        session.close()


def test_session_context_manager():
    """PyPitchSession works as a context manager and closes cleanly."""
    with PyPitchSession() as session:
        assert session is not None


def test_session_singleton_cleared_on_close():
    """Closing a session must clear the class-level _instance reference."""
    session = PyPitchSession()
    session.close()
    # After close, the singleton reference should no longer point to this session
    assert PyPitchSession._instance is not session


def test_session_engine_accessible():
    """Session exposes a QueryEngine after initialisation."""
    from pypitch.storage.engine import QueryEngine

    with PyPitchSession() as session:
        assert isinstance(session.engine, QueryEngine)
