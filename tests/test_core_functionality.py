"""
Core pypitch functionality tests — session lifecycle and cleanup.
Moved from repo root to tests/ for correct pytest discovery.
Fixes: Session → PyPitchSession, StorageEngine → QueryEngine.
"""
import pytest
from pypitch.api import session as session_module
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


def test_init_replaces_singleton_and_closes_previous(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
):
    """init() should close any previous singleton before replacing it."""
    first = PyPitchSession(data_dir=str(tmp_path / "first"), skip_registry_build=True)

    with PyPitchSession._instance_lock:
        PyPitchSession._instance = first

    closed = {"count": 0}
    original_close = first.close

    def _tracked_close() -> None:
        closed["count"] += 1
        original_close()

    monkeypatch.setattr(first, "close", _tracked_close)

    second = session_module.init(source=str(tmp_path / "second"))
    try:
        assert PyPitchSession._instance is second
        assert closed["count"] == 1
    finally:
        second.close()


def test_get_player_stats_none_input_returns_none(tmp_path) -> None:
    with PyPitchSession(data_dir=str(tmp_path / "stats-none"), skip_registry_build=True) as session:
        assert session.get_player_stats(None) is None  # type: ignore[arg-type]
