import os
import pytest

# ── Test environment — must be set before importing any pypitch modules ──────
# PYPITCH_ENV=testing enables test-safe defaults:
#   - TrustedHostMiddleware accepts "testserver" in addition to localhost
#   - API_KEY_REQUIRED defaults to false so unit tests don't need auth headers
os.environ["PYPITCH_ENV"] = "testing"
os.environ["PYPITCH_SECRET_KEY"] = "test-secret-key-for-pytest"
os.environ["PYPITCH_API_KEY_REQUIRED"] = "false"
# Redirect all DB paths away from ~/.pypitch_data so parallel tests don't
# collide on the shared registry.duckdb file.
os.environ.setdefault("PYPITCH_DATA_DIR", "/tmp/pypitch_test_data")


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    """Redirect all PyPitch DB paths to a fresh tmp dir for each test.

    Use this fixture in any test that creates a PyPitchSession, IdentityRegistry,
    or QueryEngine with a persistent path, to prevent cross-test lock contention
    on the shared ~/.pypitch_data/registry.duckdb file.
    """
    monkeypatch.setenv("PYPITCH_DATA_DIR", str(tmp_path))
    # Also clear the session singleton so the next test gets a fresh instance
    try:
        from pypitch.api.session import PyPitchSession
        PyPitchSession._instance = None
    except ImportError:
        pass
    yield tmp_path
    # Cleanup: close singleton if it was created during the test
    try:
        from pypitch.api.session import PyPitchSession
        if PyPitchSession._instance is not None:
            PyPitchSession._instance.close()
            PyPitchSession._instance = None
    except Exception:
        pass
