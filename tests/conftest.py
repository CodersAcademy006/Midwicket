import os
import pytest

# ── Test environment — must be set before importing any midwicket modules ──────
# MIDWICKET_ENV=testing enables test-safe defaults:
#   - TrustedHostMiddleware accepts "testserver" in addition to localhost
#   - API_KEY_REQUIRED defaults to false so unit tests don't need auth headers
os.environ["MIDWICKET_ENV"] = "testing"
os.environ["MIDWICKET_SECRET_KEY"] = "test-secret-key-for-pytest"
os.environ["MIDWICKET_API_KEY_REQUIRED"] = "false"
# Redirect all DB paths away from ~/.midwicket_data so parallel tests don't
# collide on the shared registry.duckdb file.
os.environ.setdefault("MIDWICKET_DATA_DIR", "/tmp/midwicket_test_data")


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    """Redirect all Midwicket DB paths to a fresh tmp dir for each test.

    Use this fixture in any test that creates a MidwicketSession, IdentityRegistry,
    or QueryEngine with a persistent path, to prevent cross-test lock contention
    on the shared ~/.midwicket_data/registry.duckdb file.
    """
    monkeypatch.setenv("MIDWICKET_DATA_DIR", str(tmp_path))
    # Also clear the session singleton so the next test gets a fresh instance
    try:
        from midwicket.api.session import MidwicketSession
        MidwicketSession._instance = None
    except ImportError:
        pass
    yield tmp_path
    # Cleanup: close singleton if it was created during the test
    try:
        from midwicket.api.session import MidwicketSession
        if MidwicketSession._instance is not None:
            MidwicketSession._instance.close()
            MidwicketSession._instance = None
    except Exception:
        pass
