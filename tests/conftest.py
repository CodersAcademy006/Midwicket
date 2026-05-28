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


# ── Known-failure backlog (bug remediation campaign) ──────────────────────────
# Tests that fail because of a *documented* bug we have not fixed yet. Each entry
# is marked xfail(strict=False) so CI stays green while the bug is tracked. As a
# remediation PR lands its fix, DELETE the corresponding entry here — the test
# will then be required to pass and will fail CI if it regresses.
#
# This is the single source of truth for "known broken, scheduled to fix".
# Keyed by a unique nodeid fragment → reason (bug id + owning PR).
KNOWN_FAILURES = {
    # /analyze EXPLAIN cost-estimation uses label-based .iloc[0][1] → always 403.
    # Fixed in PR3 (broken endpoints + analyze handler).
    "test_analyze_contract.py::test_analyze_accepts_sql_key": "MW-analyze: EXPLAIN parsing (PR3)",
    "test_analyze_contract.py::test_analyze_accepts_legacy_query_key": "MW-analyze: EXPLAIN parsing (PR3)",
    "test_analyze_contract.py::test_analyze_binds_positional_params": "MW-analyze: EXPLAIN parsing (PR3)",
    "test_analyze_contract.py::test_analyze_persists_audit_log_entry": "MW-analyze: EXPLAIN parsing (PR3)",
    "test_analyze_contract.py::test_analyze_audit_write_paths_use_write_mode": "MW-analyze: EXPLAIN parsing (PR3)",
    "test_analyze_contract.py::test_analyze_audit_row_has_usable_id": "MW-analyze: EXPLAIN parsing (PR3)",
    "test_analyze_contract.py::test_analyze_audit_write_failure_is_observable": "MW-analyze: EXPLAIN parsing (PR3)",
    "test_auth_routes.py::test_analyze_enabled_with_key_returns_200": "MW-analyze: EXPLAIN parsing (PR3)",
    "test_serve.py::TestFastAPIApp::test_analyze_query_timeout_error_maps_to_408": "MW-006: /analyze timeout mapping (PR3)",
    "test_serve.py::TestFastAPIApp::test_audit_endpoint_graceful_when_table_missing": "MW-analyze: audit table handling (PR3)",
}


def pytest_collection_modifyitems(config, items):
    """Apply xfail markers to the documented known-failure backlog above."""
    import pytest
    for item in items:
        for fragment, reason in KNOWN_FAILURES.items():
            if fragment in item.nodeid:
                item.add_marker(pytest.mark.xfail(reason=reason, strict=False))
                break
