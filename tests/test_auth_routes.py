"""
L2 — Integration tests for authentication bypass scenarios.

Verifies that protected routes return 401 when auth is required and no
valid key is presented, and that the internal health probe always passes.
"""

import pytest
from fastapi.testclient import TestClient

from pypitch.serve.api import create_app
from pypitch.storage.engine import QueryEngine
from pypitch.storage.registry import IdentityRegistry
from pypitch.runtime.cache_duckdb import DuckDBCache
from pypitch.runtime.executor import RuntimeExecutor


class _Session:
    def __init__(self) -> None:
        self.registry = IdentityRegistry(":memory:")
        self.engine = QueryEngine(":memory:")
        self.cache = DuckDBCache(":memory:")
        self.executor = RuntimeExecutor(self.cache, self.engine)


@pytest.fixture()
def app_auth(monkeypatch):
    """App with auth enforced (API_KEY_REQUIRED=True, a known valid key)."""
    import pypitch.serve.auth as auth_mod
    monkeypatch.setattr(auth_mod, "API_KEY_REQUIRED", True)
    monkeypatch.setenv("PYPITCH_API_KEYS", "test-secret-key")
    return create_app(session=_Session(), start_ingestor=False)


@pytest.fixture()
def app_no_auth(monkeypatch):
    """App with auth disabled."""
    import pypitch.serve.auth as auth_mod
    monkeypatch.setattr(auth_mod, "API_KEY_REQUIRED", False)
    return create_app(session=_Session(), start_ingestor=False)


# ── Internal health probe ─────────────────────────────────────────────────────

def test_internal_health_no_auth_required(app_auth):
    """/_internal/health must return 200 even when auth is enforced."""
    with TestClient(app_auth, raise_server_exceptions=False) as c:
        assert c.get("/_internal/health").status_code == 200


# ── Root endpoint ─────────────────────────────────────────────────────────────

def test_root_without_key_returns_401(app_auth):
    """GET / without API key → 401 when auth required."""
    with TestClient(app_auth, raise_server_exceptions=False) as c:
        resp = c.get("/")
        assert resp.status_code == 401, f"expected 401, got {resp.status_code}"


def test_root_with_valid_key_returns_200(app_auth):
    """GET / with valid X-API-Key → 200."""
    with TestClient(app_auth, raise_server_exceptions=False) as c:
        resp = c.get("/", headers={"X-API-Key": "test-secret-key"})
        assert resp.status_code == 200


def test_root_with_bearer_returns_200(app_auth):
    """GET / with valid Bearer token → 200."""
    with TestClient(app_auth, raise_server_exceptions=False) as c:
        resp = c.get("/", headers={"Authorization": "Bearer test-secret-key"})
        assert resp.status_code == 200


def test_root_with_wrong_key_returns_401(app_auth):
    """GET / with wrong API key → 401."""
    with TestClient(app_auth, raise_server_exceptions=False) as c:
        resp = c.get("/", headers={"X-API-Key": "wrong-key"})
        assert resp.status_code == 401


# ── Win probability endpoint ──────────────────────────────────────────────────

def test_win_probability_without_key_returns_401(app_auth):
    """GET /win_probability without API key → 401 when auth required."""
    with TestClient(app_auth, raise_server_exceptions=False) as c:
        resp = c.get("/win_probability", params={
            "target": 150, "current_runs": 50,
            "wickets_down": 2, "overs_done": 10.0,
        })
        assert resp.status_code == 401


def test_win_probability_with_key_returns_200(app_auth):
    """GET /win_probability with valid key → 200."""
    with TestClient(app_auth, raise_server_exceptions=False) as c:
        resp = c.get(
            "/win_probability",
            params={"target": 150, "current_runs": 50, "wickets_down": 2, "overs_done": 10.0},
            headers={"X-API-Key": "test-secret-key"},
        )
        assert resp.status_code == 200
        assert "win_prob" in resp.json()


# ── /analyze endpoint ─────────────────────────────────────────────────────────

def test_analyze_without_key_returns_401(app_auth, monkeypatch):
    """/analyze without API key → 401 when auth required (before the 403 gate)."""
    monkeypatch.setenv("PYPITCH_ANALYZE_ENABLED", "true")
    with TestClient(app_auth, raise_server_exceptions=False) as c:
        resp = c.post("/analyze", json={"sql": "SELECT 1"})
        assert resp.status_code == 401


def test_analyze_disabled_returns_403(app_no_auth, monkeypatch):
    """/analyze when PYPITCH_ANALYZE_ENABLED is not set → 403 (feature gate)."""
    monkeypatch.delenv("PYPITCH_ANALYZE_ENABLED", raising=False)
    with TestClient(app_no_auth, raise_server_exceptions=False) as c:
        resp = c.post("/analyze", json={"sql": "SELECT 1"})
        assert resp.status_code == 403
        assert "disabled" in resp.json()["detail"].lower()


def test_analyze_enabled_with_key_returns_200(app_auth, monkeypatch):
    """/analyze with valid key + enabled → 200."""
    monkeypatch.setenv("PYPITCH_ANALYZE_ENABLED", "true")
    with TestClient(app_auth, raise_server_exceptions=False) as c:
        resp = c.post(
            "/analyze",
            json={"sql": "SELECT 1 AS x"},
            headers={"X-API-Key": "test-secret-key"},
        )
        assert resp.status_code == 200
        assert resp.json()["data"][0]["x"] == 1


# ── Legacy /health endpoint ───────────────────────────────────────────────────

def test_v1_health_without_key_returns_401(app_auth):
    """GET /v1/health without API key → 401."""
    with TestClient(app_auth, raise_server_exceptions=False) as c:
        resp = c.get("/v1/health")
        assert resp.status_code == 401


def test_legacy_health_without_key_returns_401(app_auth):
    """GET /health without API key → 401."""
    with TestClient(app_auth, raise_server_exceptions=False) as c:
        resp = c.get("/health")
        assert resp.status_code == 401
