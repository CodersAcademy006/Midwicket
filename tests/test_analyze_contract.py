"""Tests for /analyze payload compatibility and parameter handling."""

from fastapi.testclient import TestClient

from pypitch.serve.api import create_app
from pypitch.storage.engine import QueryEngine
from pypitch.storage.registry import IdentityRegistry
from pypitch.runtime.cache_duckdb import DuckDBCache
from pypitch.runtime.executor import RuntimeExecutor


class _MockSession:
    def __init__(self) -> None:
        self.registry = IdentityRegistry(":memory:")
        self.engine = QueryEngine(":memory:")
        self.cache = DuckDBCache(":memory:")
        self.executor = RuntimeExecutor(self.cache, self.engine)


def test_analyze_accepts_sql_key(monkeypatch):
    monkeypatch.setattr("pypitch.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("PYPITCH_ANALYZE_ENABLED", "true")
    app = create_app(session=_MockSession(), start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"sql": "SELECT 1 AS x"})
        assert response.status_code == 200
        data = response.json()
        assert data["rows"] == 1
        assert data["data"][0]["x"] == 1


def test_analyze_accepts_legacy_query_key(monkeypatch):
    monkeypatch.setattr("pypitch.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("PYPITCH_ANALYZE_ENABLED", "true")
    app = create_app(session=_MockSession(), start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"query": "SELECT 2 AS x"})
        assert response.status_code == 200
        data = response.json()
        assert data["rows"] == 1
        assert data["data"][0]["x"] == 2


def test_analyze_binds_positional_params(monkeypatch):
    monkeypatch.setattr("pypitch.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("PYPITCH_ANALYZE_ENABLED", "true")
    app = create_app(session=_MockSession(), start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"sql": "SELECT ? AS x", "params": [7]})
        assert response.status_code == 200
        data = response.json()
        assert data["rows"] == 1
        assert data["data"][0]["x"] == 7


def test_analyze_rejects_non_list_params(monkeypatch):
    monkeypatch.setattr("pypitch.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("PYPITCH_ANALYZE_ENABLED", "true")
    app = create_app(session=_MockSession(), start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"sql": "SELECT 1", "params": {"x": 1}})
        assert response.status_code == 400
        assert "params must be a list" in response.json()["detail"]


def test_analyze_rejects_comment_injection(monkeypatch):
    monkeypatch.setattr("pypitch.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("PYPITCH_ANALYZE_ENABLED", "true")
    app = create_app(session=_MockSession(), start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"sql": "SELECT 1 -- sneaky"})
        assert response.status_code == 403
        assert "comments" in response.json()["detail"].lower()
