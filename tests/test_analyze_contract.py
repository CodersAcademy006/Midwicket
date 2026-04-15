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


class _StrictWriteModeEngine:
    """Wrap QueryEngine and reject write SQL when read_only=True."""

    def __init__(self, engine: QueryEngine) -> None:
        self._engine = engine

    def execute_sql(self, sql, params=None, read_only=True, timeout=None):
        statement = sql.strip().split(None, 1)[0].lower() if sql and sql.strip() else ""
        if statement in {"insert", "create", "update", "delete", "drop", "alter", "truncate"} and read_only:
            raise AssertionError(f"Write SQL executed in read_only mode: {statement}")
        return self._engine.execute_sql(sql, params=params, read_only=read_only, timeout=timeout)

    def __getattr__(self, name):
        return getattr(self._engine, name)


class _StrictSession:
    def __init__(self) -> None:
        self.registry = IdentityRegistry(":memory:")
        self.engine = _StrictWriteModeEngine(QueryEngine(":memory:"))
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


def test_analyze_persists_audit_log_entry(monkeypatch):
    monkeypatch.setattr("pypitch.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("PYPITCH_ANALYZE_ENABLED", "true")
    session = _MockSession()
    app = create_app(session=session, start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"sql": "SELECT 3 AS x"})
        assert response.status_code == 200

    audit_count = session.engine.execute_sql(
        "SELECT COUNT(*) AS c FROM audit_log"
    ).to_pydict()["c"][0]
    assert audit_count >= 1


def test_analyze_audit_write_paths_use_write_mode(monkeypatch):
    monkeypatch.setattr("pypitch.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("PYPITCH_ANALYZE_ENABLED", "true")
    session = _StrictSession()
    app = create_app(session=session, start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"sql": "SELECT 5 AS x"})
        assert response.status_code == 200

    audit_count = session.engine.execute_sql(
        "SELECT COUNT(*) AS c FROM audit_log"
    ).to_pydict()["c"][0]
    assert audit_count >= 1
