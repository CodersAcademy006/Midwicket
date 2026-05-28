"""Tests for /analyze payload compatibility and parameter handling."""

from fastapi.testclient import TestClient

from midwicket.serve.api import create_app
from midwicket.storage.engine import QueryEngine
from midwicket.storage.registry import IdentityRegistry
from midwicket.runtime.cache_duckdb import DuckDBCache
from midwicket.runtime.executor import RuntimeExecutor


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
    monkeypatch.setattr("midwicket.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("MIDWICKET_ANALYZE_ENABLED", "true")
    app = create_app(session=_MockSession(), start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"sql": "SELECT 1 AS x"})
        assert response.status_code == 200
        data = response.json()
        assert data["rows"] == 1
        assert data["data"][0]["x"] == 1


def test_analyze_accepts_legacy_query_key(monkeypatch):
    monkeypatch.setattr("midwicket.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("MIDWICKET_ANALYZE_ENABLED", "true")
    app = create_app(session=_MockSession(), start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"query": "SELECT 2 AS x"})
        assert response.status_code == 200
        data = response.json()
        assert data["rows"] == 1
        assert data["data"][0]["x"] == 2


def test_analyze_binds_positional_params(monkeypatch):
    monkeypatch.setattr("midwicket.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("MIDWICKET_ANALYZE_ENABLED", "true")
    app = create_app(session=_MockSession(), start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"sql": "SELECT ? AS x", "params": [7]})
        assert response.status_code == 200
        data = response.json()
        assert data["rows"] == 1
        assert data["data"][0]["x"] == 7


def test_analyze_rejects_non_list_params(monkeypatch):
    monkeypatch.setattr("midwicket.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("MIDWICKET_ANALYZE_ENABLED", "true")
    app = create_app(session=_MockSession(), start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"sql": "SELECT 1", "params": {"x": 1}})
        assert response.status_code == 400
        assert "params must be a list" in response.json()["detail"]


def test_analyze_rejects_comment_injection(monkeypatch):
    monkeypatch.setattr("midwicket.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("MIDWICKET_ANALYZE_ENABLED", "true")
    app = create_app(session=_MockSession(), start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"sql": "SELECT 1 -- sneaky"})
        assert response.status_code == 403
        assert "comments" in response.json()["detail"].lower()


def test_analyze_persists_audit_log_entry(monkeypatch):
    monkeypatch.setattr("midwicket.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("MIDWICKET_ANALYZE_ENABLED", "true")
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
    monkeypatch.setattr("midwicket.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("MIDWICKET_ANALYZE_ENABLED", "true")
    session = _StrictSession()
    app = create_app(session=session, start_ingestor=False)

    with TestClient(app) as client:
        response = client.post("/analyze", json={"sql": "SELECT 5 AS x"})
        assert response.status_code == 200

    audit_count = session.engine.execute_sql(
        "SELECT COUNT(*) AS c FROM audit_log"
    ).to_pydict()["c"][0]
    assert audit_count >= 1


def test_analyze_audit_row_has_usable_id(monkeypatch):
    """Each audit row gets a non-null, identifiable id from the sequence."""
    monkeypatch.setattr("midwicket.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("MIDWICKET_ANALYZE_ENABLED", "true")
    session = _MockSession()
    app = create_app(session=session, start_ingestor=False)

    with TestClient(app) as client:
        assert client.post("/analyze", json={"sql": "SELECT 3 AS x"}).status_code == 200
        assert client.post("/analyze", json={"sql": "SELECT 4 AS x"}).status_code == 200

    ids = session.engine.execute_sql(
        "SELECT id FROM audit_log ORDER BY id"
    ).to_pydict()["id"]
    assert len(ids) >= 2
    assert all(i is not None for i in ids)
    # ids must be distinct/identifiable
    assert len(set(ids)) == len(ids)


def test_analyze_audit_write_failure_is_observable(monkeypatch, caplog):
    """A failed audit write must be logged + metered but must not fail /analyze."""
    monkeypatch.setattr("midwicket.serve.auth.API_KEY_REQUIRED", False)
    monkeypatch.setenv("MIDWICKET_ANALYZE_ENABLED", "true")
    session = _MockSession()
    app = create_app(session=session, start_ingestor=False)

    real_execute = session.engine.execute_sql

    def _failing_execute(sql, *args, **kwargs):
        if str(sql).strip().lower().startswith("insert into audit_log"):
            raise RuntimeError("audit disk full")
        return real_execute(sql, *args, **kwargs)

    recorded = []
    monkeypatch.setattr(
        "midwicket.serve.api.record_error_metrics",
        lambda kind, detail: recorded.append((kind, detail)),
    )

    with TestClient(app) as client:
        monkeypatch.setattr(session.engine, "execute_sql", _failing_execute)
        with caplog.at_level("WARNING"):
            response = client.post("/analyze", json={"sql": "SELECT 9 AS x"})

    # Response still succeeds.
    assert response.status_code == 200
    # Failure is observable.
    assert any("audit_log write failed" in r.message for r in caplog.records)
    assert any(kind == "AuditWriteError" for kind, _ in recorded)
