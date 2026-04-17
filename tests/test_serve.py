"""
Tests for PyPitch REST API Server

Tests the FastAPI-based REST API for serving PyPitch functionality.
"""

import pytest
import json
from unittest.mock import Mock, patch
import tempfile
from pathlib import Path
from pydantic import ValidationError

from pypitch.serve.api import PyPitchAPI, create_app
from pypitch.api.validation import (
    WinPredictionRequest, PlayerLookupRequest, VenueLookupRequest,
    MatchupRequest, FantasyPointsRequest, StatsFilterRequest,
    LiveMatchRegistrationRequest, DeliveryDataRequest
)
from pypitch.exceptions import PyPitchError, DataValidationError

from pypitch.storage.engine import QueryEngine
from pypitch.storage.registry import IdentityRegistry
from pypitch.runtime.cache_duckdb import DuckDBCache
from pypitch.runtime.executor import RuntimeExecutor

class TestPyPitchAPI:
    """Test the PyPitchAPI class."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        yield path
        # Cleanup
        Path(path).unlink(missing_ok=True)

    @pytest.fixture
    def api_instance(self, temp_db_path):
        """Create a PyPitchAPI instance."""
        # Mock session to avoid full DB initialization
        mock_session = Mock()
        mock_session.engine = Mock()
        with PyPitchAPI(session=mock_session) as api:
            yield api

    def test_api_initialization(self, api_instance):
        """Test API initialization."""
        assert api_instance.session is not None
        assert api_instance.app is not None

    def test_predict_win_probability_valid(self, api_instance):
        """Test win probability prediction with valid input."""
        request = WinPredictionRequest(
            target=150,
            current_runs=50,
            wickets_down=2,
            overs_done=10.0,
            venue="wankhede"
        )

        response = api_instance.predict_win_probability(request)

        assert "win_prob" in response
        assert "confidence" in response
        assert isinstance(response["win_prob"], float)
        assert isinstance(response["confidence"], float)

        assert 0.0 <= response["win_prob"] <= 1.0
        assert 0.0 <= response["confidence"] <= 1.0

    def test_predict_win_probability_invalid(self, api_instance):
        """Test win probability prediction with invalid input."""
        # Invalid overs_done
        with pytest.raises(ValidationError):
            request = WinPredictionRequest(
                target=150,
                current_runs=50,
                wickets_down=2,
                overs_done=25.0,  # Invalid: > 20
                venue="wankhede"
            )

        # Invalid wickets_down
        with pytest.raises(ValidationError):
            request = WinPredictionRequest(
                target=150,
                current_runs=50,
                wickets_down=12,  # Invalid: > 10
                overs_done=10.0,
                venue="wankhede"
            )
            api_instance.predict_win_probability(request)

    def test_lookup_player(self, api_instance):
        """Test player lookup functionality."""
        request = PlayerLookupRequest(name="Virat Kohli")

        # This might return empty results if no data is loaded, but should not error
        response = api_instance.lookup_player(request)
        assert isinstance(response, dict)
        assert "player_name" in response
        assert "found" in response

    def test_lookup_venue(self, api_instance):
        """Test venue lookup functionality."""
        request = VenueLookupRequest(name="Wankhede")

        response = api_instance.lookup_venue(request)
        assert isinstance(response, dict)
        assert "venue_name" in response
        assert "found" in response

    def test_get_matchup_stats(self, api_instance):
        """Test matchup statistics retrieval."""
        request = MatchupRequest(
            batter="Virat Kohli",
            bowler="Jasprit Bumrah",
            venue="Wankhede"
        )

        response = api_instance.get_matchup_stats(request)
        assert isinstance(response, dict)
        assert "batter" in response
        assert "bowler" in response
        # H1 fix: returns found + stats (not matches)
        assert "found" in response
        assert "stats" in response

    def test_get_fantasy_points(self, api_instance):
        """Test fantasy points calculation."""
        request = FantasyPointsRequest(
            player_name="Virat Kohli",
            season="2023"
        )

        response = api_instance.get_fantasy_points(request)
        assert isinstance(response, dict)
        assert "player" in response
        assert "points" in response

    def test_get_player_stats(self, api_instance):
        """Test player statistics retrieval."""
        request = StatsFilterRequest(
            player_name="Virat Kohli",
            season="2023"
        )

        response = api_instance.get_player_stats(request)
        assert isinstance(response, dict)
        assert "player" in response
        assert "stats" in response

    def test_register_live_match(self, api_instance):
        """Test registering a match for live tracking."""
        request = LiveMatchRegistrationRequest(
            match_id="test_match_123",
            source="webhook",
            metadata={"venue": "Test Stadium"}
        )

        response = api_instance.register_live_match(request)
        assert response["registered"] is True
        assert response["match_id"] == "test_match_123"
        assert "match_id" in response

    def test_ingest_delivery_data(self, api_instance, monkeypatch):
        """Test ingesting live delivery data."""
        captured = {}

        def _fake_update(match_id, delivery_data):
            captured["match_id"] = match_id
            captured["delivery_data"] = delivery_data

        monkeypatch.setattr(api_instance.ingestor, "update_match_data", _fake_update)

        request = DeliveryDataRequest(
            match_id="test_match_123",
            inning=1,
            over=5,
            ball=3,
            runs_total=45,
            wickets_fallen=1,
            target=150,
            venue="Test Stadium"
        )

        response = api_instance.ingest_delivery_data(request)
        assert response["ingested"] is True
        assert response["match_id"] == "test_match_123"
        assert captured["match_id"] == "test_match_123"
        assert captured["delivery_data"] == {
            "inning": 1,
            "over": 5,
            "ball": 3,
            "runs_total": 45,
            "wickets_fallen": 1,
            "target": 150,
            "venue": "Test Stadium",
        }

    def test_get_live_matches(self, api_instance):
        """Test getting list of live matches."""
        response = api_instance.get_live_matches()
        assert isinstance(response, dict)
        assert "matches" in response
        assert isinstance(response["matches"], list)

    def test_get_health_status(self, api_instance):
        """Test health check endpoint."""
        response = api_instance.get_health_status()

        assert "status" in response
        assert "version" in response
        assert "uptime_seconds" in response
        assert "database_status" in response

        assert response["status"] in ["healthy", "degraded", "unhealthy"]

    def test_error_handling(self, api_instance):
        """Test error handling in API methods."""
        # Test with invalid data that should cause internal errors
        with patch('pypitch.serve.api.wp_func', side_effect=Exception("Test error")):
            request = WinPredictionRequest(
                target=150,
                current_runs=50,
                wickets_down=2,
                overs_done=10.0
            )

            with pytest.raises(Exception):
                api_instance.predict_win_probability(request)

@pytest.fixture
def mock_session():
    """Create a mock session with in-memory databases for testing."""
    # Create session with in-memory databases for testing
    registry = IdentityRegistry(":memory:")
    engine = QueryEngine(":memory:")
    cache = DuckDBCache(":memory:")
    executor = RuntimeExecutor(cache, engine)
    
    # Create a mock session object
    class MockSession:
        def __init__(self) -> None:
            self.registry = registry
            self.engine = engine
            self.cache = cache
            self.executor = executor
    
    return MockSession()

class TestFastAPIApp:
    """Test the FastAPI application creation."""

    def test_create_app(self, mock_session):
        """Test creating the FastAPI application."""
        session = mock_session
        app = create_app(session=session, start_ingestor=False)

        assert app is not None
        assert hasattr(app, 'routes')

        # Check that expected routes exist
        route_paths = [route.path for route in app.routes]
        expected_routes = [
            "/",
            "/health",
            "/matches",
            "/matches/{match_id}",
            "/players/{player_id}",
            "/analyze",
            "/win_probability"
        ]

        for expected_route in expected_routes:
            assert expected_route in route_paths, f"Missing route: {expected_route}"

    def test_create_app_shutdown_stops_ingestor(self, mock_session, monkeypatch):
        """ASGI shutdown should stop the background ingestor exactly once."""
        import pypitch.serve.api as api_mod
        from fastapi.testclient import TestClient

        calls = {"stop": 0}

        class _DummyIngestor:
            def __init__(self, engine):
                self.engine = engine

            def start(self):
                return None

            def stop(self):
                calls["stop"] += 1

        monkeypatch.setattr(api_mod, "StreamIngestor", _DummyIngestor)

        app = api_mod.create_app(session=mock_session, start_ingestor=True)
        with TestClient(app):
            pass

        assert calls["stop"] == 1

    def test_api_initialization_from_worker_thread(self):
        """API initialization should not fail when invoked outside main thread."""
        import threading

        outcome = {}

        def worker():
            try:
                session = Mock()
                session.engine = Mock()
                session.registry = Mock()
                with PyPitchAPI(session=session, start_ingestor=False) as api:
                    outcome["ok"] = hasattr(api.app, "routes")
            except Exception as exc:
                outcome["ok"] = False
                outcome["error"] = repr(exc)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout=5)

        assert outcome.get("ok") is True, outcome.get("error", "thread initialization failed")

    def test_drain_mode_blocks_non_probe_requests(self, mock_session):
        """When draining is active, non-probe requests should be rejected with 503."""
        from fastapi.testclient import TestClient

        with PyPitchAPI(session=mock_session, start_ingestor=False) as api:
            api._draining_event.set()
            with TestClient(api.app, raise_server_exceptions=False) as c:
                response = c.get(
                    "/win_probability",
                    params={
                        "target": 150,
                        "current_runs": 50,
                        "wickets_down": 2,
                        "overs_done": 10.0,
                    },
                )

        assert response.status_code == 503
        assert response.json().get("detail") == "Service draining"

    def test_drain_mode_allows_probe_requests(self, mock_session):
        """Internal probe endpoints should remain available during drain mode."""
        from fastapi.testclient import TestClient

        with PyPitchAPI(session=mock_session, start_ingestor=False) as api:
            api._draining_event.set()
            with TestClient(api.app, raise_server_exceptions=False) as c:
                response = c.get("/_internal/health")

        assert response.status_code == 200
        assert response.json().get("status") == "ok"

    @pytest.fixture
    def client(self, mock_session):
        """Create a test client for the FastAPI app.

        Auth is disabled via PYPITCH_API_KEY_REQUIRED=false in conftest.py.
        TrustedHostMiddleware allows 'testserver' when PYPITCH_ENV=testing.
        """
        from fastapi.testclient import TestClient

        session = mock_session
        app = create_app(session=session, start_ingestor=False)
        with TestClient(app) as client:  # default: raise_server_exceptions=True so real bugs surface
            yield client

    def test_internal_health_endpoint(self, client):
        """Unauthenticated internal health probe must always return 200."""
        response = client.get("/_internal/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_health_endpoint(self, client):
        """Test the v1 health check endpoint."""
        response = client.get("/v1/health")

        assert response.status_code == 200
        data = response.json()

        assert "status" in data
        assert "version" in data
        assert "uptime_seconds" in data
        assert "database_status" in data

    def test_health_endpoint_degraded_when_db_unhealthy(self):
        """/v1/health should degrade (not 500) when DB probe fails."""
        from fastapi.testclient import TestClient

        session = Mock()
        session.registry = Mock()
        session.engine = Mock()
        session.engine.execute_sql.side_effect = RuntimeError("db down")

        app = create_app(session=session, start_ingestor=False)
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/v1/health")

        assert response.status_code == 200
        payload = response.json()
        assert payload.get("status") == "degraded"
        assert payload.get("database_status") == "unhealthy"

    def test_health_legacy_endpoint(self, client):
        """Legacy /health path must still work."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_predict_win_endpoint_valid(self, client):
        """Test the win prediction endpoint with valid data."""
        response = client.get("/win_probability", params={
            "target": 150,
            "current_runs": 50,
            "wickets_down": 2,
            "overs_done": 10.0,
        })

        assert response.status_code == 200
        data = response.json()
        assert "win_prob" in data
        assert "confidence" in data

    def test_predict_win_endpoint_invalid_wickets(self, client):
        """Wickets beyond valid range now returns 422 (FastAPI Query validation)."""
        response = client.get("/win_probability", params={
            "target": 150,
            "current_runs": 50,
            "wickets_down": 12,  # invalid: > 10
            "overs_done": 10.0,
        })
        # H5 fix: Query(le=10) enforced at FastAPI layer → 422 Unprocessable Entity
        assert response.status_code == 422
        assert "detail" in response.json()

    def test_analyze_disabled_by_default(self, client):
        """Custom SQL analysis must be disabled when PYPITCH_ANALYZE_ENABLED is not set."""
        response = client.post("/analyze", json={"sql": "SELECT 1"})
        # Should return 403 when PYPITCH_ANALYZE_ENABLED != 'true'
        assert response.status_code == 403

    def test_auth_required_without_key(self, mock_session, monkeypatch):
        """When auth IS required, missing key returns 401."""
        import os
        import pypitch.serve.auth as auth_mod

        monkeypatch.setenv("PYPITCH_API_KEYS", "valid-key")
        # Patch the module-level constant directly since it's bound at import time
        monkeypatch.setattr(auth_mod, "API_KEY_REQUIRED", True)

        from fastapi.testclient import TestClient
        app = create_app(session=mock_session, start_ingestor=False)
        with TestClient(app, raise_server_exceptions=False) as c:
            response = c.get("/win_probability", params={
                "target": 150, "current_runs": 50,
                "wickets_down": 2, "overs_done": 10.0,
            })
            assert response.status_code == 401

    def test_auth_accepted_with_valid_key(self, mock_session, monkeypatch):
        """Valid X-API-Key header is accepted."""
        import pypitch.serve.auth as auth_mod

        monkeypatch.setenv("PYPITCH_API_KEYS", "valid-key")
        monkeypatch.setattr(auth_mod, "API_KEY_REQUIRED", True)

        from fastapi.testclient import TestClient
        app = create_app(session=mock_session, start_ingestor=False)
        with TestClient(app, raise_server_exceptions=False) as c:
            response = c.get(
                "/win_probability",
                params={"target": 150, "current_runs": 50,
                        "wickets_down": 2, "overs_done": 10.0},
                headers={"X-API-Key": "valid-key"},
            )
            assert response.status_code == 200

    def test_cors_preflight_allows_x_api_key(self, mock_session, monkeypatch):
        """CORS preflight must include X-API-Key for browser legacy clients."""
        import pypitch.serve.api as api_mod
        from fastapi.testclient import TestClient

        monkeypatch.setattr(api_mod, "API_CORS_ORIGINS", ["https://app.example.com"])

        app = api_mod.create_app(session=mock_session, start_ingestor=False)
        with TestClient(app) as c:
            response = c.options(
                "/win_probability",
                headers={
                    "Origin": "https://app.example.com",
                    "Access-Control-Request-Method": "GET",
                    "Access-Control-Request-Headers": "x-api-key,content-type",
                },
            )

        assert response.status_code in (200, 204)
        allow_headers = response.headers.get("access-control-allow-headers", "").lower()
        assert "x-api-key" in allow_headers

    def test_cors_preflight_rejects_unauthorized_origin(self, mock_session, monkeypatch):
        """CORS preflight should not grant allow-origin for disallowed origins."""
        import pypitch.serve.api as api_mod
        from fastapi.testclient import TestClient

        monkeypatch.setattr(api_mod, "API_CORS_ORIGINS", ["https://app.example.com"])

        app = api_mod.create_app(session=mock_session, start_ingestor=False)
        with TestClient(app) as c:
            response = c.options(
                "/win_probability",
                headers={
                    "Origin": "https://evil.example.com",
                    "Access-Control-Request-Method": "GET",
                    "Access-Control-Request-Headers": "x-api-key,content-type",
                },
            )

        assert response.status_code in (400, 200, 204)
        assert "access-control-allow-origin" not in response.headers

    def test_register_live_match_endpoint(self, client):
        """Live match registration returns 503 when ingestor is disabled."""
        payload = {
            "match_id": "test_match_456",
            "source": "webhook",
            "metadata": {"venue": "Test Stadium"},
        }
        response = client.post("/live/register", json=payload)
        assert response.status_code == 503

    def test_ingest_delivery_endpoint(self, client):
        """Delivery ingestion returns 503 when ingestor is disabled."""
        client.post("/live/register", json={"match_id": "test_match_789", "source": "webhook"})
        delivery_payload = {
            "match_id": "test_match_789",
            "inning": 1, "over": 5, "ball": 3,
            "runs_total": 45, "wickets_fallen": 1,
            "target": 150, "venue": "Test Stadium",
        }
        response = client.post("/live/ingest", json=delivery_payload)
        assert response.status_code == 503

    def test_get_live_matches_endpoint(self, client):
        """Live matches endpoint returns a list."""
        response = client.get("/live/matches")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_audit_endpoint_graceful_when_table_missing(self):
        """/v1/audit should degrade to empty output if audit_log table is unavailable."""
        from fastapi.testclient import TestClient

        session = Mock()
        session.registry = Mock()
        session.engine = Mock()

        def _execute_sql(sql, *args, **kwargs):
            normalized = " ".join(str(sql).split()).lower()
            if "create table if not exists audit_log" in normalized:
                raise RuntimeError("read-only connection")
            if "from audit_log" in normalized:
                raise RuntimeError("Catalog Error: Table with name audit_log does not exist")
            raise RuntimeError("unexpected SQL in test")

        session.engine.execute_sql.side_effect = _execute_sql

        app = create_app(session=session, start_ingestor=False)
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/v1/audit")

        assert response.status_code == 200
        assert response.json() == {"entries": [], "count": 0}

if __name__ == "__main__":
    pytest.main([__file__, "-v"])