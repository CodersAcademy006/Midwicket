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
        api = PyPitchAPI(session=mock_session)
        yield api
        # api.session.close() # Mock doesn't need closing

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
        assert "matches" in response
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

    def test_ingest_delivery_data(self, api_instance):
        """Test ingesting live delivery data."""
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
        with patch('pypitch.compute.winprob.win_probability', side_effect=Exception("Test error")):
            request = WinPredictionRequest(
                target=150,
                current_runs=50,
                wickets_down=2,
                overs_done=10.0
            )

            with pytest.raises(Exception):
                api_instance.predict_win_probability(request)

class TestFastAPIApp:
    """Test the FastAPI application creation."""

    def test_create_app(self):
        """Test creating the FastAPI application."""
        app = create_app()

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

    @pytest.fixture
    def client(self):
        """Create a test client for the FastAPI app."""
        from fastapi.testclient import TestClient
        app = create_app()
        return TestClient(app)

    def test_health_endpoint(self, client):
        """Test the health check endpoint."""
        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()

        assert "status" in data
        assert "version" in data
        assert "uptime_seconds" in data
        assert "database_status" in data

    def test_predict_win_endpoint_valid(self, client):
        """Test the win prediction endpoint with valid data."""
        # Use GET request with query parameters
        response = client.get("/win_probability", params={
            "target": 150,
            "current_runs": 50,
            "wickets_down": 2,
            "overs_done": 10.0
        })

        assert response.status_code == 200
        data = response.json()

        assert "win_prob" in data
        assert "confidence" in data

    def test_predict_win_endpoint_invalid(self, client):
        """Test the win prediction endpoint with invalid data."""
        # Test with invalid wickets_down
        response = client.get("/win_probability", params={
            "target": 150,
            "current_runs": 50,
            "wickets_down": 12,  # Invalid: > 10
            "overs_done": 10.0
        })

        # The function raises an exception for invalid inputs
        assert response.status_code == 500
        data = response.json()
        assert "detail" in data

    # @pytest.mark.skip(reason="Live match registration endpoint not implemented")
    def test_register_live_match_endpoint(self, client):
        """Test the live match registration endpoint."""
        payload = {
            "match_id": "test_match_456",
            "source": "webhook",
            "metadata": {"venue": "Test Stadium"}
        }

        response = client.post("/live/register", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["match_id"] == "test_match_456"

    # @pytest.mark.skip(reason="Delivery data ingestion endpoint not implemented")
    def test_ingest_delivery_endpoint(self, client):
        """Test the delivery data ingestion endpoint."""
        # First register a match
        register_payload = {
            "match_id": "test_match_789",
            "source": "webhook"
        }
        client.post("/live/register", json=register_payload)

        # Then ingest delivery data
        delivery_payload = {
            "match_id": "test_match_789",
            "inning": 1,
            "over": 5,
            "ball": 3,
            "runs_total": 45,
            "wickets_fallen": 1,
            "target": 150,
            "venue": "Test Stadium"
        }

        response = client.post("/live/ingest", json=delivery_payload)

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    # @pytest.mark.skip(reason="Live matches endpoint not implemented")
    def test_get_live_matches_endpoint(self, client):
        """Test the get live matches endpoint."""
        response = client.get("/live/matches")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])