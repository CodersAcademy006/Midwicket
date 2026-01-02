"""
Tests for PyPitch Live Data Ingestion

Tests the real-time data ingestion pipeline and overlay functionality.
"""

import pytest
import json
import time
import threading
from unittest.mock import Mock, patch
import tempfile
import os
from pathlib import Path

from pypitch.live.ingestor import StreamIngestor, LiveMatch, create_stream_ingestor
from pypitch.live.overlay import LiveStats, OverlayServer
from pypitch.storage.thread_safe_engine import create_thread_safe_engine
from pypitch.exceptions import DataIngestionError

class TestStreamIngestor:
    """Test the StreamIngestor class."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        yield path
        # Cleanup
        Path(path).unlink(missing_ok=True)

    @pytest.fixture
    def thread_safe_engine(self, temp_db_path):
        """Create a thread-safe query engine."""
        return create_thread_safe_engine(temp_db_path)

    @pytest.fixture
    def ingestor(self, thread_safe_engine):
        """Create a StreamIngestor instance."""
        ingestor = StreamIngestor(thread_safe_engine, max_workers=2)
        yield ingestor
        ingestor.stop()

    def test_ingestor_initialization(self, ingestor):
        """Test ingestor initialization."""
        assert ingestor.query_engine is not None
        assert ingestor.max_workers == 2
        assert ingestor.live_matches == {}
        assert ingestor.update_queue is not None
        assert ingestor.webhook_port == 8080

    def test_match_registration(self, ingestor):
        """Test registering matches for live tracking."""
        # Register a match
        result = ingestor.register_match("match_123", "webhook", {"venue": "Test Stadium"})
        assert result is True

        # Check match was registered
        assert "match_123" in ingestor.live_matches
        match = ingestor.live_matches["match_123"]
        assert match.match_id == "match_123"
        assert match.source == "webhook"
        assert match.status == "active"
        assert match.metadata == {"venue": "Test Stadium"}

        # Try to register the same match again
        result = ingestor.register_match("match_123", "webhook")
        assert result is False

    def test_match_unregistration(self, ingestor):
        """Test unregistering matches."""
        # Register and then unregister
        ingestor.register_match("match_123", "webhook")
        assert "match_123" in ingestor.live_matches

        ingestor.unregister_match("match_123")
        assert "match_123" not in ingestor.live_matches

        # Unregistering non-existent match should not error
        ingestor.unregister_match("non_existent")

    def test_update_match_data_registered(self, ingestor):
        """Test updating data for registered matches."""
        # Register match
        ingestor.register_match("match_123", "webhook")

        # Update match data
        delivery_data = {
            'inning': 1,
            'over': 5,
            'ball': 3,
            'runs_total': 45,
            'wickets_fallen': 1,
            'target': None,
            'venue': 'Test Stadium'
        }

        ingestor.update_match_data("match_123", delivery_data)

        # Check that update was queued (we can't easily test the processing without starting the ingestor)
        assert not ingestor.update_queue.empty()

    def test_update_match_data_unregistered(self, ingestor):
        """Test updating data for unregistered matches."""
        delivery_data = {
            'inning': 1,
            'over': 5,
            'ball': 3,
            'runs_total': 45,
            'wickets_fallen': 1
        }

        # This should not raise an error, just log a warning
        ingestor.update_match_data("unregistered_match", delivery_data)

    def test_get_live_matches(self, ingestor):
        """Test getting list of live matches."""
        # Initially empty
        matches = ingestor.get_live_matches()
        assert matches == []

        # Register some matches
        ingestor.register_match("match_1", "webhook", {"venue": "Stadium A"})
        ingestor.register_match("match_2", "api_poll", {"venue": "Stadium B"})

        matches = ingestor.get_live_matches()
        assert len(matches) == 2

        match_ids = [m['match_id'] for m in matches]
        assert "match_1" in match_ids
        assert "match_2" in match_ids

    def test_get_match_status(self, ingestor):
        """Test getting status of specific matches."""
        # Non-existent match
        status = ingestor.get_match_status("non_existent")
        assert status is None

        # Register and check status
        ingestor.register_match("match_123", "webhook", {"venue": "Test"})
        status = ingestor.get_match_status("match_123")

        assert status is not None
        assert status['match_id'] == "match_123"
        assert status['source'] == "webhook"
        assert status['status'] == "active"
        assert status['metadata'] == {"venue": "Test"}
        assert 'last_update' in status
        assert 'seconds_since_update' in status

    def test_add_api_endpoint(self, ingestor):
        """Test adding API endpoints for polling."""
        # Add an endpoint
        ingestor.add_api_endpoint("test_api", "https://api.example.com/live", {"auth": "token123"})

        assert "test_api" in ingestor.api_endpoints
        endpoint = ingestor.api_endpoints["test_api"]
        assert endpoint['url'] == "https://api.example.com/live"
        assert endpoint['headers'] == {"auth": "token123"}

    def test_set_webhook_port(self, ingestor):
        """Test setting webhook port."""
        ingestor.set_webhook_port(9090)
        assert ingestor.webhook_port == 9090

    @patch('pypitch.live.ingestor.requests.get')
    def test_api_polling(self, mock_get, ingestor):
        """Test API polling functionality."""
        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "match_id": "match_123",
            "inning": 1,
            "over": 10,
            "ball": 2,
            "runs_total": 89,
            "wickets_fallen": 2
        }
        mock_get.return_value = mock_response

        # Add API endpoint
        ingestor.add_api_endpoint("test_api", "https://api.example.com/live")

        # Register match
        ingestor.register_match("match_123", "api_poll")

        # Simulate polling (normally done in background thread)
        ingestor._poll_apis()

        # Check that API was called
        mock_get.assert_called_once_with("https://api.example.com/live", headers={}, timeout=10)

        # Check that data was queued for processing
        assert not ingestor.update_queue.empty()

    def test_ingest_delivery_data_valid(self, thread_safe_engine):
        """Test ingesting valid delivery data."""
        # Create ingestor
        ingestor = StreamIngestor(thread_safe_engine)

        # Valid delivery data
        delivery_data = {
            'match_id': 'test_match',
            'inning': 1,
            'over': 5,
            'ball': 3,
            'runs_total': 45,
            'wickets_fallen': 1,
            'target': None,
            'venue': 'Test Stadium',
            'timestamp': time.time()
        }

        # This should not raise an error
        ingestor._ingest_delivery_data('test_match', delivery_data)

        # Verify data was inserted
        result = thread_safe_engine.execute_sql("SELECT COUNT(*) as count FROM ball_events")
        assert result['count'][0] == 1

    def test_ingest_delivery_data_invalid(self, thread_safe_engine):
        """Test ingesting invalid delivery data."""
        ingestor = StreamIngestor(thread_safe_engine)

        # Missing required fields
        invalid_data = {
            'match_id': 'test_match',
            'inning': 1,
            # Missing required fields
        }

        with pytest.raises(DataIngestionError):
            ingestor._ingest_delivery_data('test_match', invalid_data)

    def test_create_stream_ingestor(self, thread_safe_engine):
        """Test the convenience function for creating stream ingestors."""
        ingestor = create_stream_ingestor(thread_safe_engine)

        assert isinstance(ingestor, StreamIngestor)
        assert ingestor.query_engine == thread_safe_engine
        assert ingestor.on_match_update is not None
        assert ingestor.on_match_complete is not None

class TestLiveStats:
    """Test the LiveStats dataclass."""

    def test_livestats_creation(self):
        """Test creating LiveStats instances."""
        stats = LiveStats(
            match_id="match_123",
            team_batting="MI",
            team_bowling="CSK",
            current_runs=145,
            wickets_down=2,
            overs_completed=15.2,
            run_rate=9.5,
            required_run_rate=8.2,
            target=180,
            win_probability=0.75
        )

        assert stats.match_id == "match_123"
        assert stats.team_batting == "MI"
        assert stats.team_bowling == "CSK"
        assert stats.current_runs == 145
        assert stats.wickets_down == 2
        assert stats.overs_completed == 15.2
        assert stats.run_rate == 9.5
        assert stats.required_run_rate == 8.2
        assert stats.target == 180
        assert stats.win_probability == 0.75

    def test_livestats_to_json(self):
        """Test converting LiveStats to JSON."""
        stats = LiveStats(
            match_id="match_123",
            team_batting="MI",
            team_bowling="CSK",
            current_runs=145,
            wickets_down=2,
            overs_completed=15.2,
            run_rate=9.5,
            required_run_rate=8.2,
            target=180,
            win_probability=0.75
        )

        json_str = stats.to_json()
        data = json.loads(json_str)

        assert data['match_id'] == "match_123"
        assert data['current_runs'] == 145
        assert data['win_probability'] == 0.75

class TestOverlayServer:
    """Test the OverlayServer class."""

    @pytest.fixture
    def overlay_server(self):
        """Create an OverlayServer instance."""
        server = OverlayServer(port=0)  # Use port 0 for automatic assignment
        yield server
        server.stop()

    def test_overlay_server_initialization(self, overlay_server):
        """Test overlay server initialization."""
        assert overlay_server.port >= 1024  # Should be assigned a port
        assert overlay_server.stats == {}
        assert overlay_server.server is not None

    def test_update_stats(self, overlay_server):
        """Test updating match statistics."""
        stats = LiveStats(
            match_id="match_123",
            team_batting="MI",
            team_bowling="CSK",
            current_runs=145,
            wickets_down=2,
            overs_completed=15.2,
            run_rate=9.5,
            required_run_rate=8.2,
            target=180,
            win_probability=0.75
        )

        overlay_server.update_stats("match_123", stats)

        assert "match_123" in overlay_server.stats
        stored_stats = overlay_server.stats["match_123"]
        assert stored_stats.match_id == "match_123"
        assert stored_stats.current_runs == 145

    def test_get_stats_json(self, overlay_server):
        """Test getting statistics as JSON."""
        # Initially empty
        json_data = overlay_server.get_stats_json()
        assert json_data == "{}"

        # Add some stats
        stats = LiveStats(
            match_id="match_123",
            team_batting="MI",
            team_bowling="CSK",
            current_runs=145,
            wickets_down=2,
            overs_completed=15.2,
            run_rate=9.5,
            required_run_rate=8.2,
            target=180,
            win_probability=0.75
        )

        overlay_server.update_stats("match_123", stats)

        json_data = overlay_server.get_stats_json()
        data = json.loads(json_data)

        assert "match_123" in data
        assert data["match_123"]["current_runs"] == 145
        assert data["match_123"]["win_probability"] == 0.75

    @patch('pypitch.live.overlay.http.server.HTTPServer')
    def test_server_start_stop(self, mock_http_server, overlay_server):
        """Test starting and stopping the server."""
        # Mock the server
        mock_server_instance = Mock()
        mock_http_server.return_value = mock_server_instance

        # Start server
        overlay_server.start()

        # Verify server was started
        mock_http_server.assert_called_once()
        mock_server_instance.serve_forever.assert_called_once()

        # Stop server
        overlay_server.stop()
        mock_server_instance.shutdown.assert_called_once()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])