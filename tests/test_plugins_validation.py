"""
Tests for PyPitch Plugin System and Input Validation

Tests the plugin management system and Pydantic validation models.
"""

import pytest
from unittest.mock import Mock, patch

from pypitch.api.plugins import PluginManager, PluginSpec, get_plugin_manager, register_plugin
from pypitch.api.validation import (
    WinPredictionRequest, PlayerLookupRequest, VenueLookupRequest,
    MatchupRequest, FantasyPointsRequest, StatsFilterRequest,
    LiveMatchRegistrationRequest, DeliveryDataRequest,
    WinPredictionResponse, ErrorResponse, HealthCheckResponse,
    PluginInfo, PluginListResponse
)

class TestPluginManager:
    """Test the PluginManager class."""

    @pytest.fixture
    def plugin_manager(self):
        """Create a fresh PluginManager instance."""
        return PluginManager()

    def test_plugin_manager_initialization(self, plugin_manager):
        """Test plugin manager initialization."""
        assert plugin_manager._registry == {}
        assert plugin_manager._loaded_plugins == {}
        assert plugin_manager._metric_functions == {}
        assert plugin_manager._report_functions == {}
        assert plugin_manager._data_sources == {}
        assert plugin_manager._models == {}

    def test_discover_plugins_no_env(self, plugin_manager):
        """Test plugin discovery when no environment variable is set."""
        plugins = plugin_manager.discover_plugins()
        assert plugins == []

    @patch.dict('os.environ', {'PYPITCH_PLUGINS': 'test_plugin:test_module'})
    def test_discover_plugins_with_env(self, plugin_manager):
        """Test plugin discovery with environment variable."""
        plugins = plugin_manager.discover_plugins()
        assert len(plugins) == 1
        assert plugins[0].name == "test_plugin"
        assert plugins[0].entry_point == "test_module"

    @patch('importlib.import_module')
    def test_load_plugin_success(self, mock_import, plugin_manager):
        """Test successful plugin loading."""
        # Mock the plugin module
        mock_module = Mock()
        mock_module.register_metrics.return_value = {"test_metric": lambda x: x}
        mock_module.register_reports.return_value = {"test_report": lambda x: "report"}
        mock_module.register_data_sources.return_value = {"test_source": "source"}
        mock_module.register_models.return_value = {"test_model": "model"}
        mock_import.return_value = mock_module

        plugin_spec = PluginSpec(
            name="test_plugin",
            entry_point="test_module"
        )

        result = plugin_manager.load_plugin(plugin_spec)
        assert result is True

        # Check that components were registered
        assert "test_metric" in plugin_manager._metric_functions
        assert "test_report" in plugin_manager._report_functions
        assert "test_source" in plugin_manager._data_sources
        assert "test_model" in plugin_manager._models
        assert "test_plugin" in plugin_manager._loaded_plugins

    @patch('importlib.import_module')
    def test_load_plugin_missing_dependency(self, mock_import, plugin_manager):
        """Test plugin loading with missing dependency."""
        mock_import.side_effect = ImportError("No module named 'missing_dep'")

        plugin_spec = PluginSpec(
            name="test_plugin",
            entry_point="test_module",
            dependencies=["missing_dep"]
        )

        result = plugin_manager.load_plugin(plugin_spec)
        assert result is False

    def test_get_metric(self, plugin_manager):
        """Test getting registered metrics."""
        # Manually add a metric
        plugin_manager._metric_functions["test_metric"] = lambda x: x * 2

        func = plugin_manager.get_metric("test_metric")
        assert func is not None
        assert func(5) == 10

        # Test non-existent metric
        func = plugin_manager.get_metric("non_existent")
        assert func is None

    def test_get_report(self, plugin_manager):
        """Test getting registered reports."""
        plugin_manager._report_functions["test_report"] = lambda x: f"Report: {x}"

        func = plugin_manager.get_report("test_report")
        assert func is not None
        assert func("data") == "Report: data"

    def test_get_data_source(self, plugin_manager):
        """Test getting registered data sources."""
        plugin_manager._data_sources["test_source"] = "mock_source"

        source = plugin_manager.get_data_source("test_source")
        assert source == "mock_source"

    def test_get_model(self, plugin_manager):
        """Test getting registered models."""
        plugin_manager._models["test_model"] = "mock_model"

        model = plugin_manager.get_model("test_model")
        assert model == "mock_model"

    def test_list_functions(self, plugin_manager):
        """Test listing registered components."""
        plugin_manager._metric_functions = {"metric1": None, "metric2": None}
        plugin_manager._report_functions = {"report1": None}
        plugin_manager._data_sources = {"source1": None}
        plugin_manager._models = {"model1": None}

        assert plugin_manager.list_metrics() == ["metric1", "metric2"]
        assert plugin_manager.list_reports() == ["report1"]
        assert plugin_manager.list_data_sources() == ["source1"]
        assert plugin_manager.list_models() == ["model1"]

    def test_register_plugin_decorator(self):
        """Test the register_plugin decorator."""
        manager = get_plugin_manager()

        @register_plugin("metrics")
        def custom_strike_rate(data):
            return len(data) * 100

        # Check that function was registered
        func = manager.get_metric("custom_strike_rate")
        assert func is not None
        assert func([1, 2, 3]) == 300

class TestValidationModels:
    """Test Pydantic validation models."""

    def test_win_prediction_request_valid(self):
        """Test valid WinPredictionRequest."""
        request = WinPredictionRequest(
            target=150,
            current_runs=50,
            wickets_down=2,
            overs_done=10.5,
            venue="wankhede"
        )

        assert request.target == 150
        assert request.current_runs == 50
        assert request.wickets_down == 2
        assert request.overs_done == 10.5
        assert request.venue == "wankhede"

    def test_win_prediction_request_invalid_overs(self):
        """Test WinPredictionRequest with invalid overs."""
        with pytest.raises(ValueError, match="overs_done must be between 0 and 20"):
            WinPredictionRequest(
                target=150,
                current_runs=50,
                wickets_down=2,
                overs_done=25.0  # Invalid
            )

    def test_win_prediction_request_invalid_wickets(self):
        """Test WinPredictionRequest with invalid wickets."""
        with pytest.raises(ValueError, match="wickets_down must be between 0 and 10"):
            WinPredictionRequest(
                target=150,
                current_runs=50,
                wickets_down=12,  # Invalid
                overs_done=10.0
            )

    def test_player_lookup_request_valid(self):
        """Test valid PlayerLookupRequest."""
        request = PlayerLookupRequest(name="Virat Kohli")
        assert request.name == "Virat Kohli"
        assert request.date_context is None

    def test_player_lookup_request_invalid_name(self):
        """Test PlayerLookupRequest with invalid name."""
        with pytest.raises(ValueError, match="Name contains invalid characters"):
            PlayerLookupRequest(name="Virat123")  # Invalid characters

    def test_venue_lookup_request(self):
        """Test VenueLookupRequest."""
        request = VenueLookupRequest(name="Wankhede Stadium")
        assert request.name == "Wankhede Stadium"

    def test_matchup_request(self):
        """Test MatchupRequest."""
        request = MatchupRequest(
            batter="Virat Kohli",
            bowler="Jasprit Bumrah",
            venue="Wankhede"
        )
        assert request.batter == "Virat Kohli"
        assert request.bowler == "Jasprit Bumrah"
        assert request.venue == "Wankhede"

    def test_fantasy_points_request(self):
        """Test FantasyPointsRequest."""
        request = FantasyPointsRequest(
            player_name="Virat Kohli",
            season="2023"
        )
        assert request.player_name == "Virat Kohli"
        assert request.season == "2023"

    def test_stats_filter_request(self):
        """Test StatsFilterRequest."""
        request = StatsFilterRequest(
            player_name="Virat Kohli",
            team="RCB",
            min_matches=10
        )
        assert request.player_name == "Virat Kohli"
        assert request.team == "RCB"
        assert request.min_matches == 10

    def test_live_match_registration_request(self):
        """Test LiveMatchRegistrationRequest."""
        request = LiveMatchRegistrationRequest(
            match_id="match_123",
            source="webhook",
            metadata={"venue": "Test"}
        )
        assert request.match_id == "match_123"
        assert request.source == "webhook"
        assert request.metadata == {"venue": "Test"}

    def test_live_match_registration_invalid_source(self):
        """Test LiveMatchRegistrationRequest with invalid source."""
        with pytest.raises(ValueError):
            LiveMatchRegistrationRequest(
                match_id="match_123",
                source="invalid_source"  # Invalid
            )

    def test_delivery_data_request_valid(self):
        """Test valid DeliveryDataRequest."""
        request = DeliveryDataRequest(
            match_id="match_123",
            inning=1,
            over=5,
            ball=3,
            runs_total=45,
            wickets_fallen=1,
            target=150,
            venue="Test Stadium"
        )

        assert request.match_id == "match_123"
        assert request.inning == 1
        assert request.over == 5
        assert request.ball == 3
        assert request.runs_total == 45
        assert request.wickets_fallen == 1

    def test_delivery_data_request_invalid_inning(self):
        """Test DeliveryDataRequest with invalid inning."""
        with pytest.raises(ValueError):
            DeliveryDataRequest(
                match_id="match_123",
                inning=3,  # Invalid: not 1 or 2
                over=5,
                ball=3,
                runs_total=45,
                wickets_fallen=1
            )

class TestResponseModels:
    """Test response model validation."""

    def test_win_prediction_response(self):
        """Test WinPredictionResponse."""
        response = WinPredictionResponse(
            win_prob=0.75,
            confidence=0.85,
            runs_remaining=100,
            balls_remaining=60,
            run_rate_required=10.0,
            venue_adjustment=0.15
        )

        assert response.win_prob == 0.75
        assert response.confidence == 0.85
        assert response.runs_remaining == 100
        assert response.balls_remaining == 60
        assert response.run_rate_required == 10.0
        assert response.venue_adjustment == 0.15

    def test_error_response(self):
        """Test ErrorResponse."""
        response = ErrorResponse(
            error="Validation failed",
            error_type="DataValidationError",
            details={"field": "target", "issue": "must be positive"}
        )

        assert response.error == "Validation failed"
        assert response.error_type == "DataValidationError"
        assert response.details == {"field": "target", "issue": "must be positive"}

    def test_health_check_response(self):
        """Test HealthCheckResponse."""
        response = HealthCheckResponse(
            status="healthy",
            version="1.0.0",
            uptime_seconds=3600.5,
            database_status="connected",
            active_connections=5
        )

        assert response.status == "healthy"
        assert response.version == "1.0.0"
        assert response.uptime_seconds == 3600.5
        assert response.database_status == "connected"
        assert response.active_connections == 5

    def test_plugin_info(self):
        """Test PluginInfo model."""
        info = PluginInfo(
            name="test_plugin",
            version="1.0.0",
            description="A test plugin",
            loaded=True
        )

        assert info.name == "test_plugin"
        assert info.version == "1.0.0"
        assert info.description == "A test plugin"
        assert info.loaded is True

    def test_plugin_list_response(self):
        """Test PluginListResponse."""
        plugins = [
            PluginInfo(name="plugin1", version="1.0", description="Plugin 1", loaded=True),
            PluginInfo(name="plugin2", version="2.0", description="Plugin 2", loaded=False)
        ]

        response = PluginListResponse(
            plugins=plugins,
            loaded_count=1
        )

        assert len(response.plugins) == 2
        assert response.loaded_count == 1

if __name__ == "__main__":
    pytest.main([__file__, "-v"])