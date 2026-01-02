"""
Tests for PyPitch Report Plugin

Tests PDF generation, chart creation, and template rendering.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch
from dataclasses import dataclass
from datetime import datetime
from reportlab.lib.pagesizes import A4

from pypitch.report.pdf import PDFGenerator, ChartConfig, create_scouting_report, create_match_report
from pypitch.api.session import PyPitchSession


# Mock classes for testing
@dataclass
class BattingStats:
    date: datetime
    average: float
    runs: int
    balls: int

@dataclass
class BowlingStats:
    date: datetime
    economy: float
    wickets: int
    overs: float

@dataclass
class RecentMatch:
    opponent: str
    date: datetime
    runs: int
    wickets: int

@dataclass
class CareerStats:
    total_runs: int
    total_wickets: int
    batting_average: float
    bowling_average: float
    highest_score: int
    best_bowling: str
    strike_rate: float
    economy_rate: float

@dataclass
class PlayerStats:
    player_id: str
    name: str
    team: str
    batting_stats: list
    bowling_stats: list
    recent_matches: list
    career_stats: CareerStats

@dataclass
class TopPerformer:
    name: str
    runs: int
    wickets: int

@dataclass
class MatchStats:
    match_id: str
    team1: str
    team2: str
    team1_score: int
    team2_score: int
    team1_wickets: int
    team2_wickets: int
    winner: str
    venue: str
    date: datetime
    margin: str
    overs: float
    run_rate: float
    partnerships: int
    top_performers: list


class TestChartConfig:
    """Test chart configuration."""

    def test_default_config(self):
        """Test default chart configuration."""
        config = ChartConfig()
        assert config.figsize == (8, 6)
        assert config.dpi == 100
        assert config.style == 'seaborn-v0_8'
        assert 'primary' in config.colors
        assert 'secondary' in config.colors

    def test_custom_config(self):
        """Test custom chart configuration."""
        colors = {'primary': '#ff0000', 'secondary': '#00ff00'}
        config = ChartConfig(figsize=(10, 8), dpi=150, colors=colors)
        assert config.figsize == (10, 8)
        assert config.dpi == 150
        assert config.colors['primary'] == '#ff0000'


class TestPDFGenerator:
    """Test PDF generator functionality."""

    @pytest.fixture
    def mock_session(self):
        """Create mock session."""
        session = Mock(spec=PyPitchSession)
        return session

    @pytest.fixture
    def generator(self, mock_session):
        """Create PDF generator instance."""
        return PDFGenerator(mock_session)

    def test_init(self, generator):
        """Test generator initialization."""
        assert generator.session is not None
        assert generator.config is not None
        assert generator.styles is not None

    def test_generate_chart(self, generator):
        """Test chart generation to image file."""
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots()
        ax.plot([1, 2, 3], [1, 4, 2])
        ax.set_title('Test Chart')

        result = generator._generate_chart_image(fig)
        assert result.endswith('.png')
        assert os.path.exists(result)

        # Clean up
        os.unlink(result)

    @patch('pypitch.report.pdf.SimpleDocTemplate')
    def test_create_scouting_report(self, mock_doc, generator, mock_session):
        """Test scouting report creation."""
        # Mock player stats using the actual PlayerStats model
        from pypitch.api.models import PlayerStats
        player_stats = PlayerStats(
            name="Test Player",
            matches=50,
            runs=1000,
            balls_faced=800,
            wickets=25,
            balls_bowled=400,
            runs_conceded=350
        )

        mock_session.get_player_stats = Mock(return_value=player_stats)

        # Mock PDF document
        mock_doc_instance = Mock()
        mock_doc.return_value = mock_doc_instance

        generator.create_scouting_report("player_123", "output.pdf")

        mock_session.get_player_stats.assert_called_once_with("player_123")
        mock_doc.assert_called_once_with("output.pdf", pagesize=A4)
        mock_doc_instance.build.assert_called_once()

    @patch('pypitch.report.pdf.PDFGenerator._create_match_comparison_chart')
    @patch('pypitch.report.pdf.SimpleDocTemplate')
    @patch('os.unlink')
    def test_create_match_report(self, mock_unlink, mock_doc, mock_chart, generator, mock_session):
        """Test match report creation."""
        # Mock match stats
        match_stats = Mock()
        match_stats.team1 = "Team A"
        match_stats.team2 = "Team B"
        match_stats.team1_score = 250
        match_stats.team2_score = 240
        match_stats.team1_wickets = 5
        match_stats.team2_wickets = 8
        match_stats.winner = "Team A"
        match_stats.venue = "Test Stadium"
        match_stats.date = datetime.now()
        match_stats.margin = "10 runs"
        match_stats.top_performers = []

        mock_session.get_match_stats = Mock(return_value=match_stats)
        mock_chart.return_value = "/tmp/test_chart.png"

        # Mock PDF document
        mock_doc_instance = Mock()
        mock_doc.return_value = mock_doc_instance

        generator.create_match_report("match_123", "output.pdf")

        mock_session.get_match_stats.assert_called_once_with("match_123")
        mock_doc.assert_called_once_with("output.pdf", pagesize=A4)
        mock_doc_instance.build.assert_called_once()
        mock_unlink.assert_called_once_with("/tmp/test_chart.png")

    def test_player_not_found(self, generator, mock_session):
        """Test error when player not found."""
        mock_session.get_player_stats = Mock(return_value=None)

        with pytest.raises(ValueError, match="Player test_player not found"):
            generator.create_scouting_report("test_player", "output.pdf")

    def test_match_not_found(self, generator, mock_session):
        """Test error when match not found."""
        mock_session.get_match_stats = Mock(return_value=None)

        with pytest.raises(ValueError, match="Match test_match not found"):
            generator.create_match_report("test_match", "output.pdf")


class TestConvenienceFunctions:
    """Test convenience functions."""

    @patch('pypitch.report.pdf.PDFGenerator')
    def test_create_scouting_report_func(self, mock_generator_class):
        """Test create_scouting_report convenience function."""
        mock_session = Mock()
        mock_generator = Mock()
        mock_generator_class.return_value = mock_generator

        create_scouting_report(mock_session, "player_123", "output.pdf")

        mock_generator_class.assert_called_once_with(mock_session)
        mock_generator.create_scouting_report.assert_called_once_with("player_123", "output.pdf")

    @patch('pypitch.report.pdf.PDFGenerator')
    def test_create_match_report_func(self, mock_generator_class):
        """Test create_match_report convenience function."""
        mock_session = Mock()
        mock_generator = Mock()
        mock_generator_class.return_value = mock_generator

        create_match_report(mock_session, "match_123", "output.pdf")

        mock_generator_class.assert_called_once_with(mock_session)
        mock_generator.create_match_report.assert_called_once_with("match_123", "output.pdf")


class TestIntegration:
    """Integration tests for report generation."""

    @pytest.fixture
    def sample_player_stats(self):
        """Create sample player statistics."""
        from pypitch.api.models import PlayerStats
        
        return PlayerStats(
            name="Virat Kohli",
            matches=100,
            runs=2500,
            balls_faced=2000,
            wickets=85,
            balls_bowled=1200,
            runs_conceded=1800
        )

    @patch('pypitch.report.pdf.SimpleDocTemplate')
    def test_full_scouting_report_generation(self, mock_doc, sample_player_stats):
        """Test full scouting report generation with real data."""
        mock_session = Mock(spec=PyPitchSession)
        mock_session.get_player_stats = Mock(return_value=sample_player_stats)

        generator = PDFGenerator(mock_session)

        # Mock PDF document
        mock_doc_instance = Mock()
        mock_doc.return_value = mock_doc_instance

        generator.create_scouting_report("virat_kohli", "output.pdf")

        # Verify session was called
        mock_session.get_player_stats.assert_called_once_with("virat_kohli")

        # Verify PDF generation was attempted
        mock_doc.assert_called_once()
        mock_doc_instance.build.assert_called_once()