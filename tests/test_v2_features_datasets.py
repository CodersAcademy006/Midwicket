"""
Unit and Integration Tests for Midwicket v2 Dataset Hub, Feature Store, and Scouting Reports.
"""

import pytest
import pandas as pd
from typing import Generator

# Internal Imports
import midwicket as md
from midwicket.datasets import list_datasets, load_dataset
from midwicket.features import load_features
from midwicket.report.scout import scouting_report

@pytest.fixture(scope="module")
def mlc_session() -> Generator[md.MidwicketSession, None, None]:
    """Test fixture establishing an active MLC dataset session."""
    session = load_dataset("mlc")
    yield session
    # Cleanup session after tests run
    session.close()

def test_list_datasets():
    """Verify that catalog metadata resolves correctly."""
    catalog = list_datasets()
    assert "ipl" in catalog
    assert "all_t20" in catalog
    assert "mlc" in catalog
    assert catalog["mlc"]["format"] == "T20"

def test_load_dataset_mlc(mlc_session):
    """Verify dataset loading maps schema correctly and registers events."""
    # Ensure ball events were loaded
    res = mlc_session.engine.execute_sql("SELECT COUNT(*) as count FROM ball_events")
    count = res.to_pydict()["count"][0]
    assert count > 0

def test_load_features_pressure_index(mlc_session):
    """Test pressure index feature generation."""
    df = load_features("pressure_index", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "pressure_index" in df.columns
    assert df["pressure_index"].min() >= 0.0
    assert df["pressure_index"].max() <= 10.0

def test_load_features_bowler_quality_rating(mlc_session):
    """Test bowler quality rating (BQR) feature calculation."""
    df = load_features("bowler_quality_rating", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "bowler_quality_rating" in df.columns
    # BQR should be scaled appropriately
    assert df["bowler_quality_rating"].min() >= 0.0
    assert df["bowler_quality_rating"].max() <= 100.0

def test_load_features_batter_intent_score(mlc_session):
    """Test batter intent score (BIS)."""
    df = load_features("batter_intent_score", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "intent_score" in df.columns
    assert df["intent_score"].min() >= 0.0

def test_load_features_match_context_score(mlc_session):
    """Test match context score (MCS)."""
    df = load_features("match_context_score", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "match_context_score" in df.columns

def test_load_features_venue_bias_rating(mlc_session):
    """Test venue bias rating (VBR)."""
    df = load_features("venue_bias_rating", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "venue_bias_rating" in df.columns
    assert df["venue_bias_rating"].min() >= 0.0

def test_load_features_expected_runs(mlc_session):
    """Test expected runs (xRuns)."""
    df = load_features("expected_runs", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "expected_runs" in df.columns
    assert df["expected_runs"].min() >= 0.0

def test_load_features_expected_wickets(mlc_session):
    """Test expected wickets (xWickets)."""
    df = load_features("expected_wickets", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "expected_wickets" in df.columns
    assert df["expected_wickets"].min() >= 0.0
    assert df["expected_wickets"].max() <= 1.0

def test_load_features_batting_form(mlc_session):
    """Test batting form moving average feature."""
    df = load_features("batting_form", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "avg_runs_last_5" in df.columns
    assert "strike_rate_last_5" in df.columns

def test_load_features_death_over_metrics(mlc_session):
    """Test death over metrics splits."""
    df = load_features("death_over_metrics", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "player_id" in df.columns
    assert "death_strike_rate" in df.columns or "death_economy" in df.columns

def test_load_features_venue_adjusted_form(mlc_session):
    """Test venue adjusted form calculation."""
    df = load_features("venue_adjusted_form", mlc_session)
    assert isinstance(df, pd.DataFrame)
    assert "venue_adjusted_rating" in df.columns

def test_scouting_report_v2(mlc_session):
    """Test generating a player scouting report on loaded data."""
    # Find an active player in database
    res = mlc_session.registry.con.execute(
        "SELECT primary_name FROM entities WHERE id = 127"
    ).fetchone()
    assert res is not None
    player_name = res[0]
    
    # Generate scouting report
    report = scouting_report(player_name)
    assert isinstance(report, dict)
    assert report["player"].lower() == player_name.lower()
    assert "strengths" in report
    assert "weaknesses" in report
    assert "career_summary" in report
    assert "recent_form" in report
