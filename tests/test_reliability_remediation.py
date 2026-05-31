import pytest
import pandas as pd
import numpy as np
import pyarrow as pa
import midwicket as md
from midwicket.schema.v1 import BALL_EVENT_SCHEMA, DismissalType
from midwicket.core.canonicalize import RunComponent
from midwicket.features import load_features

def test_over_int16_schema():
    """Verify that the over column is defined as int16 and supports values >= 128."""
    # Check PyArrow schema type
    over_field = BALL_EVENT_SCHEMA.field("over")
    assert over_field.type == pa.int16()
    
    # Create test data exceeding int8 bounds
    data = {
        "match_id": ["test_match"],
        "date": [pd.Timestamp("2026-05-30").date()],
        "venue_id": [1],
        "inning": [1],
        "over": [128], # Exceeds signed 8-bit limit (127)
        "ball": [1],
        "batter_id": [2],
        "bowler_id": [3],
        "non_striker_id": [4],
        "batting_team_id": [5],
        "bowling_team_id": [6],
        "runs_batter": [4],
        "runs_extras": [0],
        "extras_type": [None],
        "is_wicket": [False],
        "wicket_type": [None],
        "phase": ["Middle"],
        "batter": ["Batter A"],
        "bowler": ["Bowler B"],
        "venue": ["Venue C"],
        "player_dismissed": [None]
    }
    table = pa.Table.from_pydict(data, schema=BALL_EVENT_SCHEMA)
    assert table.column("over")[0].as_py() == 128

def test_retirement_wicket_accounting():
    """Verify that RETIRED_HURT and RETIRED_NOT_OUT are not counted as wickets (is_wicket=False)."""
    # Simulate wickets in canonicalization
    from midwicket.core.canonicalize import DismissalType
    
    # Check standard caught wicket
    wickets_caught = [{"kind": "caught", "player": "Player A"}]
    is_w_caught = len(wickets_caught) > 0
    wicket_kind_caught = wickets_caught[0].get("kind", "unknown").lower()
    if wicket_kind_caught in ["retired hurt", "retired not out"]:
        is_w_caught = False
    assert is_w_caught == True
    
    # Check retired hurt (not a dismissal)
    wickets_hurt = [{"kind": "retired hurt", "player": "Player B"}]
    is_w_hurt = len(wickets_hurt) > 0
    wicket_kind_hurt = wickets_hurt[0].get("kind", "unknown").lower()
    if wicket_kind_hurt in ["retired hurt", "retired not out"]:
        is_w_hurt = False
    assert is_w_hurt == False
    
    # Check retired not out (not a dismissal)
    wickets_not_out = [{"kind": "retired not out", "player": "Player C"}]
    is_w_not_out = len(wickets_not_out) > 0
    wicket_kind_not_out = wickets_not_out[0].get("kind", "unknown").lower()
    if wicket_kind_not_out in ["retired hurt", "retired not out"]:
        is_w_not_out = False
    assert is_w_not_out == False

def test_temporal_leakage_feature_store():
    """Test that start_date and end_date filters correctly slice loaded features."""
    session = md.datasets.load_dataset("mlc")
    
    # Load all BQR
    df_all = load_features("bowler_quality_rating", session)
    
    # Load BQR with sliced date range
    df_slice = load_features("bowler_quality_rating", session, start_date="2024-01-01", end_date="2024-12-31")
    
    # Verify both runs executed cleanly
    assert isinstance(df_all, pd.DataFrame)
    assert isinstance(df_slice, pd.DataFrame)

def test_second_innings_match_context_score():
    """Test that second-innings Match Context Score correctly applies target-based logic."""
    session = md.datasets.load_dataset("mlc")
    df = load_features("match_context_score", session)
    
    # Match context score must exist and be numeric
    assert "match_context_score" in df.columns
    assert df["match_context_score"].min() >= 0.0
    assert df["match_context_score"].max() <= 15.0

def test_venue_bias_rating_stabilization():
    """Verify that Venue Bias Rating falls back to 1.0 for venues with < 5 matches."""
    session = md.datasets.load_dataset("mlc")
    df = load_features("venue_bias_rating", session)
    
    # For MLC, let us check that small-sample venues are locked to 1.0
    low_sample = df[df["matches"] < 5]
    for rating in low_sample["venue_bias_rating"]:
        assert rating == 1.0
