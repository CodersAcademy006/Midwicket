"""
Test query type definitions and validation.
"""
import pytest
from pypitch.query.defs import QueryType, Phase, Role

def test_query_type_enum():
    """Test QueryType enum values."""
    assert QueryType.BATTER_VS_BOWLER.value == "batter_vs_bowler"
    assert QueryType.VENUE_STATS.value == "venue_stats"
    assert QueryType.WIN_PREDICTION.value == "win_prediction"

def test_phase_type():
    """Test Phase literal type."""
    valid_phases = ["powerplay", "middle", "death", "all"]
    for phase in valid_phases:
        assert phase in ["powerplay", "middle", "death", "all"]

def test_role_type():
    """Test Role literal type."""
    valid_roles = ["batter", "bowler", "all-rounder"]
    for role in valid_roles:
        assert role in ["batter", "bowler", "all-rounder"]

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
