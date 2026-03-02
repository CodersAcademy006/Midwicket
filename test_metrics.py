"""
Test pypitch metric calculation functions.
"""
import pytest
import pyarrow as pa
from pypitch.compute.metrics.batting import (
    calculate_strike_rate,
    calculate_batting_average,
    calculate_boundary_percentage,
)
from pypitch.compute.metrics.bowling import (
    calculate_economy_rate,
    calculate_bowling_average,
    calculate_dot_ball_percentage,
)

def test_strike_rate():
    """Test strike rate calculation."""
    runs = pa.array([50, 30, 0])
    balls = pa.array([40, 30, 5])
    
    sr = calculate_strike_rate(runs, balls)
    
    assert sr[0].as_py() == pytest.approx(125.0)
    assert sr[1].as_py() == pytest.approx(100.0)
    assert sr[2].as_py() == pytest.approx(0.0)

def test_batting_average():
    """Test batting average calculation."""
    runs = pa.array([500, 300, 0])
    dismissals = pa.array([10, 10, 0])
    
    avg = calculate_batting_average(runs, dismissals)
    
    assert avg[0].as_py() == pytest.approx(50.0)
    assert avg[1].as_py() == pytest.approx(30.0)
    assert avg[2].is_valid == False  # Division by zero

def test_boundary_percentage():
    """Test boundary percentage calculation."""
    boundaries = pa.array([20, 10, 0])
    balls_faced = pa.array([100, 50, 30])
    
    bp = calculate_boundary_percentage(boundaries, balls_faced)
    
    assert bp[0].as_py() == pytest.approx(20.0)
    assert bp[1].as_py() == pytest.approx(20.0)
    assert bp[2].as_py() == pytest.approx(0.0)

def test_economy_rate():
    """Test economy rate calculation."""
    runs = pa.array([30, 24, 0])
    overs = pa.array([4.0, 4.0, 2.0])
    
    er = calculate_economy_rate(runs, overs)
    
    assert er[0].as_py() == pytest.approx(7.5)
    assert er[1].as_py() == pytest.approx(6.0)
    assert er[2].as_py() == pytest.approx(0.0)

def test_bowling_average():
    """Test bowling average calculation."""
    runs = pa.array([100, 50, 0])
    wickets = pa.array([5, 2, 0])
    
    avg = calculate_bowling_average(runs, wickets)
    
    assert avg[0].as_py() == pytest.approx(20.0)
    assert avg[1].as_py() == pytest.approx(25.0)
    assert avg[2].is_valid == False  # Division by zero

def test_dot_ball_percentage():
    """Test dot ball percentage calculation."""
    dots = pa.array([30, 20, 0])
    balls = pa.array([60, 60, 30])
    
    dbp = calculate_dot_ball_percentage(dots, balls)
    
    assert dbp[0].as_py() == pytest.approx(50.0)
    assert dbp[1].as_py() == pytest.approx(33.333, abs=0.01)
    assert dbp[2].as_py() == pytest.approx(0.0)

# Commented out tests for unimplemented functions
# def test_calculate_impact_score():
#     """Test impact score calculation."""
#     pass

# def test_calculate_consistency_score():
#     """Test consistency score calculation."""
#     pass

# def test_calculate_partnership_metrics():
#     """Test partnership metrics calculation."""
#     pass

# def test_calculate_team_metrics():
#     """Test team metrics calculation."""
#     pass

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
