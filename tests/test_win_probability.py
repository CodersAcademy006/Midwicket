"""
Test cases for baseline win probability model.
"""
import pytest
from pypitch.models.win_predictor import WinPredictor
from pypitch.compute.winprob import win_probability, set_win_model

def test_win_predictor_default():
    model = WinPredictor()
    prob, conf = model.predict(target=150, current_runs=50, wickets_down=2, overs_done=10.0)
    assert 0.0 <= prob <= 1.0
    assert 0.0 <= conf <= 1.0

def test_win_predictor_custom_coefs():
    custom_coefs = {"intercept": 0.0, "runs_remaining": -0.01, "balls_remaining": 0.01,
                    "wickets_remaining": 0.1, "run_rate_required": -0.1, "run_rate_current": 0.1,
                    "wickets_pressure": 0.0, "momentum_factor": 0.0, "target_size_factor": 0.0}
    model = WinPredictor(custom_coefs)
    prob, conf = model.predict(target=150, current_runs=50, wickets_down=2, overs_done=10.0)
    assert 0.0 <= prob <= 1.0
    assert 0.0 <= conf <= 1.0

def test_win_probability_function():
    result = win_probability(target=150, current_runs=50, wickets_down=2, overs_done=10.0)
    assert "win_prob" in result
    assert "confidence" in result
    assert 0.0 <= result["win_prob"] <= 1.0
    assert 0.0 <= result["confidence"] <= 1.0

def test_set_win_model():
    original_model = WinPredictor()
    custom_model = WinPredictor({"intercept": 1.0, "runs_remaining": 0.0, "balls_remaining": 0.0,
                                 "wickets_remaining": 0.0, "run_rate_required": 0.0, "run_rate_current": 0.0,
                                 "wickets_pressure": 0.0, "momentum_factor": 0.0, "target_size_factor": 0.0})
    set_win_model(custom_model)
    result = win_probability(target=150, current_runs=50, wickets_down=2, overs_done=10.0)
    # With intercept=1.0 and others 0, prob should be sigmoid(1.0) ≈ 0.731
    assert abs(result["win_prob"] - 0.731) < 0.01
    # Reset
    set_win_model(original_model)