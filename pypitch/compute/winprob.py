# pypitch/compute/winprob.py
"""
Robust Win Probability Model for T20 Cricket
Implements a logistic regression-based model using historical data and cricket domain logic.
"""
import threading
import numpy as np
from typing import Dict
from ..models.win_predictor import WinPredictor

# Global default model instance — protected by lock (M3)
_model_lock = threading.Lock()
_default_model = WinPredictor.load_default()

def win_probability(
    target: int,
    current_runs: int,
    wickets_down: int,
    overs_done: float,
    venue: str = None,
    balls_per_innings: int = 120,
    snapshot: str = "latest"
) -> Dict[str, float]:
    """
    Estimate win probability for the chasing team in a T20 match.
    Uses the default shipped WinPredictor model.

    Args:
        target: Target score to chase
        current_runs: Current runs scored
        wickets_down: Wickets fallen
        overs_done: Overs completed
        venue: Optional venue (not used in baseline)
        balls_per_innings: Total balls in innings (default 120 for T20)
        snapshot: Data snapshot (not used in baseline)

    Returns:
        Dict with 'win_prob' and 'confidence' keys
    """
    with _model_lock:
        model = _default_model
    prob, conf = model.predict(target, current_runs, wickets_down, overs_done, venue)
    return {"win_prob": prob, "confidence": conf}

def set_win_model(model: WinPredictor) -> None:
    """
    Swap the default win probability model with a custom one.

    Thread-safe: uses _model_lock so concurrent predict() calls see either
    the old or the new model, never a partially-written reference.

    Usage:
        from pypitch.models.win_predictor import WinPredictor
        custom_model = WinPredictor(custom_coefs={...})
        set_win_model(custom_model)
    """
    global _default_model
    with _model_lock:
        _default_model = model
