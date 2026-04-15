# pypitch/compute/winprob.py
"""
Robust Win Probability Model for T20 Cricket
Implements a logistic regression-based model using historical data and cricket domain logic.
"""
import os
import pickle
import threading
import numpy as np
from typing import Dict
from ..models.win_predictor import WinPredictor

def _load_initial_model() -> WinPredictor:
    """Load model on module init: path from env if PYPITCH_WIN_MODEL_MODE=path, else default.

    Path mode is disabled when PYPITCH_ENV=production to prevent arbitrary
    pickle deserialization in production deployments.  Use the default shipped
    model or rebuild from source data instead.
    """
    mode = os.environ.get("PYPITCH_WIN_MODEL_MODE", "default").lower()
    if mode == "path":
        env = os.environ.get("PYPITCH_ENV", "development").lower()
        if env == "production":
            import warnings
            warnings.warn(
                "PYPITCH_WIN_MODEL_MODE=path is disabled in production "
                "(PYPITCH_ENV=production). Falling back to the default model. "
                "Rebuild the model from source data to deploy a custom model.",
                RuntimeWarning,
                stacklevel=2,
            )
        else:
            model_path = os.environ.get("PYPITCH_WIN_MODEL_PATH", "")
            if model_path:
                expected_sha256 = os.environ.get("PYPITCH_WIN_MODEL_SHA256", "")
                if not expected_sha256:
                    raise RuntimeError(
                        "PYPITCH_WIN_MODEL_SHA256 must be set when using "
                        "PYPITCH_WIN_MODEL_PATH (required to verify model integrity)."
                    )
                import hashlib
                with open(model_path, "rb") as f:
                    raw = f.read()
                actual = hashlib.sha256(raw).hexdigest()
                if actual != expected_sha256.lower():
                    raise RuntimeError(
                        f"Model file SHA-256 mismatch. "
                        f"Expected {expected_sha256.lower()}, got {actual}. "
                        "Do not load untrusted model files."
                    )
                import io
                return pickle.load(io.BytesIO(raw))  # nosec B301 — SHA-256 verified above
    return WinPredictor.load_default()

# Global default model instance — protected by lock (M3)
_model_lock = threading.Lock()
_default_model = _load_initial_model()

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
