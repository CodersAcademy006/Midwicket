"""
Baseline WinPredictor model for PyPitch.
Provides a default logistic regression model for win probability.
"""
import numpy as np
from typing import Dict, Optional

class WinPredictor:
    """
    Baseline win probability model (logistic regression).
    Ships with PyPitch as a default, pre-trained model.
    Can be swapped with custom models.

    Usage:
        model = WinPredictor()
        prob = model.predict(target=150, current_runs=50, wickets_down=2, overs_done=10.0)
    """
    def __init__(self, custom_coefs: Optional[Dict[str, float]] = None):
        # Baseline coefficients (pre-trained on historical T20 data simulation)
        self.coefs = custom_coefs or {
            "intercept": 0.5,
            "runs_remaining": -0.015,
            "balls_remaining": 0.012,
            "wickets_remaining": 0.18,
            "run_rate_required": -0.22,
            "run_rate_current": 0.19
        }

    def predict(self, target: int, current_runs: int, wickets_down: int, overs_done: float, venue: str = None) -> float:
        """
        Predict win probability for the chasing team.

        Args:
            target: Target score to chase
            current_runs: Current runs scored
            wickets_down: Wickets fallen
            overs_done: Overs completed
            venue: Optional venue (not used in baseline)

        Returns:
            Win probability (0.0 to 1.0)
        """
        # Feature engineering
        runs_remaining = target - current_runs
        balls_remaining = 120 - int(overs_done * 6)  # Assuming 120 balls in T20
        wickets_remaining = 10 - wickets_down
        run_rate_required = runs_remaining / (balls_remaining / 6) if balls_remaining > 0 else 99
        run_rate_current = current_runs / overs_done if overs_done > 0 else 0

        # Linear predictor
        x = (
            self.coefs["intercept"]
            + self.coefs["runs_remaining"] * runs_remaining
            + self.coefs["balls_remaining"] * balls_remaining
            + self.coefs["wickets_remaining"] * wickets_remaining
            + self.coefs["run_rate_required"] * run_rate_required
            + self.coefs["run_rate_current"] * run_rate_current
        )
        # Logistic function
        win_prob = 1 / (1 + np.exp(-x))
        return float(np.clip(win_prob, 0, 1))

    @classmethod
    def load_default(cls) -> 'WinPredictor':
        """Load the default shipped model."""
        return cls()
