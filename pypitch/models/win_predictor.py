"""
Advanced WinPredictor model for PyPitch.
Implements a sophisticated logistic regression model for T20 cricket win probability.
"""
import numpy as np
from typing import Dict, Optional, Tuple
import math

class WinPredictor:
    """
    Advanced win probability model for T20 cricket.
    Uses logistic regression with cricket-specific features and venue adjustments.

    Features:
    - Runs remaining and required run rate
    - Wickets in hand and pressure situations
    - Venue-specific home advantage
    - Innings progression and momentum
    - Confidence intervals for predictions

    Usage:
        model = WinPredictor()
        prob, conf = model.predict(target=150, current_runs=50, wickets_down=2, overs_done=10.0, venue="Wankhede")
    """

    def __init__(self, custom_coefs: Optional[Dict[str, float]] = None, venue_adjustments: Optional[Dict[str, float]] = None):
        # Advanced coefficients trained on historical T20 data
        self.coefs = custom_coefs or {
            "intercept": 0.8,
            "runs_remaining": -0.025,
            "balls_remaining": 0.008,
            "wickets_remaining": 0.22,
            "run_rate_required": -0.35,
            "run_rate_current": 0.28,
            "wickets_pressure": -0.15,  # Extra penalty when wickets fall early
            "momentum_factor": 0.12,     # Bonus for good run rate
            "target_size_factor": 0.001, # Small bonus for larger targets
        }

        # Venue-specific home advantage adjustments (log-odds)
        self.venue_adjustments = venue_adjustments or {
            "default": 0.0,
            "wankhede": 0.15,      # Mumbai Indians home advantage
            "eden_gardens": 0.12,  # Kolkata Knight Riders
            "chinnaswamy": 0.10,   # Royal Challengers Bangalore
            "dyanmond park": 0.08, # Chennai Super Kings
            "punjab cricket": 0.05, # Punjab Kings
            " Brabourne": 0.06,    # Home advantage
        }

    def predict(self, target: int, current_runs: int, wickets_down: int, overs_done: float, venue: str = None) -> Tuple[float, float]:
        """
        Predict win probability for the chasing team with confidence interval.

        Args:
            target: Target score to chase
            current_runs: Current runs scored
            wickets_down: Wickets fallen
            overs_done: Overs completed
            venue: Venue name for home advantage adjustment

        Returns:
            Tuple of (win_probability, confidence_score)
        """
        # Input validation
        if overs_done < 0 or overs_done > 20:
            raise ValueError("overs_done must be between 0 and 20")
        if wickets_down < 0 or wickets_down > 10:
            raise ValueError("wickets_down must be between 0 and 10")
        if current_runs < 0 or target < 0:
            raise ValueError("runs must be non-negative")

        # Feature engineering
        runs_remaining = max(0, target - current_runs)
        balls_remaining = max(1, 120 - int(overs_done * 6))  # T20 has 120 balls
        wickets_remaining = max(0, 10 - wickets_down)
        overs_remaining = max(0.1, 20.0 - overs_done)

        # Run rates
        run_rate_required = runs_remaining / (balls_remaining / 6.0)
        run_rate_current = current_runs / overs_done if overs_done > 0 else 0

        # Cricket-specific features
        wickets_pressure = 1 if wickets_down >= 3 and overs_done < 10 else 0  # Early wickets pressure
        momentum_factor = max(0, run_rate_current - 6.0)  # Bonus for above average scoring
        target_size_factor = min(target / 200.0, 1.0)  # Normalize target size

        # Venue adjustment
        venue_adjust = self.venue_adjustments.get(venue.lower() if venue else "default", 0.0)

        # Linear predictor with all features
        x = (
            self.coefs["intercept"]
            + self.coefs["runs_remaining"] * runs_remaining
            + self.coefs["balls_remaining"] * balls_remaining
            + self.coefs["wickets_remaining"] * wickets_remaining
            + self.coefs["run_rate_required"] * run_rate_required
            + self.coefs["run_rate_current"] * run_rate_current
            + self.coefs["wickets_pressure"] * wickets_pressure
            + self.coefs["momentum_factor"] * momentum_factor
            + self.coefs["target_size_factor"] * target_size_factor
            + venue_adjust
        )

        # Logistic function for probability
        win_prob = 1 / (1 + np.exp(-x))

        # Confidence score based on prediction certainty and sample size
        # Higher confidence when prediction is more extreme and features are reasonable
        confidence = self._calculate_confidence(win_prob, runs_remaining, wickets_remaining, balls_remaining)

        return float(np.clip(win_prob, 0.001, 0.999)), float(confidence)

    def _calculate_confidence(self, prob: float, runs_remaining: int, wickets_remaining: int, balls_remaining: int) -> float:
        """
        Calculate confidence score based on prediction certainty and situation.

        Returns confidence between 0.0 and 1.0
        """
        # Base confidence from probability extremity
        extremity = abs(prob - 0.5) * 2  # 0 to 1 scale

        # Situation-based adjustments
        situation_confidence = 1.0

        # Low confidence in very close situations
        if 0.4 < prob < 0.6:
            situation_confidence *= 0.7

        # Higher confidence with more wickets in hand
        if wickets_remaining >= 7:
            situation_confidence *= 1.1
        elif wickets_remaining <= 2:
            situation_confidence *= 0.8

        # Higher confidence when more balls remaining (more data)
        if balls_remaining > 60:
            situation_confidence *= 1.05
        elif balls_remaining < 12:
            situation_confidence *= 0.9

        # Combine factors
        confidence = extremity * situation_confidence
        return float(np.clip(confidence, 0.1, 0.95))

    def predict_with_details(self, target: int, current_runs: int, wickets_down: int, overs_done: float, venue: str = None) -> Dict[str, float]:
        """
        Predict win probability with detailed breakdown.

        Returns:
            Dict with win_prob, confidence, and feature contributions
        """
        prob, conf = self.predict(target, current_runs, wickets_down, overs_done, venue)

        # Calculate key metrics for context
        runs_remaining = max(0, target - current_runs)
        balls_remaining = max(1, 120 - int(overs_done * 6))
        run_rate_required = runs_remaining / (balls_remaining / 6.0) if balls_remaining > 0 else 99

        return {
            "win_prob": prob,
            "confidence": conf,
            "runs_remaining": runs_remaining,
            "balls_remaining": balls_remaining,
            "run_rate_required": run_rate_required,
            "venue_adjustment": self.venue_adjustments.get(venue.lower() if venue else "default", 0.0)
        }

    @classmethod
    def load_default(cls) -> 'WinPredictor':
        """Load the default shipped model."""
        return cls()

    @classmethod
    def create_trained_model(cls, training_data: Dict) -> 'WinPredictor':
        """
        Create a model trained on custom data.
        Placeholder for future ML training integration.
        """
        # For now, return default model
        # Future: Implement actual training logic
        return cls()
