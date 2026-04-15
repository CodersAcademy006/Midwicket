"""
Advanced WinPredictor model for PyPitch.
Implements a sophisticated logistic regression model for T20 cricket win probability.
"""
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
import math
import pandas as pd
import json
from importlib import resources

from .win_features import compute_chase_features, FEATURE_COLUMNS

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
            "venue_adjustment": 1.0,
            "rr_gap": 0.0,
            "required_boundary_rate": 0.0,
            "runs_per_wicket_remaining": 0.0,
            "wickets_per_over_remaining": 0.0,
            "chase_progress": 0.0,
            "death_overs": 0.0,
        }

        # Venue-specific home advantage adjustments (log-odds) — all keys lowercase
        self.venue_adjustments = venue_adjustments or {
            "default": 0.0,
            "wankhede": 0.15,      # Mumbai Indians home advantage
            "eden_gardens": 0.12,  # Kolkata Knight Riders
            "chinnaswamy": 0.10,   # Royal Challengers Bangalore
            "dyanmond park": 0.08, # Chennai Super Kings
            "punjab cricket": 0.05, # Punjab Kings
            "brabourne": 0.06,     # Home advantage
        }

        # Populated by create_trained_model / WinProbabilityTrainer.create_win_predictor
        self.training_metadata: Optional[Dict] = None

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


        venue_key = self._normalize_venue(venue)
        venue_adjust = self.venue_adjustments.get(venue_key, 0.0)
        feature_values = compute_chase_features(
            target=target,
            current_runs=current_runs,
            wickets_down=wickets_down,
            overs_done=overs_done,
            venue_adjustment=venue_adjust,
        )
        runs_remaining = feature_values["runs_remaining"]
        wickets_remaining = feature_values["wickets_remaining"]
        balls_remaining = feature_values["balls_remaining"]

        linear_terms = [
            feature for feature in feature_values.keys() if feature != "venue_adjustment"
        ]
        scaler_lookup = self._get_scaler_lookup()

        # Linear predictor with all features
        x = self.coefs.get("intercept", 0.0)
        for feature in linear_terms:
            feature_value = feature_values[feature]
            if scaler_lookup and feature in scaler_lookup:
                mean, scale = scaler_lookup[feature]
                feature_value = (feature_value - mean) / scale
            x += self.coefs.get(feature, 0.0) * feature_value

        # Keep legacy venue adjustment as an additive prior when no trained
        # venue coefficient exists.
        if "venue_adjustment" in self.coefs:
            x += self.coefs.get("venue_adjustment", 0.0) * feature_values["venue_adjustment"]
        else:
            x += venue_adjust

        # Logistic function for probability
        win_prob = 1 / (1 + np.exp(-x))

        # Confidence score based on prediction certainty and sample size
        # Higher confidence when prediction is more extreme and features are reasonable
        confidence = self._calculate_confidence(win_prob, runs_remaining, wickets_remaining, balls_remaining)

        return float(np.clip(win_prob, 0.001, 0.999)), float(confidence)

    def _normalize_venue(self, venue: Optional[str]) -> str:
        """Normalize venue names so aliases collapse to a stable lookup key."""
        if not venue:
            return "default"

        venue_key = venue.strip().lower()
        replacements = {
            " stadium": "",
            " cricket ground": "",
            " ground": "",
            " park": "",
            " arena": "",
            " international": "",
            " cr stadium": "",
        }
        for old, new in replacements.items():
            venue_key = venue_key.replace(old, new)

        venue_key = venue_key.replace("&", "and")
        venue_key = venue_key.replace(".", "")
        venue_key = venue_key.replace("-", " ")
        venue_key = "_".join(part for part in venue_key.split() if part)

        aliases = {
            "brabourne_stadium": "brabourne",
            "wankhede_stadium": "wankhede",
            "eden_gardens_stadium": "eden_gardens",
            "m_chinnaswamy_stadium": "chinnaswamy",
            "dy_patil_stadium": "default",
        }
        return aliases.get(venue_key, venue_key)

    def _get_venue_adjustment(self, venue: Optional[str]) -> float:
        """Venue lookup: exact match first, then substring, else default 0.0."""
        venue_key = self._normalize_venue(venue)
        if venue_key in self.venue_adjustments:
            return self.venue_adjustments[venue_key]
        if venue:
            raw_key = venue.lower()
            for key, val in self.venue_adjustments.items():
                if key != "default" and key in raw_key:
                    return val
        return self.venue_adjustments.get("default", 0.0)

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

    def _get_scaler_lookup(self) -> Optional[Dict[str, Tuple[float, float]]]:
        """Return feature -> (mean, scale) from training metadata when available."""
        if not self.training_metadata:
            return None

        means = self.training_metadata.get("scaler_mean")
        scales = self.training_metadata.get("scaler_scale")
        if not isinstance(means, list) or not isinstance(scales, list):
            return None
        if len(means) != len(scales) or len(means) != len(FEATURE_COLUMNS):
            return None

        lookup: Dict[str, Tuple[float, float]] = {}
        try:
            for feature, mean, scale in zip(FEATURE_COLUMNS, means, scales):
                m = float(mean)
                s = float(scale)
                if s == 0.0:
                    s = 1.0
                lookup[feature] = (m, s)
        except (TypeError, ValueError):
            return None

        return lookup

    def predict_with_details(self, target: int, current_runs: int, wickets_down: int, overs_done: float, venue: str = None) -> Dict[str, float]:
        """
        Predict win probability with detailed breakdown.

        Returns:
            Dict with win_prob, confidence, and feature contributions
        """
        prob, conf = self.predict(target, current_runs, wickets_down, overs_done, venue)

        # Calculate key metrics for context
        feature_values = compute_chase_features(
            target=target,
            current_runs=current_runs,
            wickets_down=wickets_down,
            overs_done=overs_done,
            venue_adjustment=self.venue_adjustments.get(self._normalize_venue(venue), 0.0),
        )

        return {
            "win_prob": prob,
            "confidence": conf,
            "runs_remaining": feature_values["runs_remaining"],
            "balls_remaining": feature_values["balls_remaining"],
            "run_rate_required": feature_values["run_rate_required"],
            "venue_adjustment": self.venue_adjustments.get(self._normalize_venue(venue), 0.0)
        }

    @classmethod
    def load_default(cls) -> 'WinPredictor':
        """Load the default shipped model."""
        try:
            bundled = resources.files("pypitch.models.data").joinpath("win_model_default.json")
            with bundled.open("r", encoding="utf-8") as f:
                payload = json.load(f)

            coefs_raw = payload.get("coefs")
            venue_raw = payload.get("venue_adjustments")
            metadata_raw = payload.get("training_metadata")
            if not isinstance(coefs_raw, dict) or not isinstance(venue_raw, dict):
                raise ValueError("Bundled model payload is missing coefs/venue_adjustments")

            coefs = {str(k): float(v) for k, v in coefs_raw.items()}
            venue_adjustments = {str(k): float(v) for k, v in venue_raw.items()}

            predictor = cls(custom_coefs=coefs, venue_adjustments=venue_adjustments)
            if isinstance(metadata_raw, dict):
                predictor.training_metadata = metadata_raw
            else:
                predictor.training_metadata = {}
            predictor.training_metadata.setdefault("source", "bundled")
            return predictor
        except Exception:
            # Fallback keeps runtime safe even if package data is unavailable.
            return cls()

    @classmethod
    def create_trained_model(cls, training_data: Union[pd.DataFrame, List[Dict]]) -> 'WinPredictor':
        """
        Create a model trained on custom data.

        Args:
            training_data: DataFrame or list of row dicts with columns:
                match_id, inning, over, ball, runs_total, wickets_fallen, target, venue

        Returns:
            WinPredictor with training_metadata set
        """
        from pypitch.models.train import WinProbabilityTrainer

        if isinstance(training_data, list):
            df = pd.DataFrame(training_data)
        else:
            df = training_data

        trainer = WinProbabilityTrainer()
        features, target = trainer.prepare_training_data(df)

        # Build match_ids list aligned with the feature rows (second innings only).
        # prepare_training_data may skip malformed rows; truncate to match features length.
        second_innings = df[df['inning'] == 2]
        raw_ids: List[str] = [str(mid) for mid in second_innings['match_id']]
        match_ids: List[str] = raw_ids[:len(features)]

        model, metrics = trainer.train_model(features, target, match_ids=match_ids)
        predictor = trainer.create_win_predictor(model, metrics)
        return predictor
