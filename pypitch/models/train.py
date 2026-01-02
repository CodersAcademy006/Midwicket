"""
PyPitch ML Training Module

Provides training capabilities for win probability and other predictive models.
Supports data preparation, model training, validation, and deployment.
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler
from typing import Dict, Any, Optional, Tuple, List
import logging
from datetime import datetime

from .win_predictor import WinPredictor
from .registry import get_model_registry
from ..exceptions import ModelTrainingError, DataValidationError

logger = logging.getLogger(__name__)

class WinProbabilityTrainer:
    """
    Trainer for win probability models.

    Handles data preparation, feature engineering, model training,
    and validation for cricket win probability prediction.
    """

    def __init__(self):
        self.scaler = StandardScaler()
        self.feature_columns = [
            'runs_remaining', 'balls_remaining', 'wickets_remaining',
            'run_rate_required', 'run_rate_current', 'wickets_pressure',
            'momentum_factor', 'target_size_factor', 'venue_adjustment'
        ]

    def prepare_training_data(self, match_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepare cricket match data for training.

        Args:
            match_data: DataFrame with match delivery data

        Returns:
            Tuple of (features_df, target_series)
        """
        if match_data.empty:
            raise DataValidationError("Training data cannot be empty")

        required_columns = ['match_id', 'inning', 'over', 'ball', 'runs_total', 'wickets_fallen', 'target', 'venue']
        missing_cols = [col for col in required_columns if col not in match_data.columns]
        if missing_cols:
            raise DataValidationError(f"Missing required columns: {missing_cols}")

        # Filter to second innings only (chasing team)
        second_innings = match_data[match_data['inning'] == 2].copy()

        if second_innings.empty:
            raise DataValidationError("No second innings data found for training")

        # Feature engineering
        features = []

        for _, delivery in second_innings.iterrows():
            try:
                # Basic features
                runs_remaining = max(0, delivery['target'] - delivery['runs_total'])
                balls_remaining = max(1, 120 - (delivery['over'] * 6 + delivery['ball']))
                wickets_remaining = max(0, 10 - delivery['wickets_fallen'])
                overs_done = delivery['over'] + delivery['ball'] / 6.0

                # Run rates
                run_rate_required = runs_remaining / (balls_remaining / 6.0) if balls_remaining > 0 else 99
                run_rate_current = delivery['runs_total'] / overs_done if overs_done > 0 else 0

                # Cricket-specific features
                wickets_pressure = 1 if delivery['wickets_fallen'] >= 3 and overs_done < 10 else 0
                momentum_factor = max(0, run_rate_current - 6.0)
                target_size_factor = min(delivery['target'] / 200.0, 1.0)

                # Venue adjustment (simplified - could be expanded)
                venue_adjustment = self._get_venue_adjustment(delivery.get('venue', ''))

                features.append({
                    'runs_remaining': runs_remaining,
                    'balls_remaining': balls_remaining,
                    'wickets_remaining': wickets_remaining,
                    'run_rate_required': run_rate_required,
                    'run_rate_current': run_rate_current,
                    'wickets_pressure': wickets_pressure,
                    'momentum_factor': momentum_factor,
                    'target_size_factor': target_size_factor,
                    'venue_adjustment': venue_adjustment
                })

            except Exception as e:
                logger.warning(f"Skipping delivery due to error: {e}")
                continue

        if not features:
            raise DataValidationError("No valid training samples generated")

        features_df = pd.DataFrame(features)

        # Target: whether the team eventually won
        # Group by match and get final result
        match_results = second_innings.groupby('match_id').agg({
            'runs_total': 'max',
            'target': 'first'
        }).reset_index()

        match_results['won'] = (match_results['runs_total'] >= match_results['target']).astype(int)

        # For each delivery, determine if the team won
        targets = []
        for _, delivery in second_innings.iterrows():
            match_result = match_results[match_results['match_id'] == delivery['match_id']]
            if not match_result.empty:
                targets.append(match_result['won'].iloc[0])
            else:
                targets.append(0)  # Default to loss if no result found

        target_series = pd.Series(targets, name='won')

        return features_df, target_series

    def _get_venue_adjustment(self, venue: str) -> float:
        """Get venue-specific adjustment factor."""
        venue_adjustments = {
            'wankhede': 0.15,
            'eden gardens': 0.12,
            'chinnaswamy': 0.10,
            'dyanmond park': 0.08,
            'punjab cricket': 0.05,
            'brabourne': 0.06
        }
        return venue_adjustments.get(venue.lower(), 0.0)

    def train_model(self, features: pd.DataFrame, target: pd.Series,
                   test_size: float = 0.2, random_state: int = 42) -> Tuple[LogisticRegression, Dict[str, Any]]:
        """
        Train a logistic regression model for win probability.

        Args:
            features: Feature DataFrame
            target: Target series
            test_size: Fraction of data for testing
            random_state: Random seed

        Returns:
            Tuple of (trained_model, training_metrics)
        """
        if len(features) != len(target):
            raise DataValidationError("Features and target must have same length")

        if len(features) < 100:
            raise DataValidationError("Insufficient training data (minimum 100 samples)")

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            features, target, test_size=test_size, random_state=random_state, stratify=target
        )

        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        # Train model
        model = LogisticRegression(random_state=random_state, max_iter=1000)
        model.fit(X_train_scaled, y_train)

        # Evaluate
        train_pred = model.predict_proba(X_train_scaled)[:, 1]
        test_pred = model.predict_proba(X_test_scaled)[:, 1]

        metrics = {
            'train_accuracy': accuracy_score(y_train, train_pred > 0.5),
            'test_accuracy': accuracy_score(y_test, test_pred > 0.5),
            'train_log_loss': log_loss(y_train, train_pred),
            'test_log_loss': log_loss(y_test, test_pred),
            'train_auc': roc_auc_score(y_train, train_pred),
            'test_auc': roc_auc_score(y_test, test_pred),
            'cross_val_scores': cross_val_score(model, X_train_scaled, y_train, cv=5).tolist(),
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'feature_importance': dict(zip(features.columns, model.coef_[0]))
        }

        logger.info(f"Model trained with {metrics['training_samples']} samples")
        logger.info(f"Test AUC: {metrics['test_auc']:.3f}, Accuracy: {metrics['test_accuracy']:.3f}")

        return model, metrics

    def create_win_predictor(self, trained_model: LogisticRegression,
                           training_metrics: Dict[str, Any]) -> WinPredictor:
        """
        Convert trained sklearn model to WinPredictor format.

        Args:
            trained_model: Trained LogisticRegression model
            training_metrics: Training metrics dictionary

        Returns:
            WinPredictor instance with trained coefficients
        """
        # Extract coefficients
        coefs = dict(zip(self.feature_columns, trained_model.coef_[0]))
        coefs['intercept'] = trained_model.intercept_[0]

        # Create venue adjustments (could be learned from data in future)
        venue_adjustments = {
            "default": 0.0,
            "wankhede": 0.15,
            "eden_gardens": 0.12,
            "chinnaswamy": 0.10,
            "dyanmond park": 0.08,
            "punjab cricket": 0.05,
            "brabourne": 0.06,
        }

        predictor = WinPredictor(custom_coefs=coefs, venue_adjustments=venue_adjustments)

        # Add training metadata
        predictor.training_metadata = {
            'trained_at': datetime.now().isoformat(),
            'metrics': training_metrics,
            'scaler_mean': self.scaler.mean_.tolist(),
            'scaler_scale': self.scaler.scale_.tolist()
        }

        return predictor

    def train_and_register(self, match_data: pd.DataFrame, model_name: str = "win_predictor") -> str:
        """
        Complete training pipeline: prepare data, train model, create predictor, register.

        Args:
            match_data: Raw match delivery data
            model_name: Name for the registered model

        Returns:
            Version string of registered model
        """
        logger.info("Starting win probability model training...")

        # Prepare data
        features, target = self.prepare_training_data(match_data)

        # Train model
        model, metrics = self.train_model(features, target)

        # Create predictor
        predictor = self.create_win_predictor(model, metrics)

        # Register model
        registry = get_model_registry()
        version = registry.register_model(
            name=model_name,
            model=predictor,
            metadata={
                'type': 'win_probability',
                'training_date': datetime.now().isoformat(),
                'metrics': metrics,
                'data_samples': len(features)
            }
        )

        logger.info(f"Successfully trained and registered model: {version}")
        return version

def retrain_win_probability_model(match_data: pd.DataFrame) -> WinPredictor:
    """
    Convenience function to retrain the win probability model.

    Args:
        match_data: DataFrame with match delivery data

    Returns:
        Trained WinPredictor instance
    """
    trainer = WinProbabilityTrainer()
    version = trainer.train_and_register(match_data)
    registry = get_model_registry()
    return registry.get_model("win_predictor", version)

__all__ = [
    'WinProbabilityTrainer',
    'retrain_win_probability_model'
]