"""
PyPitch ML Training Pipeline

Handles training, versioning, and persistence of machine learning models.
Supports win probability models and other predictive analytics.
"""

import os
import pickle
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
import logging

from ..exceptions import ModelTrainingError, ModelNotFoundError

logger = logging.getLogger(__name__)

class ModelRegistry:
    """
    Registry for managing ML model versions and persistence.

    Stores models in a structured directory with metadata.
    """

    def __init__(self, base_path: str = None):
        if base_path is None:
            # Default to user's home directory
            base_path = os.path.join(os.path.expanduser("~"), ".pypitch", "models")

        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._models: Dict[str, Dict[str, Any]] = {}

        # Load existing models on initialization
        self._load_registry()

    def _load_registry(self):
        """Load model metadata from disk."""
        registry_file = self.base_path / "registry.pkl"
        if registry_file.exists():
            try:
                with open(registry_file, 'rb') as f:
                    self._models = pickle.load(f)
            except Exception as e:
                logger.warning(f"Failed to load model registry: {e}")
                self._models = {}

    def _save_registry(self):
        """Save model metadata to disk."""
        registry_file = self.base_path / "registry.pkl"
        try:
            with open(registry_file, 'wb') as f:
                pickle.dump(self._models, f)
        except Exception as e:
            logger.error(f"Failed to save model registry: {e}")

    def register_model(self, name: str, model: Any, metadata: Dict[str, Any] = None) -> str:
        """
        Register a new model version.

        Args:
            name: Model name (e.g., 'win_predictor')
            model: Trained model object
            metadata: Additional metadata (accuracy, training_date, etc.)

        Returns:
            Version string for the registered model
        """
        if metadata is None:
            metadata = {}

        # Generate version string
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        version = f"{name}_v_{timestamp}"

        # Save model to disk
        model_path = self.base_path / f"{version}.pkl"
        try:
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
        except Exception as e:
            raise ModelTrainingError(f"Failed to save model {version}: {e}")

        # Update registry
        self._models[name] = {
            'current_version': version,
            'versions': self._models.get(name, {}).get('versions', []) + [version],
            'metadata': metadata,
            'created_at': datetime.now().isoformat()
        }

        self._save_registry()
        logger.info(f"Registered model: {version}")
        return version

    def get_model(self, name: str, version: str = None) -> Any:
        """
        Retrieve a model by name and optional version.

        Args:
            name: Model name
            version: Specific version, or None for latest

        Returns:
            Loaded model object
        """
        if name not in self._models:
            raise ModelNotFoundError(f"Model '{name}' not found")

        if version is None:
            version = self._models[name]['current_version']

        if version not in self._models[name]['versions']:
            raise ModelNotFoundError(f"Version '{version}' not found for model '{name}'")

        model_path = self.base_path / f"{version}.pkl"
        if not model_path.exists():
            raise ModelNotFoundError(f"Model file not found: {model_path}")

        try:
            with open(model_path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            raise ModelTrainingError(f"Failed to load model {version}: {e}")

    def list_models(self) -> List[str]:
        """List all registered model names."""
        return list(self._models.keys())

    def list_versions(self, name: str) -> List[str]:
        """List all versions for a model."""
        if name not in self._models:
            return []
        return self._models[name]['versions']

    def get_metadata(self, name: str, version: str = None) -> Dict[str, Any]:
        """Get metadata for a model version."""
        if name not in self._models:
            raise ModelNotFoundError(f"Model '{name}' not found")

        return self._models[name]['metadata']

    def delete_model(self, name: str, version: str = None):
        """Delete a model version or entire model."""
        if name not in self._models:
            raise ModelNotFoundError(f"Model '{name}' not found")

        if version is None:
            # Delete all versions
            for v in self._models[name]['versions']:
                model_path = self.base_path / f"{v}.pkl"
                if model_path.exists():
                    model_path.unlink()

            del self._models[name]
        else:
            # Delete specific version
            if version not in self._models[name]['versions']:
                raise ModelNotFoundError(f"Version '{version}' not found")

            model_path = self.base_path / f"{version}.pkl"
            if model_path.exists():
                model_path.unlink()

            self._models[name]['versions'].remove(version)
            if self._models[name]['current_version'] == version:
                # Set current to latest remaining version
                if self._models[name]['versions']:
                    self._models[name]['current_version'] = max(self._models[name]['versions'])
                else:
                    del self._models[name]

        self._save_registry()

# Global registry instance
_registry = ModelRegistry()

def get_model_registry() -> ModelRegistry:
    """Get the global model registry instance."""
    return _registry

__all__ = ['ModelRegistry', 'get_model_registry']