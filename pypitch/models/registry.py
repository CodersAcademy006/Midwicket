"""
PyPitch ML Model Registry

Handles versioning and persistence of machine learning models.
Supports win probability models and other predictive analytics.

Security note:
  - Model metadata is stored as JSON (no deserialization risk).
  - Model weights are stored as joblib files.  joblib uses pickle under the
    hood for sklearn objects, but files are only ever read from the controlled
    ``base_path`` directory written by this class — never from user input.
  - Never load model files from untrusted or user-supplied paths.
"""

import json
import os
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

    def __init__(self, base_path: Optional[str] = None) -> None:
        if base_path is None:
            base_path = os.path.join(os.path.expanduser("~"), ".pypitch", "models")

        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._models: Dict[str, Dict[str, Any]] = {}

        self._load_registry()

    # ── Registry metadata (JSON, no deserialization risk) ─────────────────────

    def _load_registry(self) -> None:
        """Load model metadata from disk (JSON format)."""
        registry_file = self.base_path / "registry.json"
        # Legacy: migrate registry.pkl → registry.json on first load
        legacy_file = self.base_path / "registry.pkl"

        if registry_file.exists():
            try:
                with open(registry_file, encoding="utf-8") as f:
                    self._models = json.load(f)
            except Exception as exc:
                logger.warning("Failed to load model registry from JSON: %s", exc)
                self._models = {}
        elif legacy_file.exists():
            logger.warning(
                "Legacy registry.pkl found at %s — migrating to registry.json. "
                "The old .pkl file will not be removed automatically.",
                legacy_file,
            )
            # We cannot safely load the pkl without pickle, so start fresh
            # and let the operator re-register models if needed.
            self._models = {}
            self._save_registry()

    def _save_registry(self) -> None:
        """Save model metadata to disk (JSON format)."""
        registry_file = self.base_path / "registry.json"
        try:
            with open(registry_file, "w", encoding="utf-8") as f:
                json.dump(self._models, f, indent=2, default=str)
        except Exception as exc:
            logger.error("Failed to save model registry: %s", exc)

    # ── Model weight persistence (joblib) ─────────────────────────────────────

    @staticmethod
    def _get_joblib():
        """Return the joblib module, raising ImportError if missing."""
        try:
            import joblib  # type: ignore[import]
            return joblib
        except ImportError:
            raise ImportError(
                "joblib is required for model persistence. "
                "Install it with: pip install joblib"
            )

    def _model_path(self, version: str) -> Path:
        """Return the absolute path for a model file, rejecting path traversal."""
        safe_name = Path(version).name  # strip any directory components
        if safe_name != version:
            raise ValueError(f"Invalid model version name: {version!r}")
        return self.base_path / f"{safe_name}.joblib"

    # ── Public API ────────────────────────────────────────────────────────────

    def register_model(
        self,
        name: str,
        model: Any,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register a new model version.

        Args:
            name: Model name (e.g., 'win_predictor').
            model: Trained model object (sklearn-compatible).
            metadata: Optional dict with accuracy, training_date, etc.

        Returns:
            Version string for the registered model.
        """
        if metadata is None:
            metadata = {}

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        version = f"{name}_v_{timestamp}"
        model_path = self._model_path(version)

        joblib = self._get_joblib()
        try:
            joblib.dump(model, model_path)
        except Exception as exc:
            raise ModelTrainingError(f"Failed to save model {version}: {exc}") from exc

        self._models[name] = {
            "current_version": version,
            "versions": self._models.get(name, {}).get("versions", []) + [version],
            "metadata": metadata,
            "created_at": datetime.now().isoformat(),
        }
        self._save_registry()
        logger.info("Registered model: %s", version)
        return version

    def get_model(self, name: str, version: Optional[str] = None) -> Any:
        """Retrieve a model by name and optional version.

        Model files are only ever loaded from the controlled ``base_path``
        directory; path traversal in version names is rejected.
        """
        if name not in self._models:
            raise ModelNotFoundError(f"Model '{name}' not found")

        if version is None:
            version = self._models[name]["current_version"]

        if version not in self._models[name]["versions"]:
            raise ModelNotFoundError(f"Version '{version}' not found for model '{name}'")

        model_path = self._model_path(version)
        if not model_path.exists():
            raise ModelNotFoundError(f"Model file not found: {model_path}")

        joblib = self._get_joblib()
        try:
            return joblib.load(model_path)
        except Exception as exc:
            raise ModelTrainingError(f"Failed to load model {version}: {exc}") from exc

    def list_models(self) -> List[str]:
        """List all registered model names."""
        return list(self._models.keys())

    def list_versions(self, name: str) -> List[str]:
        """List all versions for a model."""
        if name not in self._models:
            return []
        return self._models[name]["versions"]

    def get_metadata(self, name: str, version: Optional[str] = None) -> Dict[str, Any]:
        """Get metadata for a model."""
        if name not in self._models:
            raise ModelNotFoundError(f"Model '{name}' not found")
        return self._models[name]["metadata"]

    def delete_model(self, name: str, version: Optional[str] = None) -> None:
        """Delete a model version or entire model."""
        if name not in self._models:
            raise ModelNotFoundError(f"Model '{name}' not found")

        if version is None:
            for v in self._models[name]["versions"]:
                p = self._model_path(v)
                if p.exists():
                    p.unlink()
            del self._models[name]
        else:
            if version not in self._models[name]["versions"]:
                raise ModelNotFoundError(f"Version '{version}' not found")
            p = self._model_path(version)
            if p.exists():
                p.unlink()
            self._models[name]["versions"].remove(version)
            if self._models[name].get("current_version") == version:
                remaining = self._models[name]["versions"]
                if remaining:
                    self._models[name]["current_version"] = max(remaining)
                else:
                    del self._models[name]

        self._save_registry()


# ── Module-level singleton ────────────────────────────────────────────────────

_registry: Optional[ModelRegistry] = None


def get_model_registry() -> ModelRegistry:
    """Get the global model registry instance (lazy-initialized)."""
    global _registry
    if _registry is None:
        _registry = ModelRegistry()
    return _registry


__all__ = ["ModelRegistry", "get_model_registry"]
