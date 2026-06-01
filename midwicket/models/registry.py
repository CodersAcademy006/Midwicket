"""
Midwicket ML Model Registry

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
import uuid
import threading

from ..exceptions import ModelTrainingError, ModelNotFoundError

logger = logging.getLogger(__name__)


class ModelRegistry:
    """
    Registry for managing ML model versions and persistence.

    Stores models in a structured directory with metadata.
    """

    def __init__(self, base_path: Optional[str] = None) -> None:
        if base_path is None:
            base_path = os.path.join(os.path.expanduser("~"), ".midwicket", "models")

        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._models: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

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
        version = f"{name}_v_{timestamp}_{uuid.uuid4().hex[:8]}"
        model_path = self._model_path(version)

        joblib = self._get_joblib()
        try:
            joblib.dump(model, model_path)
        except Exception as exc:
            raise ModelTrainingError(f"Failed to save model {version}: {exc}") from exc

        with self._lock:
            versions = self._models.get(name, {}).get("versions", [])
            if version not in versions:
                versions.append(version)
                
            self._models[name] = {
                "current_version": version,
                "versions": versions,
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
        with self._lock:
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

    # ── Semantic versioning extension ────────────────────────────────────────
    #
    # These methods layer a MAJOR.MINOR.PATCH version scheme on top of the
    # existing per-model entries. Versioned artifacts live under
    # ``base_path/versions/{model_name}/{version}.joblib`` so they cannot
    # collide with the legacy ``base_path/{version}.joblib`` files.
    #
    # Metadata for each model is persisted in
    # ``base_path/versions/{model_name}/metadata.json``:
    #
    #     {
    #         "active_version": "1.2.0",
    #         "versions": {
    #             "1.0.0": {"artifact": "1.0.0.joblib", "metadata": {...},
    #                       "registered_at": "..."},
    #             ...
    #         }
    #     }

    def _versions_dir(self, model_name: str) -> Path:
        safe = Path(model_name).name
        if safe != model_name or "/" in model_name or "\\" in model_name:
            raise ValueError(f"invalid model name: {model_name!r}")
        return self.base_path / "versions" / safe

    def _versions_metadata_path(self, model_name: str) -> Path:
        return self._versions_dir(model_name) / "metadata.json"

    def _load_versions_metadata(self, model_name: str) -> Dict[str, Any]:
        path = self._versions_metadata_path(model_name)
        if not path.exists():
            return {"active_version": None, "versions": {}}
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            if "versions" not in data:
                data["versions"] = {}
            if "active_version" not in data:
                data["active_version"] = None
            return data
        except Exception as exc:
            logger.warning(
                "Failed to load versions metadata for %s: %s — starting fresh",
                model_name, exc,
            )
            return {"active_version": None, "versions": {}}

    def _save_versions_metadata(self, model_name: str, data: Dict[str, Any]) -> None:
        path = self._versions_metadata_path(model_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, default=str)

    def _version_artifact_path(self, model_name: str, version: str) -> Path:
        from .version import is_valid_version

        if not is_valid_version(version):
            raise ValueError(f"invalid semantic version: {version!r}")
        return self._versions_dir(model_name) / f"{version}.joblib"

    def register_version(
        self,
        model_name: str,
        version: str,
        artifact_path: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a new semantic version of a model.

        Args:
            model_name: Logical model name (e.g. ``"win_predictor"``).
            version: Semantic version string (``"1.2.3"``).
            artifact_path: Path to the serialized model file. The file is
                copied into the registry's controlled storage; the
                original is left untouched.
            metadata: Optional metadata dict (training data, metrics,
                etc.). Stored alongside the artifact.

        The newly registered version becomes the active version if no
        active version is set yet, or if it sorts higher than the
        current active version.
        """
        from .version import compare_versions, is_valid_version

        if not is_valid_version(version):
            raise ValueError(f"invalid semantic version: {version!r}")

        src = Path(artifact_path)
        if not src.exists() or not src.is_file():
            raise FileNotFoundError(f"artifact not found: {artifact_path}")

        dest = self._version_artifact_path(model_name, version)
        with self._lock:
            data = self._load_versions_metadata(model_name)
            if version in data["versions"]:
                raise ValueError(
                    f"version {version!r} of {model_name!r} is already registered"
                )

            dest.parent.mkdir(parents=True, exist_ok=True)
            import shutil
            shutil.copyfile(src, dest)

            data["versions"][version] = {
                "artifact": dest.name,
                "metadata": dict(metadata or {}),
                "registered_at": datetime.now().isoformat(),
            }
            active = data.get("active_version")
            if active is None or compare_versions(version, active) > 0:
                data["active_version"] = version

            self._save_versions_metadata(model_name, data)
        logger.info("Registered version %s of %s", version, model_name)

    def list_semver_versions(self, model_name: str) -> List[Dict[str, Any]]:
        """List all semver-registered versions, sorted by version desc.

        Returns dicts of the form
        ``{"version": str, "metadata": dict, "registered_at": str, "is_active": bool}``.

        This is the semver-aware companion to the legacy
        :meth:`list_versions` (which returns a plain list of version
        strings from the timestamp-based registry).
        """
        from .version import parse_version

        data = self._load_versions_metadata(model_name)
        versions = data.get("versions", {})
        if not versions:
            return []
        active = data.get("active_version")
        rows: List[Dict[str, Any]] = []
        for version, entry in versions.items():
            rows.append({
                "version": version,
                "metadata": entry.get("metadata", {}),
                "registered_at": entry.get("registered_at"),
                "is_active": version == active,
            })
        rows.sort(key=lambda r: parse_version(r["version"]), reverse=True)
        return rows

    def load_version(
        self, model_name: str, version: Optional[str] = None
    ) -> Any:
        """Load a registered model version.

        Args:
            model_name: Logical model name.
            version: Requested semantic version. If ``None``, the active
                version is loaded. If the requested version is not
                registered, the fallback chain is consulted: previous
                patch -> previous minor -> previous major.

        Raises:
            ModelNotFoundError: if no usable version can be found.
        """
        data = self._load_versions_metadata(model_name)
        versions = data.get("versions", {})
        if not versions:
            raise ModelNotFoundError(
                f"no versions registered for model {model_name!r}"
            )

        target = version or data.get("active_version")
        if target is None:
            raise ModelNotFoundError(
                f"no active version for model {model_name!r}"
            )

        chosen = self._resolve_fallback(target, list(versions.keys()))
        if chosen is None:
            raise ModelNotFoundError(
                f"version {target!r} of {model_name!r} not found, "
                f"and no fallback (patch/minor/major) available"
            )

        if chosen != target:
            logger.warning(
                "Requested version %s of %s not found; using fallback %s",
                target, model_name, chosen,
            )

        path = self._version_artifact_path(model_name, chosen)
        if not path.exists():
            raise ModelNotFoundError(
                f"artifact for {model_name}@{chosen} missing on disk: {path}"
            )

        joblib = self._get_joblib()
        return joblib.load(path)

    @staticmethod
    def _resolve_fallback(target: str, available: List[str]) -> Optional[str]:
        """Walk the patch -> minor -> major fallback chain.

        Returns the highest registered version that satisfies the chain,
        or ``None`` if no candidate fits.
        """
        from .version import (
            parse_version,
            previous_major,
            previous_minor,
            previous_patch,
        )

        if target in available:
            return target

        available_parsed = {v: parse_version(v) for v in available}

        # Step 1: try the next-lower patch in the same minor.
        try:
            t_major, t_minor, t_patch = parse_version(target)
        except ValueError:
            return None

        # Highest version <= target, in same minor:
        same_minor = [
            v for v, p in available_parsed.items()
            if p[0] == t_major and p[1] == t_minor and p[2] < t_patch
        ]
        if same_minor:
            return max(same_minor, key=lambda v: available_parsed[v])

        # Step 2: previous minor.
        same_major = [
            v for v, p in available_parsed.items()
            if p[0] == t_major and p[1] < t_minor
        ]
        if same_major:
            return max(same_major, key=lambda v: available_parsed[v])

        # Step 3: previous major.
        prev_major = [
            v for v, p in available_parsed.items() if p[0] < t_major
        ]
        if prev_major:
            return max(prev_major, key=lambda v: available_parsed[v])

        return None

    def rollback(self, model_name: str, to_version: str) -> None:
        """Set the active version of a model to a previously registered version.

        Raises:
            ModelNotFoundError: if ``to_version`` is not registered.
        """
        with self._lock:
            data = self._load_versions_metadata(model_name)
            if to_version not in data.get("versions", {}):
                raise ModelNotFoundError(
                    f"cannot rollback {model_name!r} to {to_version!r}: "
                    f"version not registered"
                )
            previous = data.get("active_version")
            data["active_version"] = to_version
            self._save_versions_metadata(model_name, data)
        logger.info(
            "Rolled back %s active version from %s to %s",
            model_name, previous, to_version,
        )

    def get_active_version(self, model_name: str) -> Optional[str]:
        """Return the currently active version of a model, or ``None``."""
        data = self._load_versions_metadata(model_name)
        active = data.get("active_version")
        return active if isinstance(active, str) else None


# ── Module-level singleton ────────────────────────────────────────────────────

_registry: Optional[ModelRegistry] = None
_registry_lock = threading.Lock()

def get_model_registry() -> ModelRegistry:
    """Get the global model registry instance (lazy-initialized)."""
    global _registry
    if _registry is None:
        with _registry_lock:
            if _registry is None:
                _registry = ModelRegistry()
    return _registry


__all__ = ["ModelRegistry", "get_model_registry"]
