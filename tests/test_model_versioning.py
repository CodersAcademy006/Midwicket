"""
Tests for semantic versioning support in the ModelRegistry.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from midwicket.exceptions import ModelNotFoundError
from midwicket.models.registry import ModelRegistry
from midwicket.models.version import (
    compare_versions,
    is_valid_version,
    next_major,
    next_minor,
    next_patch,
    parse_version,
    previous_major,
    previous_minor,
    previous_patch,
)


# --------------------------------------------------------------------------- #
# Semver helpers
# --------------------------------------------------------------------------- #


class TestVersionHelpers:
    def test_parse_basic(self) -> None:
        assert parse_version("1.2.3") == (1, 2, 3)
        assert parse_version("0.0.0") == (0, 0, 0)
        assert parse_version("10.20.30") == (10, 20, 30)

    def test_parse_with_prerelease(self) -> None:
        assert parse_version("1.2.3-rc1") == (1, 2, 3)
        assert parse_version("1.2.3+build42") == (1, 2, 3)
        assert parse_version("1.2.3-rc1+build42") == (1, 2, 3)

    @pytest.mark.parametrize("bad", ["", "1", "1.2", "1.2.3.4", "a.b.c", "01.2.3", "1.02.3"])
    def test_parse_invalid_raises(self, bad: str) -> None:
        with pytest.raises(ValueError):
            parse_version(bad)

    def test_parse_non_string_raises(self) -> None:
        with pytest.raises(ValueError):
            parse_version(123)  # type: ignore[arg-type]

    def test_compare(self) -> None:
        assert compare_versions("1.0.0", "1.0.0") == 0
        assert compare_versions("1.0.0", "2.0.0") == -1
        assert compare_versions("2.0.0", "1.0.0") == 1
        assert compare_versions("1.10.0", "1.9.0") == 1  # numeric, not lex
        assert compare_versions("1.2.3", "1.2.10") == -1

    def test_bumpers(self) -> None:
        assert next_patch("1.2.3") == "1.2.4"
        assert next_minor("1.2.3") == "1.3.0"
        assert next_major("1.2.3") == "2.0.0"

    def test_previous(self) -> None:
        assert previous_patch("1.2.3") == "1.2.2"
        assert previous_minor("1.2.3") == "1.1.0"
        assert previous_major("2.0.0") == "1.0.0"

    def test_previous_floor_raises(self) -> None:
        with pytest.raises(ValueError):
            previous_patch("1.2.0")
        with pytest.raises(ValueError):
            previous_minor("1.0.0")
        with pytest.raises(ValueError):
            previous_major("0.5.0")

    def test_is_valid_version(self) -> None:
        assert is_valid_version("1.2.3") is True
        assert is_valid_version("1.2") is False
        assert is_valid_version("a.b.c") is False


# --------------------------------------------------------------------------- #
# Registry semver round trips
# --------------------------------------------------------------------------- #


@pytest.fixture
def registry(tmp_path: Path) -> ModelRegistry:
    """Fresh registry pointing at an isolated base_path."""
    return ModelRegistry(base_path=str(tmp_path))


def _write_dummy_artifact(path: Path, payload: str = "model-bytes") -> None:
    """Create a fake joblib file. The registry copies bytes only; the
    contents don't have to be a real pickle for register/load tests
    that don't invoke joblib.load."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload.encode())


class TestRegisterAndList:
    def test_register_invalid_version_raises(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        artifact = tmp_path / "src.joblib"
        _write_dummy_artifact(artifact)
        with pytest.raises(ValueError):
            registry.register_version("m", "1.0", str(artifact), {})

    def test_register_missing_artifact_raises(
        self, registry: ModelRegistry
    ) -> None:
        with pytest.raises(FileNotFoundError):
            registry.register_version("m", "1.0.0", "/no/such/file", {})

    def test_register_and_list(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        artifact = tmp_path / "src.joblib"
        _write_dummy_artifact(artifact)

        registry.register_version("win_predictor", "1.0.0", str(artifact), {"auc": 0.84})
        registry.register_version("win_predictor", "1.1.0", str(artifact), {"auc": 0.85})
        registry.register_version("win_predictor", "2.0.0", str(artifact), {"auc": 0.87})

        rows = registry.list_semver_versions("win_predictor")
        assert [r["version"] for r in rows] == ["2.0.0", "1.1.0", "1.0.0"]
        assert rows[0]["is_active"] is True
        assert rows[0]["metadata"] == {"auc": 0.87}

    def test_register_duplicate_raises(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        artifact = tmp_path / "src.joblib"
        _write_dummy_artifact(artifact)
        registry.register_version("m", "1.0.0", str(artifact), {})
        with pytest.raises(ValueError):
            registry.register_version("m", "1.0.0", str(artifact), {})

    def test_list_empty_returns_empty_list(
        self, registry: ModelRegistry
    ) -> None:
        assert registry.list_semver_versions("nothing") == []


class TestActiveAndRollback:
    def test_active_version_updates_on_higher_register(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        artifact = tmp_path / "a.joblib"
        _write_dummy_artifact(artifact)
        registry.register_version("m", "1.0.0", str(artifact), {})
        assert registry.get_active_version("m") == "1.0.0"
        registry.register_version("m", "1.1.0", str(artifact), {})
        assert registry.get_active_version("m") == "1.1.0"

    def test_active_version_unchanged_for_lower_register(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        artifact = tmp_path / "a.joblib"
        _write_dummy_artifact(artifact)
        registry.register_version("m", "2.0.0", str(artifact), {})
        registry.register_version("m", "1.5.0", str(artifact), {})
        assert registry.get_active_version("m") == "2.0.0"

    def test_rollback(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        artifact = tmp_path / "a.joblib"
        _write_dummy_artifact(artifact)
        registry.register_version("m", "1.0.0", str(artifact), {})
        registry.register_version("m", "2.0.0", str(artifact), {})
        assert registry.get_active_version("m") == "2.0.0"

        registry.rollback("m", "1.0.0")
        assert registry.get_active_version("m") == "1.0.0"

        rows = registry.list_semver_versions("m")
        active_row = next(r for r in rows if r["is_active"])
        assert active_row["version"] == "1.0.0"

    def test_rollback_unknown_version_raises(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        artifact = tmp_path / "a.joblib"
        _write_dummy_artifact(artifact)
        registry.register_version("m", "1.0.0", str(artifact), {})
        with pytest.raises(ModelNotFoundError):
            registry.rollback("m", "9.9.9")

    def test_active_for_unknown_model_returns_none(
        self, registry: ModelRegistry
    ) -> None:
        assert registry.get_active_version("never_registered") is None


class TestFallbackChain:
    def _make_registry_with(
        self, tmp_path: Path, versions: list
    ) -> ModelRegistry:
        registry = ModelRegistry(base_path=str(tmp_path))
        artifact = tmp_path / "src.joblib"
        _write_dummy_artifact(artifact)
        for v in versions:
            registry.register_version("m", v, str(artifact), {"v": v})
        return registry

    def test_exact_match(self, tmp_path: Path) -> None:
        registry = self._make_registry_with(tmp_path, ["1.0.0", "1.1.0", "1.2.0"])
        # ModelRegistry._resolve_fallback is internal; test via load_version
        # by reading a real joblib. Since we only wrote bytes, mock joblib.load
        # to validate the resolved version.
        chosen = ModelRegistry._resolve_fallback(
            "1.1.0", ["1.0.0", "1.1.0", "1.2.0"]
        )
        assert chosen == "1.1.0"

    def test_patch_fallback(self) -> None:
        chosen = ModelRegistry._resolve_fallback(
            "1.2.5", ["1.2.0", "1.2.2", "1.2.3"]
        )
        assert chosen == "1.2.3"

    def test_minor_fallback(self) -> None:
        chosen = ModelRegistry._resolve_fallback(
            "1.3.0", ["1.0.0", "1.1.5", "1.2.0"]
        )
        assert chosen == "1.2.0"

    def test_major_fallback(self) -> None:
        chosen = ModelRegistry._resolve_fallback(
            "2.0.0", ["1.5.0", "1.4.0"]
        )
        assert chosen == "1.5.0"

    def test_no_candidate_returns_none(self) -> None:
        chosen = ModelRegistry._resolve_fallback(
            "1.0.0", ["2.0.0", "3.0.0"]
        )
        assert chosen is None


class TestLoadVersion:
    def test_load_active_when_no_version_given(
        self, registry: ModelRegistry, tmp_path: Path, monkeypatch
    ) -> None:
        # Provide a real pickled artifact so joblib.load succeeds.
        import joblib  # type: ignore[import]

        payload = {"hello": "world"}
        src = tmp_path / "src.joblib"
        joblib.dump(payload, src)
        registry.register_version("m", "1.0.0", str(src), {})

        loaded = registry.load_version("m")
        assert loaded == payload

    def test_load_unknown_model_raises(
        self, registry: ModelRegistry
    ) -> None:
        with pytest.raises(ModelNotFoundError):
            registry.load_version("nope")

    def test_load_uses_fallback_when_version_missing(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        import joblib  # type: ignore[import]

        payload = {"v": "1.2.0"}
        src = tmp_path / "src.joblib"
        joblib.dump(payload, src)
        registry.register_version("m", "1.0.0", str(src), {})
        registry.register_version("m", "1.2.0", str(src), {})

        # Request a non-registered patch; should fall back to 1.2.0
        loaded = registry.load_version("m", "1.2.5")
        assert loaded == payload


class TestStorageLayout:
    def test_metadata_file_created(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        artifact = tmp_path / "src.joblib"
        _write_dummy_artifact(artifact)
        registry.register_version("m", "1.0.0", str(artifact), {"x": 1})

        meta_file = Path(registry.base_path) / "versions" / "m" / "metadata.json"
        assert meta_file.exists()
        data = json.loads(meta_file.read_text())
        assert data["active_version"] == "1.0.0"
        assert "1.0.0" in data["versions"]

    def test_artifact_copied_into_storage(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        artifact = tmp_path / "src.joblib"
        artifact.write_bytes(b"unique-payload")
        registry.register_version("m", "1.0.0", str(artifact), {})

        stored = Path(registry.base_path) / "versions" / "m" / "1.0.0.joblib"
        assert stored.exists()
        assert stored.read_bytes() == b"unique-payload"

    def test_invalid_model_name_rejected(
        self, registry: ModelRegistry, tmp_path: Path
    ) -> None:
        artifact = tmp_path / "src.joblib"
        _write_dummy_artifact(artifact)
        with pytest.raises(ValueError):
            registry.register_version("../escape", "1.0.0", str(artifact), {})
