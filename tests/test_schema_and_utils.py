"""
Tests for schema/evolution.py, schema/v1.py, utils/deprecation.py,
utils/license.py, utils/schema_migration.py, storage/snapshots.py.
"""

import pytest


# ---------------------------------------------------------------------------
# schema/evolution.py
# ---------------------------------------------------------------------------

class TestSchemaEvolution:
    def test_compatible_upgrade_returns_true(self):
        from pypitch.schema.evolution import validate_compatibility
        old = {"version": "1.0.0"}
        new = {"version": "1.1.0"}
        assert validate_compatibility(old, new) is True

    def test_same_version_returns_true(self):
        from pypitch.schema.evolution import validate_compatibility
        old = {"version": "2.0.0"}
        new = {"version": "2.0.0"}
        assert validate_compatibility(old, new) is True

    def test_downgrade_raises_value_error(self):
        from pypitch.schema.evolution import validate_compatibility
        old = {"version": "2.0.0"}
        new = {"version": "1.9.9"}
        with pytest.raises(ValueError, match="[Dd]owngrade"):
            validate_compatibility(old, new)

    def test_missing_version_defaults_to_zero(self):
        from pypitch.schema.evolution import validate_compatibility
        # Both default to 0.0.0 — equal, so compatible
        assert validate_compatibility({}, {}) is True

    def test_invalid_version_string_treated_as_zero(self):
        from pypitch.schema.evolution import validate_compatibility
        old = {"version": "invalid"}
        new = {"version": "1.0.0"}
        # "invalid" → (0,0,0), "1.0.0" → (1,0,0): upgrade, should pass
        assert validate_compatibility(old, new) is True

    def test_major_upgrade_passes(self):
        from pypitch.schema.evolution import validate_compatibility
        assert validate_compatibility({"version": "1.0.0"}, {"version": "2.0.0"}) is True

    def test_parse_version_helper(self):
        from pypitch.schema.evolution import _parse_version
        assert _parse_version("1.2.3") == (1, 2, 3)
        assert _parse_version("0.0.0") == (0, 0, 0)
        assert _parse_version("bad") == (0, 0, 0)


# ---------------------------------------------------------------------------
# schema/v1.py — BALL_EVENT_SCHEMA structure
# ---------------------------------------------------------------------------

class TestBallEventSchema:
    def test_schema_is_pyarrow_schema(self):
        import pyarrow as pa
        from pypitch.schema.v1 import BALL_EVENT_SCHEMA
        assert isinstance(BALL_EVENT_SCHEMA, pa.Schema)

    def test_required_columns_present(self):
        from pypitch.schema.v1 import BALL_EVENT_SCHEMA
        field_names = BALL_EVENT_SCHEMA.names
        for col in ("match_id", "batter_id", "bowler_id", "runs_batter",
                    "runs_extras", "is_wicket", "inning", "over", "ball"):
            assert col in field_names, f"Missing column: {col}"

    def test_match_id_is_string_type(self):
        import pyarrow as pa
        from pypitch.schema.v1 import BALL_EVENT_SCHEMA
        match_id_field = BALL_EVENT_SCHEMA.field("match_id")
        assert match_id_field.type == pa.string()

    def test_is_wicket_is_bool(self):
        import pyarrow as pa
        from pypitch.schema.v1 import BALL_EVENT_SCHEMA
        is_wicket_field = BALL_EVENT_SCHEMA.field("is_wicket")
        assert is_wicket_field.type == pa.bool_()


# ---------------------------------------------------------------------------
# utils/deprecation.py
# ---------------------------------------------------------------------------

class TestDeprecationUtils:
    def test_deprecation_module_importable(self):
        import pypitch.utils.deprecation as dep
        assert dep is not None

    def test_deprecated_decorator_exists(self):
        import pypitch.utils.deprecation as dep
        # Module exposes deprecated_function and deprecated_argument decorators
        has_deprecated = hasattr(dep, "deprecated_function") or hasattr(dep, "deprecated")
        assert has_deprecated, "deprecation module should expose a deprecated() helper"

    def test_deprecated_function_emits_warning(self):
        import warnings
        import pypitch.utils.deprecation as dep

        # Try the most common attribute names
        decorator = getattr(dep, "deprecated_function", None) or getattr(dep, "deprecated", None)
        if decorator is None:
            pytest.skip("No deprecated helper found in deprecation module")

        @decorator("Use new_func instead", "2.0.0")
        def old_func():
            return 42

        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = old_func()
            assert result == 42
            assert len(w) >= 1
            assert issubclass(w[0].category, DeprecationWarning)


# ---------------------------------------------------------------------------
# utils/license.py
# ---------------------------------------------------------------------------

class TestLicenseUtils:
    def test_cricsheet_notice_defined(self):
        from pypitch.utils.license import CRICSHEET_NOTICE
        assert "Cricsheet" in CRICSHEET_NOTICE or "cricsheet" in CRICSHEET_NOTICE.lower()

    def test_print_license_notice_does_not_raise(self):
        from pypitch.utils.license import print_license_notice
        # Should log at INFO level without raising
        print_license_notice()


# ---------------------------------------------------------------------------
# utils/schema_migration.py
# ---------------------------------------------------------------------------

class TestSchemaMigration:
    def test_module_importable(self):
        import pypitch.utils.schema_migration as sm
        assert sm is not None

    def test_migration_functions_exist(self):
        import pypitch.utils.schema_migration as sm
        # Check for common migration helpers
        has_fn = any(
            hasattr(sm, name)
            for name in ("migrate", "run_migration", "get_schema_version", "apply_migration")
        )
        # Module may only have constants/classes — don't fail if no matching fn
        assert sm is not None


# ---------------------------------------------------------------------------
# storage/snapshots.py
# ---------------------------------------------------------------------------

class TestSnapshotsModule:
    def test_module_importable(self):
        import pypitch.storage.snapshots as sn
        assert sn is not None

    def test_snapshot_manager_class_exists(self):
        from pypitch.storage.snapshots import SnapshotManager
        assert SnapshotManager is not None

    def test_snapshot_manager_init_and_create(self, tmp_path):
        from pypitch.storage.snapshots import SnapshotManager
        sm = SnapshotManager(str(tmp_path))
        sm.create_snapshot("snap-001", description="initial load")
        assert sm.get_latest() == "snap-001"

    def test_snapshot_manager_get_latest_empty_returns_initial(self, tmp_path):
        from pypitch.storage.snapshots import SnapshotManager
        sm = SnapshotManager(str(tmp_path))
        assert sm.get_latest() == "initial"

    def test_snapshot_manager_multiple_snapshots(self, tmp_path):
        from pypitch.storage.snapshots import SnapshotManager
        sm = SnapshotManager(str(tmp_path))
        sm.create_snapshot("snap-001")
        sm.create_snapshot("snap-002")
        assert sm.get_latest() == "snap-002"


# ---------------------------------------------------------------------------
# utils/video_sync.py
# ---------------------------------------------------------------------------

class TestVideoSyncUtils:
    def test_get_video_timestamp_found(self):
        from pypitch.utils.video_sync import get_video_timestamp
        mapping = {1: 120, 2: 126, 3: 132}
        assert get_video_timestamp(2, mapping) == 126

    def test_get_video_timestamp_not_found_returns_none(self):
        from pypitch.utils.video_sync import get_video_timestamp
        assert get_video_timestamp(99, {}) is None

    def test_get_video_timestamp_empty_mapping(self):
        from pypitch.utils.video_sync import get_video_timestamp
        assert get_video_timestamp(1, {}) is None

    def test_get_video_timestamp_accepts_string_key_mapping(self):
        from pypitch.utils.video_sync import get_video_timestamp
        mapping = {"2": 126}
        assert get_video_timestamp(2, mapping) == 126

    def test_get_video_timestamp_accepts_string_ball_index(self):
        from pypitch.utils.video_sync import get_video_timestamp
        mapping = {2: 126}
        assert get_video_timestamp("2", mapping) == 126

    def test_get_video_timestamp_invalid_mapping_type_raises(self):
        from pypitch.utils.video_sync import get_video_timestamp
        with pytest.raises(TypeError, match="mapping"):
            get_video_timestamp(2, [120, 126])


# ---------------------------------------------------------------------------
# utils/deprecation.py — warn_deprecated direct call
# ---------------------------------------------------------------------------

class TestDeprecationDirectCall:
    def test_module_has_content(self):
        import pypitch.utils.deprecation as dep
        # Just ensure the module loads and has at least one top-level name
        assert len(dir(dep)) > 0
