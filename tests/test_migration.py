"""
Tests for pypitch.core.migration — schema versioning and migrate_on_connect.

Coverage target: raise migration.py from 29% to >= 60%.
"""

import pytest
from pathlib import Path


class TestSchemaMigration:
    """Tests for the SchemaMigration class."""

    def test_fresh_install_writes_current_version(self, isolated_data_dir):
        """migrate_on_connect on a fresh dir writes current version, no SQL run."""
        from pypitch.core.migration import migrate_on_connect, get_schema_version, SchemaMigration

        migrate_on_connect(str(isolated_data_dir))

        version = get_schema_version(str(isolated_data_dir))
        assert version == SchemaMigration.CURRENT_SCHEMA_VERSION

    def test_fresh_install_no_migration_sql(self, isolated_data_dir, caplog):
        """A fresh install must not attempt to run ALTER TABLE on missing tables."""
        import logging
        from pypitch.core.migration import migrate_on_connect

        with caplog.at_level(logging.WARNING, logger="pypitch"):
            migrate_on_connect(str(isolated_data_dir))

        # No warnings should mention ALTER TABLE or deliveries table
        for record in caplog.records:
            assert "deliveries" not in record.message.lower(), (
                f"Unexpected warning about 'deliveries': {record.message}"
            )

    def test_already_current_no_op(self, isolated_data_dir):
        """migrate_on_connect is a no-op when schema is already current."""
        from pypitch.core.migration import migrate_on_connect, SchemaMigration

        # Set version to current manually
        schema_file = isolated_data_dir / ".schema_version"
        schema_file.write_text(SchemaMigration.CURRENT_SCHEMA_VERSION)

        # Should not raise, should be a no-op
        migrate_on_connect(str(isolated_data_dir))

        assert schema_file.read_text().strip() == SchemaMigration.CURRENT_SCHEMA_VERSION

    def test_get_current_schema_version_default(self, isolated_data_dir):
        """get_current_schema_version returns '1.0' when no file exists."""
        from pypitch.core.migration import SchemaMigration

        migrator = SchemaMigration(str(isolated_data_dir))
        # No schema file exists yet
        assert not migrator.schema_file.exists()
        assert migrator.get_current_schema_version() == "1.0"

    def test_set_and_get_schema_version(self, isolated_data_dir):
        """set_schema_version persists; get_current_schema_version reads it back."""
        from pypitch.core.migration import SchemaMigration

        migrator = SchemaMigration(str(isolated_data_dir))
        migrator.set_schema_version("1.1")
        assert migrator.get_current_schema_version() == "1.1"


class TestSchemaMigrator:
    """Tests for SchemaMigrator (Parquet file migrator)."""

    def test_no_snapshots_dir(self, isolated_data_dir):
        """Returns 'no_snapshots' status when snapshots dir doesn't exist."""
        from pypitch.core.migration import SchemaMigrator

        migrator = SchemaMigrator(str(isolated_data_dir))
        result = migrator.check_and_migrate()
        assert result["status"] == "no_snapshots"
        assert result["migrated"] == 0

    def test_empty_snapshots_dir(self, isolated_data_dir):
        """Returns zero migrated when snapshots dir is empty."""
        from pypitch.core.migration import SchemaMigrator

        snapshots = isolated_data_dir / "snapshots"
        snapshots.mkdir()

        migrator = SchemaMigrator(str(isolated_data_dir))
        result = migrator.check_and_migrate()
        assert result["status"] == "completed"
        assert result["migrated"] == 0


class TestConvenienceFunctions:
    """Tests for module-level convenience wrappers."""

    def test_migrate_data_lake_no_data(self, isolated_data_dir):
        """migrate_data_lake with empty dir returns completed status."""
        from pypitch.core.migration import migrate_data_lake

        result = migrate_data_lake(str(isolated_data_dir))
        assert result["status"] in ("no_snapshots", "completed")

    def test_force_migration_to_current(self, isolated_data_dir):
        """force_migration returns False when already at target version."""
        from pypitch.core.migration import SchemaMigration, force_migration

        # Write current version first
        schema_file = isolated_data_dir / ".schema_version"
        schema_file.write_text(SchemaMigration.CURRENT_SCHEMA_VERSION)

        result = force_migration(str(isolated_data_dir))
        assert result is False
