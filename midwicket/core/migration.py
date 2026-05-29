"""
Midwicket Schema Migration: Automatic Schema Evolution

Handles schema changes without breaking existing user data.
Automatically patches Parquet files and databases when new columns are added.
"""

import logging
import os
from pathlib import Path
from typing import Dict, List, Set, Any
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

class SchemaMigration:
    """Handles automatic schema evolution for Midwicket data."""

    # Current expected schema version
    CURRENT_SCHEMA_VERSION = "1.1"

    # Schema definitions by version
    SCHEMA_VERSIONS = {
        "1.0": {
            "deliveries": [
                "match_id", "inning", "over", "ball", "batter", "bowler",
                "runs", "extras", "total_runs", "wicket", "dismissal_type"
            ],
            "matches": [
                "match_id", "date", "venue", "team1", "team2", "winner", "margin"
            ]
        },
        "1.1": {
            "deliveries": [
                "match_id", "inning", "over", "ball", "batter", "bowler",
                "runs", "extras", "total_runs", "wicket", "dismissal_type",
                "is_impact_player"  # New column for IPL 2023+ rules
            ],
            "matches": [
                "match_id", "date", "venue", "team1", "team2", "winner", "margin",
                "competition", "season"  # New metadata columns
            ]
        }
    }

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.db_path = self.data_dir / "midwicket.duckdb"
        self.schema_file = self.data_dir / ".schema_version"
        # Number of real tables changed by the most recent migration step.
        self.tables_migrated = 0

    def get_current_schema_version(self) -> str:
        """Get the current schema version from disk."""
        if self.schema_file.exists():
            return self.schema_file.read_text().strip()
        return "1.0"  # Default for existing installations

    def set_schema_version(self, version: str):
        """Update the schema version on disk."""
        self.schema_file.write_text(version)

    def check_and_migrate(self) -> bool:
        """
        Check if migration is needed and perform it.

        Returns True if migration was performed, False if already up to date.
        """
        current_version = self.get_current_schema_version()
        target_version = self.CURRENT_SCHEMA_VERSION

        if current_version == target_version:
            return False

        logger.info("Migrating schema from %s to %s...", current_version, target_version)

        # Perform migrations step by step
        if current_version == "1.0" and target_version == "1.1":
            self.tables_migrated = self._migrate_1_0_to_1_1()

        self.set_schema_version(target_version)
        logger.info(
            "Schema version advanced to %s (%d legacy table(s) migrated).",
            target_version, self.tables_migrated,
        )
        return True

    @staticmethod
    def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        """True only if a real table of this name exists in the database."""
        row = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()
        return bool(row and row[0] > 0)

    def _migrate_1_0_to_1_1(self) -> int:
        """Migrate 1.0 -> 1.1, touching only tables that actually exist.

        Older revisions blindly ran ``ALTER TABLE deliveries`` / ``matches`` even
        though canonicalized data lives in a single ``ball_events`` table, so the
        ALTERs failed silently while the caller still logged success (MW-019). We
        now guard every statement on real table existence and return the number of
        tables actually changed, so callers can log the truth.
        """
        migrated = 0
        con = duckdb.connect(str(self.db_path))
        try:
            if self._table_exists(con, "deliveries"):
                con.execute(
                    "ALTER TABLE deliveries "
                    "ADD COLUMN IF NOT EXISTS is_impact_player BOOLEAN DEFAULT FALSE"
                )
                con.execute(
                    "UPDATE deliveries SET is_impact_player = TRUE "
                    "WHERE batter IN (SELECT batter FROM deliveries "
                    "GROUP BY batter HAVING COUNT(DISTINCT match_id) >= 10)"
                )
                migrated += 1
                logger.debug("Migrated deliveries table to schema 1.1")

            if self._table_exists(con, "matches"):
                con.execute(
                    "ALTER TABLE matches "
                    "ADD COLUMN IF NOT EXISTS competition VARCHAR DEFAULT 'unknown'"
                )
                con.execute(
                    "ALTER TABLE matches "
                    "ADD COLUMN IF NOT EXISTS season INTEGER DEFAULT 2023"
                )
                migrated += 1
                logger.debug("Migrated matches table to schema 1.1")
        except Exception as e:  # nosec B110 — schema patch is best-effort
            logger.warning("Migration warning: %s", e)
        finally:
            con.close()
        return migrated

    def validate_schema(self) -> Dict[str, Any]:
        """
        Validate that the current database matches the expected schema.

        Returns dict with validation results.
        """
        con = duckdb.connect(str(self.db_path))
        results: Dict[str, Any] = {"valid": True, "issues": []}

        try:
            expected_tables = self.SCHEMA_VERSIONS[self.CURRENT_SCHEMA_VERSION]

            for table_name, expected_columns in expected_tables.items():
                # Check if table exists
                row = con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_name = ?",
                    [table_name],
                ).fetchone()
                table_exists = (row[0] > 0) if row is not None else False

                if not table_exists:
                    results["issues"].append(f"Missing table: {table_name}")
                    results["valid"] = False
                    continue

                # Check columns
                actual_columns = con.execute(
                    "SELECT column_name "
                    "FROM information_schema.columns "
                    "WHERE table_name = ? "
                    "ORDER BY ordinal_position",
                    [table_name],
                ).fetchall()

                actual_column_names = [col[0] for col in actual_columns]

                for expected_col in expected_columns:
                    if expected_col not in actual_column_names:
                        results["issues"].append(f"Missing column '{expected_col}' in table '{table_name}'")
                        results["valid"] = False

        except Exception as e:
            results["valid"] = False
            results["issues"].append(f"Validation error: {e}")

        finally:
            con.close()

        return results

class SchemaMigrator:
    """
    Handles schema migrations for Midwicket data lake.

    When a new version introduces schema changes, this automatically
    patches existing Parquet files to maintain compatibility.
    """

    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
        self.snapshots_dir = self.data_dir / "snapshots"

    def check_and_migrate(self) -> Dict[str, Any]:
        """
        Checks all Parquet files and migrates schemas if needed.

        Returns:
            Dict with migration statistics
        """
        if not self.snapshots_dir.exists():
            return {"status": "no_snapshots", "migrated": 0, "errors": 0}

        migrated = 0
        errors = 0

        for parquet_file in self.snapshots_dir.glob("**/*.parquet"):
            try:
                if self._needs_migration(parquet_file):
                    self._migrate_file(parquet_file)
                    migrated += 1
            except Exception as e:
                logger.error("Migration failed for %s: %s", parquet_file, e)
                errors += 1

        return {
            "status": "completed",
            "migrated": migrated,
            "errors": errors,
            "total_files": len(list(self.snapshots_dir.glob("**/*.parquet")))
        }

    def _needs_migration(self, parquet_file: Path) -> bool:
        """Check if a Parquet file needs schema migration."""
        try:
            table = pa.parquet.read_table(parquet_file)
            current_schema = table.schema

            # Check for missing columns that were added in newer versions
            required_fields = ['is_impact_player']  # Example: new field for impact player

            for field_name in required_fields:
                if field_name not in current_schema.names:
                    return True

            return False
        except Exception:
            return False

    def _migrate_file(self, parquet_file: Path) -> None:
        """Migrate a single Parquet file to the current schema."""
        table = pa.parquet.read_table(parquet_file)

        # Add missing columns with null values
        new_columns = []

        # Example: Add is_impact_player column if missing
        if 'is_impact_player' not in table.schema.names:
            # For now, default to False (not impact player)
            # In real implementation, this would be determined from match metadata
            impact_col = pa.array([False] * table.num_rows, type=pa.bool_())
            new_columns.append(('is_impact_player', impact_col))

        if new_columns:
            # Create new table with additional columns
            existing_arrays = [table.column(name) for name in table.schema.names]
            new_arrays = existing_arrays + [col for _, col in new_columns]
            new_field_names = table.schema.names + [name for name, _ in new_columns]

            new_schema = pa.schema([
                pa.field(name, arr.type)
                for name, arr in zip(new_field_names, new_arrays)
            ])

            new_table = pa.Table.from_arrays(new_arrays, schema=new_schema)

            # Write back with updated schema
            temp_file = parquet_file.with_suffix('.temp')
            pa.parquet.write_table(new_table, temp_file)
            temp_file.replace(parquet_file)

def migrate_on_connect(data_dir: str) -> None:
    """
    Automatically migrate schema when connecting to a session.

    This should be called in MidwicketSession.__init__ or similar.
    Skips migration entirely on fresh installations (no schema_version file
    and no pre-existing DB tables) to avoid misleading warnings.
    """
    if data_dir == ":memory:":
        return
    migrator = SchemaMigration(data_dir)

    # Fresh install: write current version directly without running any migration.
    if not migrator.schema_file.exists():
        # Mark as current — no legacy tables to migrate from.
        migrator.set_schema_version(migrator.CURRENT_SCHEMA_VERSION)
        return

    if migrator.check_and_migrate():
        if migrator.tables_migrated:
            logger.info(
                "Database schema updated; %d legacy table(s) migrated. Existing data is safe.",
                migrator.tables_migrated,
            )
        else:
            logger.info(
                "Schema version marker advanced to %s; no legacy tables present, no data changed.",
                migrator.CURRENT_SCHEMA_VERSION,
            )

def validate_database_integrity(data_dir: str) -> Dict[str, Any]:
    """
    Validate database integrity and schema compliance.
    """
    migrator = SchemaMigration(data_dir)
    return migrator.validate_schema()

def migrate_data_lake(data_dir: str = "./data") -> Dict[str, Any]:
    """
    Public API for data lake migration.

    Call this after upgrading Midwicket to ensure data compatibility.
    """
    migrator = SchemaMigrator(data_dir)
    result = migrator.check_and_migrate()

    if result["migrated"] > 0:
        logger.info("Migrated %d files to current schema", result["migrated"])
    if result["errors"] > 0:
        logger.warning("%d files had migration errors", result["errors"])

    return result

# Convenience functions
def get_schema_version(data_dir: str) -> str:
    """Get current schema version for a data directory."""
    migrator = SchemaMigration(data_dir)
    return migrator.get_current_schema_version()

def force_migration(data_dir: str, target_version: str = None) -> bool:
    """Force a schema migration (for testing/debugging)."""
    migrator = SchemaMigration(data_dir)
    if target_version:
        migrator.CURRENT_SCHEMA_VERSION = target_version
    return migrator.check_and_migrate()

__all__ = [
    'SchemaMigration',
    'SchemaMigrator',
    'migrate_on_connect',
    'validate_database_integrity',
    'migrate_data_lake',
    'get_schema_version',
    'force_migration'
]