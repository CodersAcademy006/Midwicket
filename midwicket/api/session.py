import logging
import threading
from pathlib import Path
from typing import Optional, Any
from datetime import date
from midwicket.api.models import PlayerStats
import pyarrow as pa
from tqdm import tqdm

# Internal Imports
from midwicket.storage.thread_safe_engine import ThreadSafeQueryEngine as QueryEngine
from midwicket.storage.registry import IdentityRegistry
from midwicket.storage.snapshots import SnapshotManager
from midwicket.runtime.executor import RuntimeExecutor
from midwicket.runtime.cache_duckdb import DuckDBCache
from midwicket.data.loader import DEFAULT_DATA_DIR, DataLoader
from midwicket.data.pipeline import build_registry_stats
from midwicket.core.canonicalize import canonicalize_match
from midwicket.core.migration import migrate_on_connect

logger = logging.getLogger(__name__)

class MidwicketSession:
    _instance: Optional["MidwicketSession"] = None
    _instance_lock = threading.Lock()

    def __init__(self, data_dir: Optional[str] = None, skip_registry_build: bool = False,
                 engine: Optional[QueryEngine] = None) -> None:
        if data_dir is None:
            self.data_dir = Path(":memory:")
        else:
            self.data_dir = Path(data_dir)
            
        self.is_memory = (str(self.data_dir) == ":memory:")
        
        if self.is_memory:
            import tempfile
            import shutil
            self._temp_dir: Optional[str] = tempfile.mkdtemp(prefix="midwicket_ram_")
            temp_path = Path(self._temp_dir)
            
            self.db_path = str(temp_path / "midwicket.duckdb")
            self.registry_path = str(temp_path / "registry.duckdb")
            self.cache_path = str(temp_path / "cache.duckdb")
            
            # Copy pre-packaged databases to the temp folder if they exist
            pkg_data_dir = Path(__file__).parent.parent / "data"
            pkg_db = pkg_data_dir / "midwicket.duckdb"
            pkg_registry = pkg_data_dir / "registry.duckdb"
            
            if pkg_db.exists():
                try:
                    shutil.copyfile(pkg_db, self.db_path)
                except Exception as e:
                    logger.warning("Failed to copy pre-built midwicket.duckdb: %s", e)
            if pkg_registry.exists():
                try:
                    shutil.copyfile(pkg_registry, self.registry_path)
                except Exception as e:
                    logger.warning("Failed to copy pre-built registry.duckdb: %s", e)
        else:
            self._temp_dir = None
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self.db_path = str(self.data_dir / "midwicket.duckdb")
            self.registry_path = str(self.data_dir / "registry.duckdb")
            self.cache_path = str(self.data_dir / "cache.duckdb")
        
        # Initialize Components
        self.registry = IdentityRegistry(self.registry_path)
        self.engine = engine if engine else QueryEngine(self.db_path)
        self.snapshot_manager = SnapshotManager(str(self.data_dir))
        
        # Restore latest snapshot on boot
        latest_snap = self.snapshot_manager.get_latest()
        if latest_snap != "initial":
            self.engine._snapshot_id = latest_snap
            
        self.cache = DuckDBCache(self.cache_path)
        self.executor = RuntimeExecutor(self.cache, self.engine)
        self.loader = DataLoader(str(self.data_dir))
        
        # Auto-migrate schema if needed
        migrate_on_connect(str(self.data_dir))
        
        # Auto-Setup (skip if using bundled data)
        if not skip_registry_build:
            # Only attempt registry build when raw data is actually present.
            # This prevents a NotImplementedError crash on first import when
            # build_registry_stats() has not been implemented yet.
            if self.is_memory:
                raw_data_present = self.loader.zip_path.exists()
            else:
                raw_data_present = (
                    self.loader.raw_dir is not None
                    and self.loader.raw_dir.exists()
                    and bool(list(self.loader.raw_dir.glob("*.json")))
                )
            try:
                _row = self.registry.con.execute("SELECT COUNT(*) FROM player_stats").fetchone()
                row_count = _row[0] if _row is not None else 0
                registry_empty = (row_count == 0)
            except Exception:
                registry_empty = True

            # Also rebuild when matchup_stats table does not exist (handles schema migration
            # from older registry.duckdb files that pre-date matchup tracking).
            try:
                self.registry.con.execute("SELECT 1 FROM matchup_stats LIMIT 1")
                matchup_table_missing = False
            except Exception:
                matchup_table_missing = True

            needs_build = registry_empty or matchup_table_missing

            if needs_build:
                if not raw_data_present:
                    logger.info("No raw data found. Run loader.download() or call session.download_data() to fetch data.")
                else:
                    logger.info("Building registry & summary stats from raw data...")
                    try:
                        build_registry_stats(self.loader, self.registry)
                    except NotImplementedError:
                        logger.warning(
                            "build_registry_stats() is not yet implemented. "
                            "The registry will be empty until it is provided. "
                            "You can still load individual matches via session.load_match()."
                        )

    def download_data(self, force: bool = False) -> None:
        """Download raw match data. Safe to call multiple times."""
        self.loader.download(force=force)

    def load_match(self, match_id: str) -> None:
        """
        Lazy loads a specific match into the 'Heavy' engine.

        Idempotent: if the match is already present in ball_events, this is a
        no-op. This prevents the double-counting bug where a read endpoint
        (GET /matches/{match_id}) would append duplicate rows on every hit.
        """
        # Fast-path: skip if match is already loaded.
        try:
            result = self.engine.execute_sql(
                "SELECT 1 FROM ball_events WHERE match_id = ? LIMIT 1",
                [match_id],
            )
            rows = result.to_pydict()
            if rows and any(len(v) > 0 for v in rows.values()):
                logger.debug("Match %s already loaded, skipping.", match_id)
                return
        except Exception:
            # Table may not exist yet — proceed to load.
            pass

        logger.info("Loading match %s", match_id)
        try:
            data = self.loader.get_match(match_id)
            table = canonicalize_match(data, self.registry, match_id)
            tag = f"match_{match_id}"
            self.engine.ingest_events(table, snapshot_tag=tag, append=True)
            self.snapshot_manager.create_snapshot(tag=tag, description=f"Loaded match {match_id}")
            logger.info("Match %s loaded successfully.", match_id)
        except (FileNotFoundError, ValueError, RuntimeError) as e:
            logger.error("Failed to load match %s: %s", match_id, e)

    def get_player_stats(self, player_id: str) -> Optional[PlayerStats]:
        """Get player statistics by ID or name."""
        # Try to resolve as name first, then as ID
        try:
            player_id_int = int(player_id)
            entity_id: Optional[int] = player_id_int
        except (TypeError, ValueError):
            # Use centralized name-to-id helper from IdentityRegistry (MW-033)
            entity_id = self.registry.resolve_player_without_date(player_id)
        
        if entity_id is None:
            return None
            
        # Get stats from registry
        stats_dict = self.registry.get_player_stats(entity_id)
        if not stats_dict:
            return None
            
        # Get player name from entities table
        name_result = self.registry.con.execute(
            "SELECT primary_name FROM entities WHERE id = ?", [entity_id]
        ).fetchone()
        player_name = name_result[0] if name_result else f"Player {entity_id}"
        
        return PlayerStats(
            name=player_name,
            matches=stats_dict["matches"],
            runs=stats_dict["runs"],
            balls_faced=stats_dict["balls_faced"],
            dismissals=int(stats_dict.get("dismissals", 0) or 0),
            wickets=stats_dict["wickets"],
            balls_bowled=stats_dict["balls_bowled"],
            runs_conceded=stats_dict["runs_conceded"]
        )

    def get_match_stats(self, match_id: str) -> Optional[dict]:
        """Get aggregate match statistics for a given match_id.

        Queries the ball_events table (must be loaded first via load_match).
        Returns a dict of match-level aggregates, or None if the match has no
        data or the table does not exist yet.
        """
        try:
            result = self.engine.execute_sql(
                """
                SELECT
                    match_id,
                    COUNT(*) AS total_balls,
                    SUM(runs_batter + runs_extras) AS total_runs,
                    SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS total_wickets,
                    MAX(over) + 1 AS overs_played
                FROM ball_events
                WHERE match_id = ?
                GROUP BY match_id
                """,
                params=[match_id],
            )
            rows = result.to_pydict()
            if not rows.get("match_id"):
                logger.debug("get_match_stats: no rows for match_id=%s", match_id)
                return None

            # Per-inning breakdown
            inning_result = self.engine.execute_sql(
                """
                SELECT
                    inning,
                    SUM(runs_batter + runs_extras) AS runs,
                    SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS wickets,
                    MAX(over) + 1 AS overs
                FROM ball_events
                WHERE match_id = ?
                GROUP BY inning
                ORDER BY inning
                """,
                params=[match_id],
            )
            inning_rows = inning_result.to_pydict()
            innings = []
            if inning_rows.get("inning"):
                for i in range(len(inning_rows["inning"])):
                    innings.append({k: inning_rows[k][i] for k in inning_rows})

            summary = {k: v[0] for k, v in rows.items()}
            summary["innings"] = innings
            return summary
        except (RuntimeError, AttributeError, TypeError, ValueError):
            logger.warning("get_match_stats: failed for match_id=%s", match_id, exc_info=True)
            return None

    def _setup_db(self) -> None:
        """Deprecated: Use lazy loading."""
        pass

    @classmethod
    def get(cls) -> "MidwicketSession":
        """Singleton Accessor"""
        with cls._instance_lock:
            if cls._instance is None:
                # AUTO-BOOT: If user forgot md.init(), just do it for them.
                logger.info("Auto-initializing Midwicket (defaulting to ./data)...")
                cls._instance = MidwicketSession(data_dir="./data")
            return cls._instance

    @classmethod
    def cleanup(cls) -> None:
        """Clean up the singleton instance."""
        instance: Optional["MidwicketSession"]
        with cls._instance_lock:
            instance = cls._instance
            cls._instance = None

        if instance is not None:
            instance.close()

    def close(self) -> None:
        """Close all database connections."""
        for name, component in (
            ("registry", self.registry),
            ("engine", self.engine),
            ("cache", self.cache),
        ):
            try:
                component.close()
            except Exception:
                logger.warning("Failed to close session %s component", name, exc_info=True)

        # Clear singleton reference to prevent stale instances.
        # Hold the class lock so concurrent get/init calls see a consistent state.
        with MidwicketSession._instance_lock:
            if MidwicketSession._instance is self:
                MidwicketSession._instance = None

        # Clean up temp directory
        if getattr(self, "_temp_dir", None) is not None:
            import shutil
            try:
                assert self._temp_dir is not None
                shutil.rmtree(self._temp_dir, ignore_errors=True)
            except Exception as e:
                logger.warning("Failed to clean up in-memory temp directory %s: %s", self._temp_dir, e)

    def __enter__(self) -> "MidwicketSession":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

# Helper to expose the executor directly to API modules
def get_executor() -> RuntimeExecutor:
    return MidwicketSession.get().executor

def get_registry() -> IdentityRegistry:
    return MidwicketSession.get().registry

def get_session() -> MidwicketSession:
    return MidwicketSession.get()

def init(source: Optional[str] = None) -> MidwicketSession:
    """
    Initialize the Midwicket session.
    """
    session = MidwicketSession(data_dir=source)
    previous: Optional[MidwicketSession]
    with MidwicketSession._instance_lock:
        previous = MidwicketSession._instance
        MidwicketSession._instance = session

    if previous is not None and previous is not session:
        try:
            previous.close()
        except Exception:
            logger.warning("Failed to close previous singleton session during init()", exc_info=True)

    return session

