import logging
from pathlib import Path
from typing import Optional, Any
from datetime import date
from pypitch.api.models import PlayerStats
import pyarrow as pa
from tqdm import tqdm

# Internal Imports
from pypitch.storage.engine import QueryEngine
from pypitch.storage.registry import IdentityRegistry
from pypitch.runtime.executor import RuntimeExecutor
from pypitch.runtime.cache_duckdb import DuckDBCache
from pypitch.data.loader import DEFAULT_DATA_DIR, DataLoader
from pypitch.data.pipeline import build_registry_stats
from pypitch.core.canonicalize import canonicalize_match
from pypitch.core.migration import migrate_on_connect

logger = logging.getLogger(__name__)

class PyPitchSession:
    _instance: Optional["PyPitchSession"] = None

    def __init__(self, data_dir: Optional[str] = None, skip_registry_build: bool = False,
                 engine: Optional[QueryEngine] = None) -> None:
        self.data_dir = Path(data_dir) if data_dir else DEFAULT_DATA_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.db_path = str(self.data_dir / "pypitch.duckdb")
        self.registry_path = str(self.data_dir / "registry.duckdb")
        self.cache_path = str(self.data_dir / "cache.duckdb")
        
        # Initialize Components
        self.registry = IdentityRegistry(self.registry_path)
        self.engine = engine if engine else QueryEngine(self.db_path)
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
            raw_data_present = (
                self.loader.raw_dir.exists()
                and bool(list(self.loader.raw_dir.glob("*.json")))
            )
            registry_empty = not self.registry.get_player_stats(1)

            # Also rebuild when matchup_stats is empty (handles schema migration
            # from older registry.duckdb files that pre-date matchup tracking).
            try:
                matchup_count = self.registry.con.execute(
                    "SELECT count(*) FROM matchup_stats"
                ).fetchone()
                matchup_empty = (matchup_count[0] if matchup_count else 0) == 0
            except Exception:
                matchup_empty = True

            needs_build = registry_empty or matchup_empty

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
        """
        logger.info("Loading match %s", match_id)
        try:
            data = self.loader.get_match(match_id)
            table = canonicalize_match(data, self.registry, match_id)
            self.engine.ingest_events(table, snapshot_tag=f"match_{match_id}", append=True)
            logger.info("Match %s loaded successfully.", match_id)
        except Exception as e:
            logger.error("Failed to load match %s: %s", match_id, e)

    def get_player_stats(self, player_id: str) -> Optional[PlayerStats]:
        """Get player statistics by ID or name."""
        # Try to resolve as name first, then as ID
        try:
            player_id_int = int(player_id)
            entity_id = player_id_int
        except ValueError:
            # It's a name, try to resolve with different dates
            dates_to_try = [date.today(), date(2024, 1, 1), date(2023, 1, 1), date(2022, 1, 1)]
            entity_id = None
            for try_date in dates_to_try:
                try:
                    resolved_id = self.registry.resolve_player(player_id, try_date)
                    if resolved_id:
                        entity_id = resolved_id
                        break
                except Exception:
                    continue
        
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
            wickets=stats_dict["wickets"],
            balls_bowled=stats_dict["balls_bowled"],
            runs_conceded=stats_dict["runs_conceded"]
        )

    def get_match_stats(self, match_id: str) -> Optional[Any]:
        """Get match statistics by ID."""
        # This would need to be implemented based on what match stats are needed
        # For now, return None as placeholder
        return None

    def _setup_db(self) -> None:
        """Deprecated: Use lazy loading."""
        pass

    @classmethod
    def get(cls) -> "PyPitchSession":
        """Singleton Accessor"""
        if cls._instance is None:
            # AUTO-BOOT: If user forgot pp.init(), just do it for them.
            logger.info("Auto-initializing PyPitch (defaulting to ./data)...")
            cls._instance = PyPitchSession(data_dir="./data")
        return cls._instance

    @classmethod
    def cleanup(cls) -> None:
        """Clean up the singleton instance."""
        if cls._instance is not None:
            cls._instance.close()
            cls._instance = None

    def close(self) -> None:
        """Close all database connections."""
        self.registry.close()
        self.engine.close()
        self.cache.close()
        # Clear singleton reference to prevent stale instances
        if PyPitchSession._instance is self:
            PyPitchSession._instance = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        """Fallback cleanup in case close() wasn't called explicitly."""
        try:
            self.close()
        except:
            pass  # Ignore errors during cleanup

# Helper to expose the executor directly to API modules
def get_executor() -> RuntimeExecutor:
    return PyPitchSession.get().executor

def get_registry() -> IdentityRegistry:
    return PyPitchSession.get().registry

def init(source: Optional[str] = None) -> PyPitchSession:
    """
    Initialize the PyPitch session.
    """
    session = PyPitchSession(data_dir=source)
    PyPitchSession._instance = session
    return session

