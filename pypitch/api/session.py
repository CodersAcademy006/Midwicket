from pathlib import Path
from typing import Optional
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

class PyPitchSession:
    _instance: Optional["PyPitchSession"] = None

    def __init__(self, data_dir: Optional[str] = None) -> None:
        self.data_dir = Path(data_dir) if data_dir else DEFAULT_DATA_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.db_path = str(self.data_dir / "pypitch.duckdb")
        self.registry_path = str(self.data_dir / "registry.duckdb")
        self.cache_path = str(self.data_dir / "cache.duckdb")
        
        # Initialize Components
        self.registry = IdentityRegistry(self.registry_path)
        self.engine = QueryEngine(self.db_path)
        self.cache = DuckDBCache(self.cache_path)
        self.executor = RuntimeExecutor(self.cache, self.engine)
        self.loader = DataLoader(str(self.data_dir))
        
        # Auto-Setup
        # Check if registry is populated (simple check: do we have any players?)
        # If not, run the "Light" ingestion.
        if not self.registry.get_player_stats(1): # Dummy check, or better check if table exists/has rows
             # Actually, let's just check if we have data downloaded.
             if not (self.loader.raw_dir.exists() and list(self.loader.raw_dir.glob("*.json"))):
                 print("[PyPitch] First time setup detected. Downloading data...")
                 self.loader.download()
             
             # Now check if we need to build stats
             # We can check if the 'player_stats' table is empty or missing
             try:
                 self.registry.con.execute("SELECT count(*) FROM player_stats")
                 count = self.registry.con.fetchone()[0]
                 if count == 0:
                     raise Exception("Empty")
             except:
                 print("[PyPitch] Building Registry & Summary Stats...")
                 build_registry_stats(self.loader, self.registry)

    def load_match(self, match_id: str) -> None:
        """
        Lazy loads a specific match into the 'Heavy' engine.
        """
        print(f"Loading match {match_id}...")
        try:
            data = self.loader.get_match(match_id)
            table = canonicalize_match(data, self.registry, match_id)
            self.engine.ingest_events(table, snapshot_tag=f"match_{match_id}", append=True)
            print(f"Match {match_id} loaded successfully.")
        except Exception as e:
            print(f"Failed to load match {match_id}: {e}")

    def _setup_db(self) -> None:
        """Deprecated: Use lazy loading."""
        pass

    @classmethod
    def get(cls) -> "PyPitchSession":
        """Singleton Accessor"""
        if cls._instance is None:
            # AUTO-BOOT: If user forgot pp.init(), just do it for them.
            print("⚙️  Auto-initializing PyPitch (defaulting to ./data)...")
            cls._instance = PyPitchSession(data_dir="./data")
        return cls._instance

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

