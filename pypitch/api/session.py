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
        
        # Auto-Setup
        if not self.engine.table_exists("ball_events"):
            print("[PyPitch] First time setup detected. Initializing database...")
            self._setup_db()

    def _setup_db(self) -> None:
        """Downloads and ingests data automatically."""
        loader = DataLoader(str(self.data_dir))
        
        # 1. Download
        try:
            loader.download()
        except Exception as e:
            print(f"[PyPitch] Failed to download data: {e}")
            return

        # 2. Process
        print("[PyPitch] Processing matches (this may take a minute)...")
        tables = []
        matches = list(loader.iter_matches())
        
        for match_data in tqdm(matches, desc="Ingesting"):
            try:
                table = canonicalize_match(match_data, self.registry)
                tables.append(table)
            except Exception:
                continue

        if tables:
            full_table = pa.concat_tables(tables)
            self.engine.ingest_events(full_table, snapshot_tag="latest")
            print("[PyPitch] Setup complete. Database ready.")
        else:
            print("[PyPitch] Warning: No data found to ingest.")

    @classmethod
    def get(cls) -> "PyPitchSession":
        """Singleton Accessor"""
        if cls._instance is None:
            # Default initialization
            cls._instance = PyPitchSession()
        return cls._instance

# Helper to expose the executor directly to API modules
def get_executor() -> RuntimeExecutor:
    return PyPitchSession.get().executor

def get_registry() -> IdentityRegistry:
    return PyPitchSession.get().registry

