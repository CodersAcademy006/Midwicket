from pathlib import Path
from typing import Optional

# Internal Imports
from pypitch.storage.engine import StorageEngine
from pypitch.storage.registry import IdentityRegistry
from pypitch.runtime.executor import Executor
from pypitch.runtime.cache import InMemoryCache
from pypitch.data.loader import DEFAULT_DATA_DIR

class PyPitchSession:
    _instance = None

    def __init__(self, data_dir: Optional[str] = None):
        self.data_dir = Path(data_dir) if data_dir else DEFAULT_DATA_DIR
        self.db_path = str(self.data_dir / "pypitch.duckdb")
        self.registry_path = str(self.data_dir / "registry.sqlite")
        
        # Initialize Components
        self.registry = IdentityRegistry(self.registry_path)
        self.engine = StorageEngine(self.db_path)
        self.cache = InMemoryCache()
        self.executor = Executor(self.engine, self.cache)

    @classmethod
    def get(cls):
        """Singleton Accessor"""
        if cls._instance is None:
            # Default initialization
            cls._instance = PyPitchSession()
        return cls._instance

# Helper to expose the executor directly to API modules
def get_executor() -> Executor:
    return PyPitchSession.get().executor

def get_registry() -> IdentityRegistry:
    return PyPitchSession.get().registry
