from typing import Optional, Any, Dict
from abc import ABC, abstractmethod

class CacheInterface(ABC):
    @abstractmethod
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def set(self, key: str, value: Dict[str, Any], ttl: int = 3600):
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        pass

class InMemoryCache(CacheInterface):
    """
    Simple dict-based cache for single-process use.
    Not thread-safe or persistent.
    """
    def __init__(self):
        self._store = {}

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        return self._store.get(key)

    def set(self, key: str, value: Dict[str, Any], ttl: int = 3600):
        # TTL ignored in simple dict implementation
        self._store[key] = value

    def exists(self, key: str) -> bool:
        return key in self._store
