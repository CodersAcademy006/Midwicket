from abc import ABC, abstractmethod
from typing import Any, Optional

class CacheInterface(ABC):
    """
    The Protocol for valid storage backends.
    """
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve data if it exists and hasn't expired.
        Must return None if expired.
        """
        pass

    @abstractmethod
    def set(self, key: str, value: Any, ttl: int = 3600) -> None:
        """
        Persist data with a Time-To-Live (seconds).
        """
        pass

    @abstractmethod
    def clear(self) -> None:
        """
        Nuke the cache (maintenance/debugging).
        """
        pass

