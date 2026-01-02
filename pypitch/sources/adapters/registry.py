from typing import Dict, Type
from .base import BaseAdapter

class AdapterRegistry:
    """
    Generic registry for all data adapters. Allows registration and lookup by key.
    """
    _registry: Dict[str, Type[BaseAdapter]] = {}

    @classmethod
    def register(cls, name: str, adapter_cls: Type[BaseAdapter]):
        if name in cls._registry:
            raise ValueError(f"Adapter '{name}' already registered.")
        cls._registry[name] = adapter_cls

    @classmethod
    def get(cls, name: str) -> Type[BaseAdapter]:
        if name not in cls._registry:
            raise KeyError(f"Adapter '{name}' not found.")
        return cls._registry[name]

    @classmethod
    def list_adapters(cls):
        return list(cls._registry.keys())
