"""
Data pipeline utilities — public surface for the data sub-package.

Re-exports the canonical implementation from pipeline.py so callers use
a stable import path (``from pypitch.data import build_registry_stats``)
while the actual logic lives in exactly one place.
"""
from .pipeline import build_registry_stats

__all__ = ["build_registry_stats"]
