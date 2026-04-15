"""Midwicket public package.

This package re-exports the existing ``pypitch`` API surface so users can
migrate immediately with ``import midwicket as mw`` while legacy
``import pypitch`` continues to work.
"""

from pypitch import *  # noqa: F401,F403
from pypitch import __all__ as _PYPITCH_ALL
from pypitch import __author__, __email__, __version__

__all__ = list(_PYPITCH_ALL)
