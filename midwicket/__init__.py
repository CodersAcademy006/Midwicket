"""Midwicket public package.

This package re-exports the existing ``pypitch`` API surface so users can
migrate immediately with ``import midwicket as mw`` while legacy
``import pypitch`` continues to work.
"""

import pypitch as _pypitch

from pypitch import *  # noqa: F401,F403
from pypitch import __all__ as _PYPITCH_ALL
from pypitch import __author__, __email__, __version__

__all__ = list(_PYPITCH_ALL)

# Mirror pypitch's package path so imports like "midwicket.express" and
# "midwicket.data.loader" resolve against the existing implementation.
__path__ = list(getattr(_pypitch, "__path__", []))


def __getattr__(name: str):
    """Delegate unknown attributes to pypitch for forward compatibility."""
    return getattr(_pypitch, name)


def __dir__():
    return sorted(set(globals()) | set(dir(_pypitch)))
