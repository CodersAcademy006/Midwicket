"""
Logging configuration for Midwicket.

.. deprecated:: 0.1.0
   This module has been consolidated into ``midwicket.logging_config``.
   Import from there instead::

       from midwicket.logging_config import setup_logging
"""

# Re-export from the canonical location for backward compatibility
from midwicket.logging_config import setup_logging  # noqa: F401

__all__ = ["setup_logging"]