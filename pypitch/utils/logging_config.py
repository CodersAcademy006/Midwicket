"""
Logging configuration for PyPitch.

.. deprecated:: 0.1.0
   This module has been consolidated into ``pypitch.logging_config``.
   Import from there instead::

       from pypitch.logging_config import setup_logging
"""

# Re-export from the canonical location for backward compatibility
from pypitch.logging_config import setup_logging  # noqa: F401

__all__ = ["setup_logging"]