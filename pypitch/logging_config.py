"""
Centralized logging configuration for pypitch.

Call ``setup_logging()`` once at application startup.  The ``force=True``
flag on ``basicConfig`` ensures the configuration is applied even when
another library has already initialised the root logger, making
re-configuration deterministic across all call sites.
"""
import logging
import sys
from pathlib import Path
from typing import List, Optional


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
) -> None:
    """
    Configure logging for the entire pypitch package.

    Args:
        level:    Root logging level (default: ``logging.INFO``).
        log_file: Optional path to also write log records to a file.

    Notes:
        ``force=True`` is required so that calling this function after
        another library has called ``basicConfig`` still takes effect.
        Without it, ``basicConfig`` is a silent no-op on subsequent
        calls, which makes log-level changes invisible and hard to debug.
    """
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file is not None:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,  # Override any prior basicConfig call (e.g. from third-party libs)
    )

    # Silence noisy third-party loggers that flood INFO output
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Return a module-level logger with the given name.

    Usage::

        from pypitch.logging_config import get_logger
        logger = get_logger(__name__)
        logger.info("Processing data...")

    Args:
        name: Logger name, typically ``__name__``.

    Returns:
        A :class:`logging.Logger` instance.
    """
    return logging.getLogger(name)
