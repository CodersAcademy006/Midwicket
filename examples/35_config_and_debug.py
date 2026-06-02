"""Midwicket config, debug mode, structured logging."""

import os, logging
os.environ.setdefault("MIDWICKET_ENV", "development")

from midwicket.logging_config import setup_logging, get_logger
from midwicket.config import get_config
import midwicket as md

setup_logging(level=logging.DEBUG)
log = get_logger(__name__)

print("config:", get_config())
md.set_debug_mode(True);  log.debug("eager execution ON")
md.set_debug_mode(False); print(f"midwicket v{md.__version__} by {md.__author__}")
