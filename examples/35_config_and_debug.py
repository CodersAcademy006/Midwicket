"""
35_config_and_debug.py — Configuration, Debug Mode & Logging

Demonstrates:
  • How to configure PyPitch via environment variables
  • Debug / eager execution mode
  • Structured logging setup
  • Reading the current configuration dict

Usage:
    python examples/35_config_and_debug.py

Environment variables (all optional):
    PYPITCH_DATA_DIR        Path for DuckDB files  (default: ~/.pypitch_data)
    PYPITCH_DB_THREADS      DuckDB threads          (default: 4)
    PYPITCH_DB_MEMORY       DuckDB memory limit     (default: 2GB)
    PYPITCH_CACHE_TTL       Cache TTL in seconds    (default: 3600)
    PYPITCH_ENV             Set to "development" to skip SECRET_KEY requirement
"""

import os
import logging

# Ensure we run in dev mode so SECRET_KEY isn't required
os.environ.setdefault("PYPITCH_ENV", "development")

from pypitch.logging_config import setup_logging, get_logger
import pypitch as pp


def main() -> None:
    # ------------------------------------------------------------------
    # 1. Set up structured logging
    # ------------------------------------------------------------------
    setup_logging(level=logging.DEBUG)
    logger = get_logger(__name__)
    logger.info("PyPitch Config & Debug Demo starting")

    print("\n[1] Logging configured at DEBUG level")

    # ------------------------------------------------------------------
    # 2. Show current configuration
    # ------------------------------------------------------------------
    from pypitch.config import get_config
    cfg = get_config()
    print("\n[2] Active configuration:")
    for key, val in cfg.items():
        print(f"  {key:<25} = {val}")

    # ------------------------------------------------------------------
    # 3. Debug mode — forces eager (non-lazy) execution
    # ------------------------------------------------------------------
    print("\n[3] Debug mode")
    pp.set_debug_mode(True)
    logger.debug("Debug mode is now ON")

    # In debug mode, lazy Arrow expressions are collected immediately,
    # surfacing errors early rather than at materialization time.
    print("  Debug mode: ON  (queries execute eagerly)")

    pp.set_debug_mode(False)
    print("  Debug mode: OFF (production mode)")

    # ------------------------------------------------------------------
    # 4. Version info
    # ------------------------------------------------------------------
    print(f"\n[4] pypitch version : {pp.__version__}")
    print(f"    author          : {pp.__author__}")

    logger.info("Demo complete")


if __name__ == "__main__":
    main()
