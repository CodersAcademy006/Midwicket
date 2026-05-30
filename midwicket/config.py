"""
Global configuration and debug mode for Midwicket.
"""

import os
import secrets
import logging
from pathlib import Path

# Debug mode
debug = False

# Data sources
CRICSHEET_URL = os.getenv("CRICSHEET_URL", "https://cricsheet.org/downloads/ipl_json.zip")

# Database settings
data_dir_env = os.getenv("MIDWICKET_DATA_DIR")
DEFAULT_DATA_DIR = Path(data_dir_env) if data_dir_env else Path.home() / ".midwicket_data"
def _safe_int_env(
    name: str,
    default: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    """Return a bounded integer env var, falling back to default on invalid input."""
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except (TypeError, ValueError):
        logging.getLogger(__name__).warning(
            "Invalid %s=%r; using default %s", name, raw, default
        )
        return default

    if minimum is not None and value < minimum:
        logging.getLogger(__name__).warning(
            "%s=%r is below minimum %s; using default %s",
            name,
            value,
            minimum,
            default,
        )
        return default
    if maximum is not None and value > maximum:
        logging.getLogger(__name__).warning(
            "%s=%r is above maximum %s; using default %s",
            name,
            value,
            maximum,
            default,
        )
        return default

    return value


DATABASE_THREADS = _safe_int_env("MIDWICKET_DB_THREADS", 4, minimum=1, maximum=16)
DATABASE_MEMORY_LIMIT = os.getenv("MIDWICKET_DB_MEMORY", "2GB")
DATABASE_MAX_CONNECTIONS = _safe_int_env("MIDWICKET_DB_MAX_CONNECTIONS", 20, minimum=5)
DATABASE_READ_POOL_SIZE = _safe_int_env("MIDWICKET_DB_READ_POOL_SIZE", 15, minimum=3)
DATABASE_WRITE_POOL_SIZE = _safe_int_env("MIDWICKET_DB_WRITE_POOL_SIZE", 5, minimum=2)

# API settings
API_HOST = os.getenv("MIDWICKET_API_HOST", "0.0.0.0")  # nosec B104 – container default, operator configures via env
API_PORT = _safe_int_env("MIDWICKET_API_PORT", 8000, minimum=1, maximum=65535)
# Default to empty list (no cross-origin access) — operators must explicitly
# allow origins via MIDWICKET_CORS_ORIGINS="https://app.example.com".
# Wildcards ("*") are intentionally not accepted as a default.
_cors_raw = os.getenv("MIDWICKET_CORS_ORIGINS", "")
API_CORS_ORIGINS = [o.strip() for o in _cors_raw.split(",") if o.strip()]

# Cache settings
CACHE_TTL = _safe_int_env("MIDWICKET_CACHE_TTL", 3600, minimum=1)  # 1 hour default

# Security settings — lazy accessor to avoid crashing on import
_SECRET_KEY: str | None = os.getenv("MIDWICKET_SECRET_KEY")


def get_secret_key() -> str:
    """
    Return the secret key, generating a dev key if necessary.

    Raises ``RuntimeError`` in production (MIDWICKET_ENV != 'development')
    when no key is configured — but only when the key is *actually needed*
    (JWT creation, token verification), not at import time.
    """
    global _SECRET_KEY

    if _SECRET_KEY:
        return _SECRET_KEY

    if os.getenv("MIDWICKET_ENV") != "development":
        raise RuntimeError(
            "MIDWICKET_SECRET_KEY is required in production. "
            "Set the environment variable before starting the server."
        )

    # Persistent development key — stored in the XDG config directory, NOT in
    # the data cache directory, so that deleting cached data does not rotate
    # the signing key (or vice versa).
    dev_secret_file = (Path.home() / ".config" / "midwicket" / "dev_secret").resolve()

    try:
        dev_secret_file.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to create directory %s", dev_secret_file.parent
        )
        raise

    if dev_secret_file.exists():
        try:
            with open(dev_secret_file, encoding="utf-8") as f:
                _SECRET_KEY = f.read().strip()
        except Exception as err:
            logging.getLogger(__name__).exception(
                "Failed to read development secret key"
            )
            raise RuntimeError("Failed to read existing secret key file") from err

    if not _SECRET_KEY:
        log = logging.getLogger(__name__)
        log.warning("Transient development secret key generated. Restart will produce a new key unless persistence succeeds.")
        _SECRET_KEY = secrets.token_hex(32)

        try:
            import tempfile
            with tempfile.NamedTemporaryFile(
                mode="w", dir=dev_secret_file.parent, delete=False
            ) as tmp:
                tmp.write(_SECRET_KEY)
                tmp.flush()
                os.fsync(tmp.fileno())
                temp_path = Path(tmp.name)
            os.chmod(temp_path, 0o600)
            os.replace(temp_path, dev_secret_file)
        except Exception as e:
            log.warning("Failed to persist development secret key: %s", e)

    return _SECRET_KEY


# SECRET_KEY is intentionally NOT exposed as a module-level constant. Importing
# it eagerly captured "" in production (signing JWTs with an empty key); callers
# must use get_secret_key(), which fails fast when no key is configured.

# Secure default: require API key authentication unless explicitly disabled.
# Set MIDWICKET_API_KEY_REQUIRED=false only for local development.
#
# MW-032: Keep a module-level alias so existing ``from config import
# API_KEY_REQUIRED`` and ``monkeypatch.setattr(mod, "API_KEY_REQUIRED", …)``
# callers continue to work.  New code should call ``is_api_key_required()``
# which re-reads the env var at call time and therefore respects any
# post-import env changes (e.g. test fixtures that set the var after loading).
API_KEY_REQUIRED = os.getenv("MIDWICKET_API_KEY_REQUIRED", "true").lower() == "true"


def is_api_key_required() -> bool:
    """Return ``True`` unless ``MIDWICKET_API_KEY_REQUIRED`` is ``'false'``.

    Unlike the module-level ``API_KEY_REQUIRED`` constant, this function
    re-reads the environment variable on every call, so it correctly reflects
    changes made after module import (e.g. in tests or hot-reload scenarios).
    """
    return os.getenv("MIDWICKET_API_KEY_REQUIRED", "true").lower() != "false"


def is_production() -> bool:
    """Return True when MIDWICKET_ENV is set to 'production'."""
    return os.getenv("MIDWICKET_ENV", "development") == "production"


def set_debug(value: bool = True) -> None:
    """
    Set debug mode. If True, forces eager execution and verbose errors.
    """
    global debug
    debug = value
    _log = logging.getLogger(__name__)
    if debug:
        _log.info("[Midwicket] Debug mode ON: Forcing eager execution and verbose errors.")
    else:
        _log.info("[Midwicket] Debug mode OFF.")
    # Unify duplicate debug flags (MW-033)
    try:
        import midwicket.runtime.modes as modes
        modes.debug_mode = value
    except Exception:
        pass
    try:
        import midwicket.express as express
        with express._debug_lock:
            express._DEBUG_MODE = value
    except Exception:
        pass

def is_debug() -> bool:
    return debug

def get_config() -> dict:
    """Get all configuration as a dict."""
    return {
        "debug": debug,
        "cricsheet_url": CRICSHEET_URL,
        "data_dir": str(DEFAULT_DATA_DIR),
        "db_threads": DATABASE_THREADS,
        "db_memory_limit": DATABASE_MEMORY_LIMIT,
        "db_max_connections": DATABASE_MAX_CONNECTIONS,
        "db_read_pool_size": DATABASE_READ_POOL_SIZE,
        "db_write_pool_size": DATABASE_WRITE_POOL_SIZE,
        "api_host": API_HOST,
        "api_port": API_PORT,
        "cors_origins": API_CORS_ORIGINS,
        "cache_ttl": CACHE_TTL,
        "api_key_required": is_api_key_required(),
    }
