"""
Global configuration and debug mode for PyPitch.
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
data_dir_env = os.getenv("PYPITCH_DATA_DIR")
DEFAULT_DATA_DIR = Path(data_dir_env) if data_dir_env else Path.home() / ".pypitch_data"
_raw_threads = os.getenv("PYPITCH_DB_THREADS", "4")
try:
    DATABASE_THREADS = int(_raw_threads)
except ValueError:
    raise ValueError(
        f"PYPITCH_DB_THREADS must be an integer, got {_raw_threads!r}"
    )
if not (1 <= DATABASE_THREADS <= 16):
    raise ValueError(
        f"PYPITCH_DB_THREADS must be between 1 and 16, got {DATABASE_THREADS}"
    )
DATABASE_MEMORY_LIMIT = os.getenv("PYPITCH_DB_MEMORY", "2GB")

# API settings
API_HOST = os.getenv("PYPITCH_API_HOST", "0.0.0.0")  # nosec B104 – container default, operator configures via env
API_PORT = int(os.getenv("PYPITCH_API_PORT", "8000"))
# Default to empty list (no cross-origin access) — operators must explicitly
# allow origins via PYPITCH_CORS_ORIGINS="https://app.example.com".
# Wildcards ("*") are intentionally not accepted as a default.
_cors_raw = os.getenv("PYPITCH_CORS_ORIGINS", "")
API_CORS_ORIGINS = [o.strip() for o in _cors_raw.split(",") if o.strip()]

# Cache settings
CACHE_TTL = int(os.getenv("PYPITCH_CACHE_TTL", "3600"))  # 1 hour default

# Security settings — lazy accessor to avoid crashing on import
_SECRET_KEY: str | None = os.getenv("PYPITCH_SECRET_KEY")


def get_secret_key() -> str:
    """
    Return the secret key, generating a dev key if necessary.

    Raises ``RuntimeError`` in production (PYPITCH_ENV != 'development')
    when no key is configured — but only when the key is *actually needed*
    (JWT creation, token verification), not at import time.
    """
    global _SECRET_KEY

    if _SECRET_KEY:
        return _SECRET_KEY

    if os.getenv("PYPITCH_ENV") != "development":
        raise RuntimeError(
            "PYPITCH_SECRET_KEY is required in production. "
            "Set the environment variable before starting the server."
        )

    # Persistent development key
    dev_secret_file = (DEFAULT_DATA_DIR / ".pypitch_dev_secret").resolve()

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
        log.warning("Using insecure random secret key for development")
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


# Backward-compat: modules that read config.SECRET_KEY get the lazy accessor
# via a property-like pattern. For now, keep a module-level alias that defers.
SECRET_KEY = os.getenv("PYPITCH_SECRET_KEY", "")

# Secure default: require API key authentication unless explicitly disabled.
# Set PYPITCH_API_KEY_REQUIRED=false only for local development.
API_KEY_REQUIRED = os.getenv("PYPITCH_API_KEY_REQUIRED", "true").lower() == "true"

def is_production() -> bool:
    """Return True when PYPITCH_ENV is set to 'production'."""
    return os.getenv("PYPITCH_ENV", "development") == "production"


def set_debug(value: bool = True) -> None:
    """
    Set debug mode. If True, forces eager execution and verbose errors.
    """
    global debug
    debug = value
    if debug:
        print("[PyPitch] Debug mode ON: Forcing eager execution and verbose errors.")
    else:
        print("[PyPitch] Debug mode OFF.")

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
        "api_host": API_HOST,
        "api_port": API_PORT,
        "cors_origins": API_CORS_ORIGINS,
        "cache_ttl": CACHE_TTL,
        "api_key_required": API_KEY_REQUIRED,
    }
