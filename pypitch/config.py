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
DATABASE_THREADS = int(os.getenv("PYPITCH_DB_THREADS", "4"))
DATABASE_MEMORY_LIMIT = os.getenv("PYPITCH_DB_MEMORY", "2GB")

# API settings
API_HOST = os.getenv("PYPITCH_API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("PYPITCH_API_PORT", "8000"))
API_CORS_ORIGINS = os.getenv("PYPITCH_CORS_ORIGINS", "*").split(",")

# Cache settings
CACHE_TTL = int(os.getenv("PYPITCH_CACHE_TTL", "3600"))  # 1 hour default

# Security settings
SECRET_KEY = os.getenv("PYPITCH_SECRET_KEY")
if not SECRET_KEY:
    if os.getenv("PYPITCH_ENV") != "development":
        raise RuntimeError("PYPITCH_SECRET_KEY is required in production")
    
    # Persistent development key
    dev_secret_file = (DEFAULT_DATA_DIR / ".pypitch_dev_secret").resolve()
    
    # Ensure parent directory exists
    try:
        dev_secret_file.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        logging.getLogger(__name__).exception("Failed to create directory %s", dev_secret_file.parent)
        raise

    if dev_secret_file.exists():
        try:
            with open(dev_secret_file, encoding='utf-8') as f:
                SECRET_KEY = f.read().strip()
        except Exception as err:
            logging.getLogger(__name__).exception("Failed to read development secret key")
            raise RuntimeError("Failed to read existing secret key file") from err
            
    if not SECRET_KEY:
        logger = logging.getLogger(__name__)
        logger.warning("Using insecure random secret key for development")
        SECRET_KEY = secrets.token_hex(32)
        
        try:
            import tempfile
            # Write atomically with restrictive permissions
            with tempfile.NamedTemporaryFile(mode='w', dir=dev_secret_file.parent, delete=False) as tmp:
                tmp.write(SECRET_KEY)
                tmp.flush()
                os.fsync(tmp.file.fileno())
                temp_path = Path(tmp.name)
            
            # Ensure permissions
            os.chmod(temp_path, 0o600)
            # Atomic replace
            os.replace(temp_path, dev_secret_file)
        except Exception as e:
            logger.warning("Failed to persist development secret key: %s", e)

API_KEY_REQUIRED = os.getenv("PYPITCH_API_KEY_REQUIRED", "false").lower() == "true"

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

def is_debug():
    return debug

def get_config():
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
