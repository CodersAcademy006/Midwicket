import logging
import os
import tempfile
import requests
import zipfile
import json
from pathlib import Path
from pydantic import BaseModel, ValidationError, Field
from typing import Iterator, Dict, Any, Optional, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from tqdm import tqdm

class CricsheetInfoSchema(BaseModel):
    match_type: str
    city: Optional[str] = None
    dates: List[str]
    teams: List[str]

class CricsheetMatchSchema(BaseModel):
    info: CricsheetInfoSchema
    innings: List[Dict[str, Any]]
# Constants
from midwicket.config import CRICSHEET_URL, DEFAULT_DATA_DIR


def _safe_int_env(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, str(default))
    log = logging.getLogger(__name__)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        log.warning("Invalid %s=%r; using default %s", name, raw, default)
        return default
    if value < minimum:
        log.warning(
            "%s=%r is below minimum %s; using default %s",
            name,
            value,
            minimum,
            default,
        )
        return default
    return value


def _safe_float_env(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.getenv(name, str(default))
    log = logging.getLogger(__name__)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        log.warning("Invalid %s=%r; using default %s", name, raw, default)
        return default
    if value < minimum:
        log.warning(
            "%s=%r is below minimum %s; using default %s",
            name,
            value,
            minimum,
            default,
        )
        return default
    return value


# M4: make timeouts configurable via env vars
_DOWNLOAD_TIMEOUT = _safe_int_env("MIDWICKET_DOWNLOAD_TIMEOUT", 60, minimum=1)
_EXTRACT_TIMEOUT = _safe_int_env("MIDWICKET_EXTRACT_TIMEOUT", 120, minimum=1)
_DOWNLOAD_RETRY_ATTEMPTS = _safe_int_env("MIDWICKET_DOWNLOAD_RETRIES", 3, minimum=1)
_DOWNLOAD_RETRY_BACKOFF_BASE = _safe_float_env(
    "MIDWICKET_DOWNLOAD_RETRY_BACKOFF_BASE", 0.5, minimum=0.0
)
_DOWNLOAD_RETRY_BACKOFF_MAX = _safe_float_env(
    "MIDWICKET_DOWNLOAD_RETRY_BACKOFF_MAX", 8.0, minimum=0.0
)

logger = logging.getLogger(__name__)


@retry(
    reraise=True,
    stop=stop_after_attempt(_DOWNLOAD_RETRY_ATTEMPTS),
    wait=wait_exponential(
        multiplier=_DOWNLOAD_RETRY_BACKOFF_BASE,
        min=_DOWNLOAD_RETRY_BACKOFF_BASE,
        max=_DOWNLOAD_RETRY_BACKOFF_MAX,
    ),
    retry=retry_if_exception_type(requests.RequestException),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def _download_stream(url: str) -> requests.Response:
    """Fetch URL with retries for transient network/HTTP failures."""
    response = requests.get(url, stream=True, timeout=_DOWNLOAD_TIMEOUT)
    response.raise_for_status()
    return response

class DataLoader:
    def __init__(self, data_dir: Optional[str] = None):
        """
        Manages raw data storage.
        Defaults to ":memory:" for high-performance in-memory caching.
        """
        if data_dir is None:
            self.data_dir = Path(":memory:")
        else:
            self.data_dir = Path(data_dir)
            
        self.is_memory = (str(self.data_dir) == ":memory:")
        
        if self.is_memory:
            self.raw_dir = None
            self.zip_path = Path(__file__).parent / "ipl_json.zip"
            self._load_zip_to_memory()
        else:
            self.raw_dir = self.data_dir / "raw" / "ipl"
            self.zip_path = self.data_dir / "ipl_json.zip"
            # Ensure directories exist
            self.raw_dir.mkdir(parents=True, exist_ok=True)

    def _load_zip_to_memory(self) -> None:
        """Validates that the bundled ZIP is present for lazy loading."""
        if not self.zip_path.exists():
            # Fallback paths
            possible_paths = [
                self.zip_path,
                Path("./data/ipl_json.zip"),
                Path.home() / ".midwicket_data" / "ipl_json.zip"
            ]
            for path in possible_paths:
                if path.exists():
                    self.zip_path = path
                    break
            else:
                logger.warning("Bundled dataset ZIP not found at %s. Running with empty RAM cache.", self.zip_path)
                return

    def download(self, force: bool = False) -> None:
        """
        Downloads the latest dataset from Cricsheet.
        Skips if already exists, unless force=True.
        """
        if self.is_memory:
            logger.info("Running in in-memory mode, dataset already pre-cached in RAM.")
            return

        if self.zip_path.exists() and not force:
            logger.info("Data already exists at %s", self.zip_path)
            return

        logger.info("Downloading IPL Data from %s", CRICSHEET_URL)
        
        try:
            response = _download_stream(CRICSHEET_URL)

            total_size = int(response.headers.get('content-length', 0))

            # Write to a temp file then rename atomically to avoid TOCTOU corruption.
            tmp_fd, tmp_name = tempfile.mkstemp(dir=self.zip_path.parent, suffix=".tmp")
            tmp_path = Path(tmp_name)
            try:
                with os.fdopen(tmp_fd, 'wb') as f, tqdm(
                    desc="Downloading",
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
                ) as bar:
                    for chunk in response.iter_content(chunk_size=8192):
                        size = f.write(chunk)
                        bar.update(size)
                tmp_path.replace(self.zip_path)
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise

            logger.info("Extracting files...")
            self._extract()
            # Remove the intermediate archive to avoid double disk usage;
            # the extracted JSON files in raw_dir are the canonical source.
            try:
                self.zip_path.unlink()
                logger.info("Removed intermediate archive %s", self.zip_path)
            except OSError as unlink_err:
                logger.warning("Could not remove archive %s: %s", self.zip_path, unlink_err)
            logger.info("Download complete. Data stored at: %s", self.raw_dir)

        except Exception as e:
            # Clean up partial downloads
            if self.zip_path.exists():
                self.zip_path.unlink()
            raise ConnectionError(f"Failed to download data: {e}")

    def _extract(self) -> None:
        """
        Unzips the downloaded file into the raw directory.

        Guards against zip-slip: any member whose resolved path would escape
        ``self.raw_dir`` is skipped with a warning.
        """
        if self.is_memory:
            return
        raw_dir_resolved = self.raw_dir.resolve()
        with zipfile.ZipFile(self.zip_path, "r") as z:
            for member in z.namelist():
                target = (raw_dir_resolved / member).resolve()
                # Reject paths that escape the extraction directory
                try:
                    target.relative_to(raw_dir_resolved)
                except ValueError:
                    logger.warning("Skipping unsafe zip entry: %s", member)
                    continue
                z.extract(member, self.raw_dir)

    def get_match(self, match_id: str) -> Dict[str, Any]:
        """
        Fetches a specific match by ID.
        """
        # Sanitize match_id: only allow alphanumeric, hyphens, and underscores
        # to prevent path traversal attacks (e.g. "../../../etc/passwd").
        safe_id = Path(match_id).name
        if safe_id != match_id or "/" in match_id or "\\" in match_id:
            raise ValueError(f"Invalid match_id: {match_id!r}")

        if self.is_memory:
            if not hasattr(self, "_zip_file"):
                self._zip_file = zipfile.ZipFile(self.zip_path, "r")
            
            filename = f"{safe_id}.json"
            try:
                content = self._zip_file.read(filename)
                return json.loads(content.decode("utf-8"))
            except Exception:
                raise FileNotFoundError(f"Match {match_id} not found in in-memory zip cache")

        file_path = self.raw_dir / f"{safe_id}.json"
        if not file_path.exists():
            raise FileNotFoundError(f"Match {match_id} not found in {self.raw_dir}")
            
        with open(file_path, 'r') as f:
            return json.load(f)

    def iter_matches(self) -> Iterator[Dict[str, Any]]:
        """
        Yields match data one by one.
        Generator pattern prevents RAM overflow when processing 10k+ matches.
        """
        if self.is_memory:
            if not hasattr(self, "_zip_file"):
                self._zip_file = zipfile.ZipFile(self.zip_path, "r")
                
            json_members = [m for m in self._zip_file.namelist() if m.endswith(".json") and "/" not in m and "\\" not in m]
            logger.info("Found %d matches in RAM zip cache", len(json_members))
            for member in json_members:
                try:
                    content = self._zip_file.read(member)
                    data = json.loads(content.decode("utf-8"))
                    CricsheetMatchSchema(**data)
                    yield data
                except Exception as e:
                    logger.warning(f"Schema validation failed for in-memory match {member}: {e}")
                    continue
            return

        json_files = list(self.raw_dir.glob("*.json"))
        
        if not json_files:
            raise FileNotFoundError("No JSON files found. Run loader.download() first.")
            
        logger.info("Found %d matches in %s", len(json_files), self.raw_dir)
        
        for file_path in json_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    
                    try:
                        CricsheetMatchSchema(**data)
                        yield data
                    except ValidationError as e:
                        logger.warning(f"Schema validation failed for {file_path}: {e}")
                        continue
            except json.JSONDecodeError:
                continue # Skip corrupt files

    def data_path(self) -> Optional[Path]:
        """Return the on-disk data directory, or ``None`` for in-memory mode."""
        return None if self.is_memory else self.data_dir

    def clear_data(self) -> None:
        """Delete all downloaded data files and the data directory.

        Safe to call when in-memory mode (no-op).  After calling this,
        ``download()`` must be run again before ``iter_matches()`` or
        ``get_match()`` will return data.

        Raises:
            RuntimeError: If the data directory cannot be fully removed.
        """
        if self.is_memory:
            logger.info("In-memory mode: nothing to clear.")
            return
        import shutil
        if self.data_dir.exists():
            try:
                shutil.rmtree(self.data_dir)
                logger.info("Removed data directory: %s", self.data_dir)
            except Exception as exc:
                raise RuntimeError(f"Failed to clear data directory {self.data_dir}: {exc}") from exc
        else:
            logger.info("Data directory %s does not exist; nothing to clear.", self.data_dir)
