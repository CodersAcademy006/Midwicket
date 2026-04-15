import logging
import os
import tempfile
import requests
import zipfile
import json
from pathlib import Path
from typing import Iterator, Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from tqdm import tqdm
# Constants
from pypitch.config import CRICSHEET_URL, DEFAULT_DATA_DIR

# M4: make timeouts configurable via env vars
_DOWNLOAD_TIMEOUT = int(os.getenv("PYPITCH_DOWNLOAD_TIMEOUT", "60"))
_EXTRACT_TIMEOUT = int(os.getenv("PYPITCH_EXTRACT_TIMEOUT", "120"))
_DOWNLOAD_RETRY_ATTEMPTS = max(1, int(os.getenv("PYPITCH_DOWNLOAD_RETRIES", "3")))
_DOWNLOAD_RETRY_BACKOFF_BASE = float(os.getenv("PYPITCH_DOWNLOAD_RETRY_BACKOFF_BASE", "0.5"))
_DOWNLOAD_RETRY_BACKOFF_MAX = float(os.getenv("PYPITCH_DOWNLOAD_RETRY_BACKOFF_MAX", "8"))

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
        Defaults to ~/.pypitch_data/ to keep the user's project clean.
        """
        self.data_dir = Path(data_dir) if data_dir else DEFAULT_DATA_DIR
        self.raw_dir = self.data_dir / "raw" / "ipl"
        self.zip_path = self.data_dir / "ipl_json.zip"
        
        # Ensure directories exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def download(self, force: bool = False) -> None:
        """
        Downloads the latest dataset from Cricsheet.
        Skips if already exists, unless force=True.
        """
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
            logger.info("Download complete.")

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
        json_files = list(self.raw_dir.glob("*.json"))
        
        if not json_files:
            raise FileNotFoundError("No JSON files found. Run loader.download() first.")
            
        logger.info("Found %d matches in %s", len(json_files), self.raw_dir)
        
        for file_path in json_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    # Basic validation: ensure it looks like a match file
                    if 'info' in data and 'innings' in data:
                        yield data
            except json.JSONDecodeError:
                continue # Skip corrupt files
